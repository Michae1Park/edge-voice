# Call Lifecycle + JSON Payloads — Design Plan

**Status:** Not started. Scoping only — no code written.
**Companion to:** `ARCHITECTURE.md`, `BUILDPLAN.md`

## Goal

Two changes arriving together:

1. **Audio packets become JSON-wrapped** instead of raw PCM bytes on the MQTT
   topic.
2. **Call-start / call-end signals** arrive over MQTT. Call-start resets the
   pipeline and erases the UI, so a new call begins as if the program had just
   launched. Call-end has no defined action yet, but is assumed to arrive in
   case one is added.

---

## What the codebase already says about this

Two pre-existing notes bear directly on the design and should be read before
writing any code.

**`VADWorker.reset_channel()`** carries an UNRESOLVED note:

> this DISCARDS any in-progress segment. That is right for a reconnect gap (the
> buffered audio is stale and has a hole in it), but wrong for a call ending,
> which is the same drop-the-last-utterance bug flush() was added to fix:
> `tx_recorded_1.wav` ends mid-utterance and lost 2.62s of real speech that way.
> Nothing calls this yet, so nothing is losing data today -- but before wiring
> it to a call-end signal, decide per caller and add flush-then-reset for the
> end-of-call case.

So the split is already decided, just not implemented:

- **call-start** → plain reset (discarding stale audio is correct)
- **call-end** → flush-then-reset (or the last utterance is lost)

**`Repacketizer`** (`channel/router.py`) has a quieter but nastier one: without
`reset_channel()` on a discontinuity, "timestamps will drift silently." No
exception, no warning — just wrong timestamps from then on. Both resets must
fire on a call boundary.

---

## The core problem: a race, not a routing question

However the signal arrives — separate topic or an in-stream envelope — it lands
on the paho thread at a moment when audio from the old call is still sitting in
`ingest_queue`, `routed_queue`, and `segment_queue`. Resetting the stages
directly leaves those packets to flow through afterward, producing transcripts
from the old call inside the freshly-cleared UI.

The race bites exactly at the boundary that matters: call-end flushes the last
utterance → that segment is in flight → the next call-start clears the UI → the
old transcript lands after the clear.

**Separate topics do not avoid this, and weaken ordering.** In one envelope the
control message sits in that channel's audio stream, so its position is
unambiguous. Across two topics you rely on broker delivery order between
independent subscriptions, which isn't guaranteed.

---

## Approach A — control events through the queues (rejected)

A `CallEvent` flows through the same queues as audio; each stage resets when it
passes. Ordered by construction, but:

- queues stop being `Queue[AudioPacket]` — union types throughout
- every worker's run loop grows a control branch
- touches `pipeline/queues.py` and every stage

**~3-5 days.** Correct, but pays a lot for it.

## Approach B — session id on existing messages (recommended)

Carry a **session id** on the messages already flowing. Stale work becomes
*harmless* rather than *impossible*:

```
call-start → MQTT thread bumps the session counter
           → stamps every AudioPacket with it
           → SpeechSegment and TranscriptEvent inherit it
```

Each stateful stage resets itself when it sees a new id. No control event, no
queue type change, no branch in any run loop:

```python
if packet.session_id != self._session.get(channel_id):
    self.reset_channel(channel_id)      # repacketizer buffer / VAD state
    self._session[channel_id] = packet.session_id
```

The UI does the same: clear the feed when the id changes, and ignore anything
stamped with an older one. Late arrivals from the previous call are dropped on
an id mismatch instead of racing the reset.

**~2 days.** `pipeline/queues.py` drops off the list entirely.

### Known gap in Approach B

It assumes call-start always precedes the new call's first packet. If a call can
start with no audio following, nothing carries the new id and the UI never
clears. Separate topics make this more likely than an in-stream envelope —
**confirm against how the publisher actually behaves before committing.**

If that case is real, the fallback is a direct UI-clear on call-start (out of
band, since the UI has no ordering constraint) while stage resets stay
data-driven.

---

## Files to change (Approach B)

| File | Change |
|---|---|
| `pipeline/models.py` | `session_id` on `AudioPacket`, `SpeechSegment`, `TranscriptEvent` |
| `audio_ingest/mqtt_client.py` | JSON parse, session tracking, stamping |
| `channel/router.py` | reset repacketizer on id change |
| `vad/vad_worker.py` | flush-or-reset on id change; resolve the UNRESOLVED note |
| `stt/stt_worker.py` | propagate id into `TranscriptEvent` |
| `pipeline/transcript_hub.py` | drop backlog on id change |
| `webui/app.py` | serialize `session_id` |
| `webui/templates/console.html` | clear feed + `liveBubbles` on id change |
| `config/settings.py`, `configs/default.yaml` | payload format, control topic names |

Adding the field is cheap: 8 construction sites across `src/`, all of which keep
working if it has a default.

---

## JSON payloads — two gotchas

`mqtt_client.py` currently uses `msg.payload` directly as PCM.

- **The Repacketizer rejects wrong-sized packets.** `process()` raises
  `ValueError` when `len(samples) != incoming_bytes`, and the router catches it
  and *drops the packet*. If JSON framing shifts the 20ms boundary, you get
  silent audio loss behind a warning log.
- **Base64 costs ~33% bandwidth plus a decode per packet** on the real-time
  path — 100 decodes/sec at 20ms × 2 channels. Probably fine on a Pi 5, worth
  measuring rather than assuming.

---

## Call-end is nearly free either way

`idle_flush_s: 2.0` already flushes an in-progress segment after 2s of no
packets, which *is* the end-of-call case. An explicit call-end signal therefore
buys **~2 seconds of latency on the final utterance**, and nothing else. Wire it
to an immediate flush if that matters; skip it otherwise.

Either way, `reset_channel()` still needs the flush-then-reset split — a
session-id reset that discards an in-progress segment loses the last utterance
exactly as the note warns.

---

## Interaction with partial transcripts

This work should branch off `feat-partial-transcription`, not `main`.

`mqtt_client.py`, `channel/router.py`, and `pipeline/queues.py` — the bulk of the
new work — are untouched by partials, so there's no conflict there. Overlap lands
in ~6 files, nearly all additive-adjacent (new field beside new field), which are
minutes to resolve. Two need real thought:

- **`console.html`** — partials rewired `source.onmessage` and added the
  `liveBubbles` map. A call-start clear must *also* clear `liveBubbles`, or a
  partial from the old call resurrects a bubble in the new one. A merge won't
  catch this.
- **`stt_worker.py` / `vad_worker.py` dispatch points** — partials added a branch
  at the top of `_handle_segment` and a hook in `_continue_segment`; session
  checks want the same spots.

Branching off `main` doesn't avoid that work, it defers it to a merge with less
context. The one condition that flips this: if partials might be abandoned, keep
the branches independent.

---

## Open questions

1. Does call-start always precede the new call's first audio packet? (Decides
   whether Approach B is sufficient on its own — see "Known gap".)
2. Separate control topics, or a type-tagged envelope on the audio topic?
   Envelope gives stronger ordering; separate topics are simpler to publish.
3. Is per-channel or per-call session scope correct — can rx and tx belong to
   different calls, or is a call always both legs together?
