(function () {
  var feed = document.getElementById("feed");
  var inner = document.getElementById("feedInner");
  var empty = document.getElementById("empty");
  var jumpBtn = document.getElementById("jumpBtn");
  var clockEl = document.getElementById("clock");
  var livePill = document.getElementById("livePill");
  var liveLabel = document.getElementById("liveLabel");
  var clearBtn = document.getElementById("clearBtn");
  var chipMqtt = document.getElementById("chipMqtt");
  var mqttVal = document.getElementById("mqttVal");
  var chipQueues = document.getElementById("chipQueues");
  var queueVal = document.getElementById("queueVal");
  var chipMetrics = document.getElementById("chipMetrics");
  var metricsVal = document.getElementById("metricsVal");
  var channelChips = document.getElementById("channelChips");
  var modulesEl = document.getElementById("modules");
  var chanCache = {};
  var modCache = {};
  var DASH = "—";

  // Pipeline order, with the short label shown on the kiosk and which
  // `latencies` key (if any) belongs to that module. MqttAudioIngest is a
  // source with nothing to time -- it has no latency entry by design.
  //
  // STTWorker is one row per channel, not one shared row -- there's a
  // dedicated decoder per channel now (see orchestrator.py's per-channel
  // _build_stt), each with its own worker name ("STTWorker-rx" etc.) and
  // its own entry in latencies.stt. The `channel` field tells
  // renderModules to read latencies[mod.latency][mod.channel] directly
  // instead of worstLatency() across the whole dict -- without it, both
  // rows would show the same worst-of-both-channels number, which would
  // look like real per-channel data but actively mislead.
  var MODULES = [
    { worker: "MqttAudioIngest", label: "Ingest", latency: null },
    { worker: "ChannelRouter", label: "Router", latency: "router" },
    { worker: "VADWorker", label: "VAD", latency: "vad_silero" },
    { worker: "STTWorker-rx", label: "STT RX", latency: "stt", channel: "rx" },
    { worker: "STTWorker-tx", label: "STT TX", latency: "stt", channel: "tx" },
    // Not a pipeline stage -- the thread that restarts the ones above.
    // Nothing restarts it in turn, so its state is only ever
    // running/stopped, never restarting/degraded.
    { worker: "Supervisor", label: "Supervisor", latency: null },
  ];

  var cursorRow = document.createElement("div");
  cursorRow.className = "cursor-row";
  cursorRow.innerHTML = '<span class="blink stopped" id="blink">&#9612;</span><span id="cursorText">connecting…</span>';
  inner.appendChild(cursorRow);
  var blink = document.getElementById("blink");
  var cursorText = document.getElementById("cursorText");

  function isNearBottom() {
    return feed.scrollHeight - feed.scrollTop - feed.clientHeight < 90;
  }

  function scrollToBottom(smooth) {
    feed.scrollTo({ top: feed.scrollHeight, behavior: smooth ? "smooth" : "auto" });
    jumpBtn.hidden = true;
  }

  function formatTime(epochSeconds) {
    var d = new Date(epochSeconds * 1000);
    return d.toLocaleTimeString([], { hour12: false });
  }

  // One chat bubble per utterance, avatar-led: rx on the left in a neutral
  // bubble, tx on the right in the accent gradient (row-reverse, via the
  // .tx class), matching the reference mockup's incoming/outgoing layout.
  function renderMessage(m) {
    var wrap = document.createElement("div");
    wrap.className = "msg " + m.channel_id + (m.is_final ? "" : " partial");
    // When the audio was spoken, not when the transcript arrived -- see
    // insertByTimestamp. `start` is real wall-clock (AudioPacket.timestamp
    // propagates unchanged from MQTT ingest), so it's directly comparable
    // across channels.
    wrap.dataset.start = String(m.start);

    var avatar = document.createElement("div");
    avatar.className = "avatar " + m.channel_id;
    avatar.textContent = m.channel_id.toUpperCase();

    var col = document.createElement("div");
    col.className = "msg-col";

    var bubble = document.createElement("div");
    bubble.className = "bubble";
    bubble.textContent = m.text;

    var time = document.createElement("div");
    time.className = "msg-meta";
    time.textContent = formatTime(m.created_at);

    col.appendChild(bubble);
    col.appendChild(time);
    wrap.appendChild(avatar);
    wrap.appendChild(col);
    return { wrap: wrap, bubble: bubble, time: time };
  }

  // Place a message by WHEN IT WAS SPOKEN, not when it arrived.
  //
  // Each channel has its own STT worker (its own process, in the deployed
  // config), so the two decode in parallel and finish independently: a
  // short `tx` utterance can overtake a long `rx` one spoken before it, and
  // appending blindly would show the conversation out of order. Enforcing
  // order in the pipeline instead would mean holding finished transcripts
  // behind the slowest channel -- giving back exactly the latency the
  // parallel decoders bought. See docs/STT_MULTIPROCESS_PLAN.md section 7.
  //
  // In practice partials usually resolve this before it's visible: a
  // partial for a long utterance arrives ~1s in and claims the slot early,
  // and its final then replaces it in place (keyed by segment_id below).
  //
  // Returns true when the message landed above the end of the feed.
  function insertByTimestamp(wrap) {
    var startTs = parseFloat(wrap.dataset.start);
    var existing = inner.querySelectorAll(".msg[data-start]");
    var ref = cursorRow;
    for (var i = 0; i < existing.length; i++) {
      if (parseFloat(existing[i].dataset.start) > startTs) {
        ref = existing[i];
        break;
      }
    }
    inner.insertBefore(wrap, ref);
    return ref !== cursorRow;
  }

  function appendMessage(wrap) {
    empty.hidden = true;
    var wasNearBottom = isNearBottom();
    var prevTop = feed.scrollTop;
    var prevHeight = feed.scrollHeight;

    var insertedAbove = insertByTimestamp(wrap);

    if (wasNearBottom) {
      scrollToBottom(true);
    } else if (insertedAbove && wrap.offsetTop < prevTop) {
      // Something was inserted above what the reader is looking at, so the
      // content below it just shifted down by that much. Compensate, or
      // the view appears to jump while they're reading scrollback.
      feed.scrollTop = prevTop + (feed.scrollHeight - prevHeight);
    } else {
      jumpBtn.hidden = false;
    }
  }

  // Partials arrive under the same segment_id as the final that replaces
  // them, so the feed keys on it: the first event for an id creates the
  // bubble, later ones rewrite that same bubble instead of appending.
  //
  // The entry is dropped once the final lands, which both bounds this map
  // over a long session and is safe: VAD queues a segment's partials
  // strictly before its final on one ordered queue, and the hub replays
  // its backlog in order too, so nothing arrives for an id after its
  // final. A finalized bubble is therefore never rewritten.
  var liveBubbles = Object.create(null);

  function applyMessage(m) {
    var existing = liveBubbles[m.segment_id];
    if (existing) {
      existing.bubble.textContent = m.text;
      existing.time.textContent = formatTime(m.created_at);
      existing.wrap.classList.toggle("partial", !m.is_final);
      if (m.is_final) delete liveBubbles[m.segment_id];
      if (isNearBottom()) scrollToBottom(true);
      return;
    }

    var rendered = renderMessage(m);
    // Only in-progress bubbles are tracked; a final is never revised.
    if (!m.is_final) liveBubbles[m.segment_id] = rendered;
    appendMessage(rendered.wrap);
  }

  jumpBtn.addEventListener("click", function () {
    scrollToBottom(true);
  });

  // Kiosk has no mouse (see legend-bar comment) -- a touchscreen invites
  // stray taps, so this confirms before wiping anything.
  clearBtn.addEventListener("click", function () {
    if (!window.confirm("Clear all transcripts?")) return;
    fetch("/api/transcripts/clear", { method: "POST" }).catch(function () {});
    // Clears this tab's view immediately rather than waiting on the fetch
    // -- it's the only client (see app.py), so there's nothing to stay in
    // sync with, and a slow/failed POST shouldn't leave a stale feed.
    liveBubbles = Object.create(null);
    while (inner.firstChild && inner.firstChild !== cursorRow) {
      inner.removeChild(inner.firstChild);
    }
    empty.hidden = false;
    jumpBtn.hidden = true;
  });

  feed.addEventListener("scroll", function () {
    if (isNearBottom()) jumpBtn.hidden = true;
  });

  // ── live wall clock ───────────────────────────────────

  function tickClock() {
    clockEl.textContent = new Date().toLocaleTimeString([], { hour12: false });
  }
  tickClock();
  setInterval(tickClock, 1000);

  // ── transcript stream (SSE) ────────────────────────────
  // EventSource reconnects on its own; nothing to manage here beyond
  // rendering what arrives. Whether the *pipeline* is running is a
  // separate question, answered by /api/status below -- the SSE
  // connection itself stays open as long as the (same-process) server
  // does, regardless of whether the pipeline is started or stopped.

  var source = new EventSource("/api/transcripts/stream");
  source.onmessage = function (e) {
    applyMessage(JSON.parse(e.data));
  };

  // ── pipeline status poll ───────────────────────────────

  // Value helper: set text + which of ok/warn/dim (if any) applies.
  function setValue(valEl, text, state) {
    valEl.textContent = text;
    valEl.classList.toggle("ok", state === "ok");
    valEl.classList.toggle("warn", state === "warn");
    valEl.classList.toggle("dim", state === "dim");
  }

  // Freshness reads as seconds since the last packet on that channel.
  // Clamped at 0 because the router's clock is wall-clock (time.time()),
  // so a clock step could otherwise render a negative age.
  function formatAge(seconds) {
    if (seconds === null || seconds === undefined) return DASH;
    var s = Math.max(0, seconds);
    return s < 10 ? s.toFixed(1) + "s" : Math.round(s) + "s";
  }

  // Channel rows -- avatar-badge led, same visual language as the feed's
  // message avatars, with a trailing status dot for freshness.
  function renderChannels(channels) {
    if (!channels) {
      for (var cached in chanCache) {
        setValue(chanCache[cached].val, DASH, "dim");
        setDot(chanCache[cached].dot, "dim");
      }
      return;
    }
    // Built from the payload rather than hardcoded rx/tx: the channel set
    // is config-driven, so hardcoding would contradict the server.
    for (var id in channels) {
      var entry = chanCache[id];
      if (!entry) {
        var row = document.createElement("div");
        row.className = "srow";
        row.innerHTML =
          '<span class="ico ' + id + '">' + id.toUpperCase() + '</span>' +
          '<span class="body"><span class="name">' + id.toUpperCase() + ' channel</span>' +
          '<span class="sub val"></span></span>' +
          '<span class="dot"></span>';
        channelChips.appendChild(row);
        entry = chanCache[id] = { val: row.querySelector(".val"), dot: row.querySelector(".dot") };
      }
      var info = channels[id] || {};
      var seen = info.freshness_s !== null && info.freshness_s !== undefined;
      var state = info.stale ? "warn" : seen ? null : "dim";
      setValue(entry.val, formatAge(info.freshness_s), state);
      setDot(entry.dot, info.stale ? "warn" : seen ? "ok" : "dim");
    }
  }

  // Sets which of ok/warn/dim a trailing status dot shows (see .srow .dot
  // in console.css) -- distinct from setValue's warn/dim text coloring,
  // since a dot also needs the "healthy" state, which text never colors.
  function setDot(dotEl, state) {
    dotEl.classList.toggle("ok", state === "ok");
    dotEl.classList.toggle("warn", state === "warn");
    dotEl.classList.toggle("dim", state === "dim");
  }

  // One line per queue (ingest, routed, dump, segment_<channel>, ...) --
  // the set is config-driven (orchestrator.queue_depths omits queues that
  // were never built), so this can't be a fixed set of rows the way
  // MODULES is. Rebuilt each poll rather than diffed in place: it's a
  // handful of short text nodes, not worth a cache keyed by queue name.
  function renderQueues(depths) {
    var names = depths ? Object.keys(depths) : [];
    if (!names.length) {
      queueVal.innerHTML = '<span class="qline dim">' + DASH + '</span>';
      return;
    }
    var html = "";
    for (var i = 0; i < names.length; i++) {
      html += '<span class="qline"><span class="qname">' + names[i] +
        '</span><span class="qval">' + depths[names[i]] + '</span></span>';
    }
    queueVal.innerHTML = html;
  }

  // Latency spans microseconds (router repacketize) to hundreds of
  // milliseconds (STT inference), so fixed units would render most stages
  // as 0. Scale per value instead.
  function formatLatency(seconds) {
    if (seconds === null || seconds === undefined) return DASH;
    if (seconds < 0.001) return Math.round(seconds * 1e6) + "µs";
    if (seconds < 1) return (seconds * 1000).toFixed(seconds < 0.01 ? 2 : 0) + "ms";
    return seconds.toFixed(2) + "s";
  }

  // One number per module on a kiosk this size, so show the slowest
  // channel: it's the one that would breach a latency budget first.
  function worstLatency(byChannel) {
    if (!byChannel) return null;
    var worst = null;
    for (var id in byChannel) {
      if (worst === null || byChannel[id] > worst) worst = byChannel[id];
    }
    return worst;
  }

  function renderModules(data) {
    var workers = data.workers || {};
    var latencies = data.latencies;
    for (var i = 0; i < MODULES.length; i++) {
      var mod = MODULES[i];
      var entry = modCache[mod.worker];
      if (!entry) {
        var row = document.createElement("div");
        row.className = "srow mrow";
        row.innerHTML =
          '<span class="dot"></span>' +
          '<span class="body"><span class="name">' + mod.label + '</span>' +
          '<span class="sub lat"></span></span>';
        modulesEl.appendChild(row);
        entry = modCache[mod.worker] = { row: row, lat: row.querySelector(".lat") };
      }
      // State is always live, even when the metrics tick hasn't landed.
      var state = workers[mod.worker] || null;
      entry.row.classList.toggle("running", state === "running");
      entry.row.classList.toggle("restarting", state === "restarting");
      entry.row.classList.toggle("mod-degraded", state === "degraded");
      entry.row.classList.toggle("stopped", state === "stopped");
      entry.row.classList.toggle("dim", !state);
      // Latency is snapshot-derived, so it can be absent while state isn't.
      // mod.channel (STT rows) reads that one channel's own number
      // directly -- NOT worstLatency(), which would fold every channel
      // into one value and make two different-channel rows show the
      // same (misleading) number. Modules without a channel still use
      // worstLatency() across whatever channels report to that stage.
      var latencyValue = null;
      if (mod.latency && latencies && latencies[mod.latency]) {
        latencyValue = mod.channel
          ? latencies[mod.latency][mod.channel]
          : worstLatency(latencies[mod.latency]);
      }
      entry.lat.textContent = latencyValue != null ? formatLatency(latencyValue) : "";
    }
  }

  function renderStrip(data) {
    var mqtt = data.mqtt_connected;
    setValue(mqttVal,
      mqtt === true ? "connected" : mqtt === false ? "down" : DASH,
      mqtt === true ? "ok" : "dim");

    renderQueues(data.queue_depths);

    var metrics = data.metrics || {};
    var ok = metrics.state === "ok";
    setValue(metricsVal,
      metrics.state === "disabled" ? "off"
        : metrics.state === "pending" ? "waiting"
        : ok ? formatAge(metrics.age_s) + " ago" : DASH,
      ok ? null : "dim");

    renderChannels(data.channels);
    renderModules(data);
  }

  // Three states, not two: a warning pipeline is still running (some
  // workers healthy) but a worker has crash-looped past its restart budget,
  // or MQTT is unreachable -- so it must read distinctly from both "live"
  // and "stopped". Keyed off the server's `status` verdict, with a
  // `running` fallback for the fetch-failure payload below.
  function setStatus(data) {
    var verdict = data.status || (data.running ? "ok" : "down");
    var stopped = verdict === "down";
    var warn = verdict === "warn";
    livePill.classList.toggle("stopped", stopped);
    livePill.classList.toggle("degraded", warn);
    blink.classList.toggle("stopped", stopped);

    var mqttDown = data.mqtt_connected === false;
    liveLabel.textContent = stopped
      ? "stopped"
      : data.degraded
      ? "degraded"
      : mqttDown
      ? "mqtt down"
      : "live";
    cursorText.textContent = stopped
      ? "pipeline stopped"
      : data.degraded
      ? "running — worker degraded"
      : mqttDown
      ? "running — mqtt disconnected"
      : "waiting for next line";

    renderStrip(data);
  }

  function pollStatus() {
    fetch("/api/status")
      .then(function (r) { return r.json(); })
      .then(function (data) { setStatus(data); })
      // No payload to render on a failed poll: blank the strip rather than
      // leaving stale values that look current.
      .catch(function () { setStatus({ running: false }); });
  }
  pollStatus();
  setInterval(pollStatus, 3000);
})();
