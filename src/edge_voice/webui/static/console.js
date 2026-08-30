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

  // One ledger row per utterance: time, channel badge, text -- reversed
  // (row-reverse, via the .tx class) for tx, so the two sides read from
  // the outside in. Channel identity rests on the badge letters and that
  // reversal, not on colour -- see the legend note in console.html.
  function renderMessage(m) {
    var row = document.createElement("div");
    row.className = "row " + m.channel_id + (m.is_final ? "" : " partial");
    // When the audio was spoken, not when the transcript arrived -- see
    // insertByTimestamp. `start` is real wall-clock (AudioPacket.timestamp
    // propagates unchanged from MQTT ingest), so it's directly comparable
    // across channels.
    row.dataset.start = String(m.start);

    var time = document.createElement("div");
    time.className = "time";
    time.textContent = formatTime(m.created_at);

    var badge = document.createElement("div");
    badge.className = "badge";
    badge.textContent = m.channel_id.toUpperCase();

    var text = document.createElement("div");
    text.className = "text";
    text.textContent = m.text;

    row.appendChild(time);
    row.appendChild(badge);
    row.appendChild(text);
    return { wrap: row, text: text, time: time };
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
  function insertByTimestamp(row) {
    var startTs = parseFloat(row.dataset.start);
    var existing = inner.querySelectorAll(".row[data-start]");
    var ref = cursorRow;
    for (var i = 0; i < existing.length; i++) {
      if (parseFloat(existing[i].dataset.start) > startTs) {
        ref = existing[i];
        break;
      }
    }
    inner.insertBefore(row, ref);
    return ref !== cursorRow;
  }

  function appendMessage(row) {
    empty.hidden = true;
    var wasNearBottom = isNearBottom();
    var prevTop = feed.scrollTop;
    var prevHeight = feed.scrollHeight;

    var insertedAbove = insertByTimestamp(row);

    if (wasNearBottom) {
      scrollToBottom(true);
    } else if (insertedAbove && row.offsetTop < prevTop) {
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
  // row, later ones rewrite that same row instead of appending.
  //
  // The entry is dropped once the final lands, which both bounds this map
  // over a long session and is safe: VAD queues a segment's partials
  // strictly before its final on one ordered queue, and the hub replays
  // its backlog in order too, so nothing arrives for an id after its
  // final. A finalized row is therefore never rewritten.
  var liveRows = Object.create(null);

  function applyMessage(m) {
    var existing = liveRows[m.segment_id];
    if (existing) {
      existing.text.textContent = m.text;
      existing.time.textContent = formatTime(m.created_at);
      existing.wrap.classList.toggle("partial", !m.is_final);
      if (m.is_final) delete liveRows[m.segment_id];
      if (isNearBottom()) scrollToBottom(true);
      return;
    }

    var rendered = renderMessage(m);
    // Only in-progress rows are tracked; a final is never revised.
    if (!m.is_final) liveRows[m.segment_id] = rendered;
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
    liveRows = Object.create(null);
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

  // Value helper: set text + which of warn/dim (if either) applies.
  function setValue(valEl, text, state) {
    valEl.textContent = text;
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

  function renderChannels(channels) {
    if (!channels) {
      for (var cached in chanCache) {
        setValue(chanCache[cached].val, DASH, "dim");
      }
      return;
    }
    // Built from the payload rather than hardcoded rx/tx: the channel set
    // is config-driven, so hardcoding would contradict the server.
    for (var id in channels) {
      var entry = chanCache[id];
      if (!entry) {
        var cell = document.createElement("span");
        cell.className = "cell";
        cell.innerHTML = '<span class="sc">' + id.toUpperCase() + ' Freshness</span><span class="v"></span>';
        channelChips.appendChild(cell);
        entry = chanCache[id] = { val: cell.querySelector(".v") };
      }
      var info = channels[id] || {};
      var seen = info.freshness_s !== null && info.freshness_s !== undefined;
      setValue(entry.val, formatAge(info.freshness_s), info.stale ? "warn" : seen ? null : "dim");
    }
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
        var cell = document.createElement("span");
        cell.className = "mcell";
        cell.innerHTML =
          '<span class="name"><span class="dot"></span>' + mod.label +
          '</span><span class="lat"></span>';
        modulesEl.appendChild(cell);
        entry = modCache[mod.worker] = { cell: cell, lat: cell.querySelector(".lat") };
      }
      // State is always live, even when the metrics tick hasn't landed.
      var state = workers[mod.worker] || null;
      entry.cell.classList.toggle("running", state === "running");
      entry.cell.classList.toggle("restarting", state === "restarting");
      entry.cell.classList.toggle("mod-degraded", state === "degraded");
      entry.cell.classList.toggle("stopped", state === "stopped");
      entry.cell.classList.toggle("dim", !state);
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
      mqtt === false ? "warn" : mqtt === true ? null : "dim");

    var depths = data.queue_depths;
    var parts = [];
    for (var name in depths || {}) parts.push(name + ":" + depths[name]);
    setValue(queueVal, parts.length ? parts.join(" ") : DASH, parts.length ? null : "dim");

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
