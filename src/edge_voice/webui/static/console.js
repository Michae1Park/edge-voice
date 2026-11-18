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

  var MODULES = [
    { worker: "MqttAudioIngest", label: "Ingest", latency: null },
    { worker: "ChannelRouter", label: "Router", latency: "router" },
    { worker: "VADWorker", label: "VAD", latency: "vad_silero" },
    { worker: "STTWorker-rx", label: "STT RX", latency: "stt", channel: "rx" },
    { worker: "STTWorker-tx", label: "STT TX", latency: "stt", channel: "tx" },
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

  function renderMessage(m) {
    var wrap = document.createElement("div");
    wrap.className = "msg " + m.channel_id + (m.is_final ? "" : " partial");
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
      feed.scrollTop = prevTop + (feed.scrollHeight - prevHeight);
    } else {
      jumpBtn.hidden = false;
    }
  }

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
    if (!m.is_final) liveBubbles[m.segment_id] = rendered;
    appendMessage(rendered.wrap);
  }

  jumpBtn.addEventListener("click", function () {
    scrollToBottom(true);
  });

  clearBtn.addEventListener("click", function () {
    if (!window.confirm("Clear all transcripts?")) return;
    fetch("/api/transcripts/clear", { method: "POST" }).catch(function () {});
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


  function tickClock() {
    clockEl.textContent = new Date().toLocaleTimeString([], { hour12: false });
  }
  tickClock();
  setInterval(tickClock, 1000);


  var source = new EventSource("/api/transcripts/stream");
  source.onmessage = function (e) {
    applyMessage(JSON.parse(e.data));
  };


  function setValue(valEl, text, state) {
    valEl.textContent = text;
    valEl.classList.toggle("ok", state === "ok");
    valEl.classList.toggle("warn", state === "warn");
    valEl.classList.toggle("dim", state === "dim");
  }

  function formatAge(seconds) {
    if (seconds === null || seconds === undefined) return DASH;
    var s = Math.max(0, seconds);
    return s < 10 ? s.toFixed(1) + "s" : Math.round(s) + "s";
  }

  function renderChannels(channels) {
    if (!channels) {
      for (var cached in chanCache) {
        setValue(chanCache[cached].val, DASH, "dim");
        setDot(chanCache[cached].dot, "dim");
      }
      return;
    }
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

  function setDot(dotEl, state) {
    dotEl.classList.toggle("ok", state === "ok");
    dotEl.classList.toggle("warn", state === "warn");
    dotEl.classList.toggle("dim", state === "dim");
  }

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

  function formatLatency(seconds) {
    if (seconds === null || seconds === undefined) return DASH;
    if (seconds < 0.001) return Math.round(seconds * 1e6) + "µs";
    if (seconds < 1) return (seconds * 1000).toFixed(seconds < 0.01 ? 2 : 0) + "ms";
    return seconds.toFixed(2) + "s";
  }

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
      var state = workers[mod.worker] || null;
      entry.row.classList.toggle("running", state === "running");
      entry.row.classList.toggle("restarting", state === "restarting");
      entry.row.classList.toggle("mod-degraded", state === "degraded");
      entry.row.classList.toggle("stopped", state === "stopped");
      entry.row.classList.toggle("dim", !state);
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
      .catch(function () { setStatus({ running: false }); });
  }
  pollStatus();
  setInterval(pollStatus, 3000);
})();
