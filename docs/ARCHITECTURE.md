# edge-voice Architecture (v0.1)

**Status:** Draft

## System Overview

```text
MQTT audio channels
        │
        ▼
  Channel Router
        │
        ▼
 Per-channel Silero VAD
        │
        ▼
 Shared Moonshine STT
        │
        ▼
 Transcript Events
        │
        ├─ Logs
        ├─ WebSocket UI
        └─ Future persistence
```

## 1. Problem Statement

`edge-voice` transcribes two-party phone calls in near real time on resource-constrained edge devices such as Raspberry Pi 5 and Jetson. Korean is the default language; Moonshine also supports Arabic, English, Spanish, Japanese, Ukrainian, Vietnamese, and Chinese, selected via configuration.

Each call leg arrives as a separate MQTT audio stream. The system must:

* Produce ordered, channel-attributed transcripts.
* Operate reliably despite transient failures.
* Provide enough observability to diagnose issues without direct shell access.
* Run efficiently on limited CPU and memory resources.

## 2. Non-Goals (v0.1)

The following are intentionally out of scope for the initial release:

* Docker packaging.
* External metrics systems (Prometheus, Grafana, etc.).
* Multi-tenant deployments.
* Speaker diarization beyond channel attribution.
* Transcript persistence beyond logs and live streaming.

## 3. Architecture

### Core Decision: Per-Channel VAD, Shared STT

The system runs one Silero VAD model instance per channel, but a single shared Moonshine STT instance across all channels.

This differs from earlier prototypes that ran fully independent pipelines per audio source, and from an earlier version of this design that used one shared VAD instance for all channels.

**VAD is per-channel, not shared.** A single shared model was tried first and reverted: `VADIterator` only holds the segmentation state machine, the LSTM hidden state used for inference lives inside the model instance itself. Two channels interleaving packets against one shared model corrupted each other's hidden state — measured as doubled, garbage segment counts against recorded call fixtures. Each channel therefore gets its own model instance (one `VADIterator` + one Silero model per `channel_id`), which also removes any need for cross-channel locking since a channel's state is only ever touched by that channel's packets. The extra cost (~4MB, ~0.06s load) per channel is negligible next to correctness.

**STT remains shared.** Unlike VAD, Moonshine's `Transcriber.start()`/`stop()` fully resets its decoder state between segments (verified byte-for-byte against a fresh instance per segment), and the STT worker only ever processes one finalized segment at a time regardless of which channel it came from — there is no concurrent access to isolate. Sharing one instance also fits the turn-taking nature of phone conversations: the previous speaker's context shouldn't bleed into the next line regardless of channel, and it avoids holding a second copy of the model in memory (~175MB saved).

Reasons this split still favors resource efficiency and conversation semantics over one independent pipeline per audio source:

1. **Resource efficiency**

   * A full independent VAD+STT pipeline per channel would duplicate CPU and memory usage well beyond the per-channel VAD model's small footprint.
   * Phone conversations are typically turn-based, making parallel STT instances unnecessary.

2. **Conversation semantics**

   * Separate call legs represent one conversation rather than unrelated audio streams.
   * A shared STT stage preserves conversation ordering and simplifies downstream processing.

### Routing Model

```text
MQTT channel 1 ──┐               ┌─▶ VAD (channel 1) ─┐
                 ├─▶ Router ─────┤                     ├─▶ Shared STT ─▶ Transcript
MQTT channel 2 ──┘               └─▶ VAD (channel 2) ─┘
```

* Incoming audio packets are tagged with `channel_id`.
* Packets are placed onto a shared ingest queue.
* VAD processes packets serially (one worker thread) but against independent per-channel model instances and state — no cross-channel interference, no locking required.
* Finalized speech segments are placed onto a shared STT queue.
* STT processes one segment at a time, against one shared model instance.
* Channel attribution is preserved throughout the pipeline.

### Tradeoffs

Overlapping speech is processed sequentially rather than simultaneously.

This favors resource efficiency and implementation simplicity over perfect concurrent transcription. For typical phone-call workloads this is considered an acceptable tradeoff.

If overlap-heavy workloads become common, additional STT workers can be introduced without redesigning the routing architecture.

## 4. Concurrency Model

The pipeline consists of three long-lived workers connected by queues.

### MQTT Ingest

* Receives audio packets from MQTT topics.
* Tags packets with channel metadata.
* Pushes packets onto the ingest queue.
* Performs no expensive processing.

### VAD Worker

* Consumes packets from the ingest queue.
* Maintains per-channel VAD state.
* Produces finalized speech segments.

### STT Worker

* Consumes finalized segments.
* Runs Moonshine inference.
* Emits transcript events.

This producer/consumer architecture prevents transcription latency from blocking audio ingestion.

## 5. Reliability

Reliability is a primary design goal.

### Worker Supervision

Workers are supervised and automatically restarted after unexpected failures.

Repeated failures are surfaced as degraded health rather than silently retried forever.

### MQTT Recovery

MQTT connections automatically reconnect using exponential backoff.

Connection status is exposed through health reporting.

### Fault Isolation

Malformed audio packets and inference failures are logged and discarded without terminating the pipeline.

### Backpressure

Queues are bounded.

Queue depth is tracked and surfaced through health and metrics reporting to detect overload conditions before memory usage becomes problematic.

## 6. Configuration

Configuration is managed through a single typed settings model.

Configuration sources are applied in the following order:

1. Built-in defaults
2. Default configuration file
3. Local device overrides
4. Environment variables

All configuration updates are validated before being applied.

## 7. Observability

### Structured Logging

Structured JSON logs are the primary operational interface.

Log events include contextual information such as:

* Channel ID
* Pipeline stage
* Segment ID

This allows a segment's lifecycle to be traced across the pipeline.

### Metrics

Metrics are emitted as structured log events and aggregated in memory.

Examples include:

* STT inference latency
* Queue depth
* Worker restart counts
* MQTT connection status

### Health Reporting

A health endpoint exposes:

* Overall system status
* Worker status and restart counts
* Queue depths
* MQTT connectivity
* Per-channel activity freshness

## 8. Web UI

The web interface provides:

### Control

* Start pipeline
* Stop pipeline
* Restart workers

### Configuration

* View effective configuration
* Edit local configuration
* Validate changes before applying

### Live Monitoring

* Real-time transcript stream
* Health dashboard
* Metrics dashboard

The initial implementation uses server-rendered pages and WebSockets to minimize complexity and resource usage.

## 9. Testing

Testing is performed at two levels.

### Unit Tests

Verify isolated component behavior using mocked dependencies.

Examples include:

* Channel routing
* VAD segmentation logic
* Configuration validation

### Integration Tests

Exercise multiple real components together using realistic MQTT and audio fixtures.

Examples include:

* MQTT → Router → VAD
* End-to-end transcription pipeline

Continuous integration validates correctness, while performance validation is performed on target hardware.

## 10. Deferred Decisions

The following decisions are intentionally postponed until implementation experience is available:

* Threads versus asyncio for pipeline execution.
* Transcript persistence backend.
* Enhanced overlap handling.
* Deployment packaging and containerization.
