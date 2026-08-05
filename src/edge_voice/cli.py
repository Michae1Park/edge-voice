"""
Entry point for `edge-voice` console script: parses CLI args, loads
config, and starts the pipeline (and optionally the web UI) as configured.
"""

import argparse
import logging
import signal
import subprocess
import sys

import uvicorn

from edge_voice.config.settings import Settings
from edge_voice.observability.logging import configure_logging
from edge_voice.pipeline.orchestrator import PipelineOrchestrator
from edge_voice.webui.app import create_app

logger = logging.getLogger(__name__)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="edge-voice",
        description="Real-time dual-channel phone-call transcription for edge devices",
    )
    parser.add_argument(
        "--run-secs", type=int, default=0, help="Run duration in seconds (0 = until Ctrl-C)"
    )
    parser.add_argument("--debug", action="store_true", default=False, help="Enable debug logging")
    parser.add_argument(
        "--mic",
        action="store_true",
        default=False,
        help="Also launch a live mic capture (mic_source.py) as a subprocess, "
        "publishing to the rx channel over MQTT",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)

    settings = Settings.load()
    # Field is `logging_` (alias "logging") -- Settings avoids a plain
    # `logging` attribute name on the model.
    configure_logging(settings.logging_, debug=args.debug)

    orchestrator = PipelineOrchestrator(settings)

    # A live mic is a separate process publishing over MQTT, same as a real
    # call leg -- see mic_source.py's docstring. --mic just spawns it for
    # convenience rather than folding capture into the orchestrator.
    mic_process: subprocess.Popen | None = None
    if args.mic:
        logger.info(
            "Starting mic capture subprocess (broker=%s:%s)",
            settings.mqtt.broker_host,
            settings.mqtt.broker_port,
        )
        mic_process = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "edge_voice.utils.audio_generation.mic_source",
                "--broker",
                settings.mqtt.broker_host,
                "--port",
                str(settings.mqtt.broker_port),
            ]
        )

    try:
        if args.run_secs > 0:
            # Headless, bounded-duration run -- no web UI. Used by
            # tests/CI (tests/test_pipeline_integration.py) where nothing
            # binds a port or needs a browser.
            orchestrator.run_with_timer(duration_s=args.run_secs)
        else:
            # Default path: the kiosk UI (see docs/BUILDPLAN.md Milestone 5).
            # uvicorn.run() blocks in this thread and handles Ctrl-C itself;
            # the pipeline's own worker threads run in the background the
            # whole time, started here rather than via orchestrator.run()
            # (which has its own blocking wait loop -- redundant with
            # uvicorn's).
            orchestrator.build()
            orchestrator.start()
            app = create_app(orchestrator)
            try:
                logger.info(
                    "Serving UI on http://%s:%s (Ctrl-C to stop)",
                    settings.webui.host,
                    settings.webui.port,
                )
                uvicorn.run(app, host=settings.webui.host, port=settings.webui.port)
            finally:
                orchestrator.stop()
                orchestrator.wait()
    finally:
        if mic_process is not None:
            mic_process.send_signal(signal.SIGINT)
            mic_process.wait()
        logger.info("Final status: %s", orchestrator.get_status())


if __name__ == "__main__":
    main()
