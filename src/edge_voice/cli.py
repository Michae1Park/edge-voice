"""
Entry point for `edge-voice` console script: parses CLI args, loads
config, and starts the pipeline (and optionally the web UI) as configured.
"""

import argparse
import logging

import uvicorn

from edge_voice.config.settings import Settings
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
    return parser.parse_args(argv)


def setup_logging(debug: bool = False) -> None:
    """
    This is the top level logger config shared across all modules
    """
    level = logging.DEBUG if debug else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
    )


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    setup_logging(debug=args.debug)

    settings = Settings.load()
    orchestrator = PipelineOrchestrator(settings)

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
        logger.info("Final status: %s", orchestrator.get_status())


if __name__ == "__main__":
    main()
