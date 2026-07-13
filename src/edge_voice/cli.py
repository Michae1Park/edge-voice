"""
Entry point for `edge-voice` console script: parses CLI args, loads
config, and starts the pipeline (and optionally the web UI) as configured.
"""

import argparse
import logging

from edge_voice.config.settings import Settings
from edge_voice.pipeline.orchestrator import PipelineOrchestrator

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
            orchestrator.run_with_timer(duration_s=args.run_secs)
        else:
            logger.info("Running until Ctrl-C...")
            orchestrator.run()
    finally:
        logger.info("Final status: %s", orchestrator.get_status())


if __name__ == "__main__":
    main()
