"""
Entry point for `edge-voice` console script: parses CLI args, loads
config, and starts the pipeline (and optionally the web UI) as configured.
"""

import argparse
import logging

from edge_voice.config.settings import Settings, SourceSettings
from edge_voice.pipeline.orchestrator import PipelineOrchestrator

logger = logging.getLogger(__name__)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="edge-voice",
        description="Real-time dual-channel phone-call transcription for edge devices",
    )
    parser.add_argument(
        "--channels",
        nargs="*",
        choices=["rx", "tx"],
        help="Override channels to listen on (default: all from config)",
    )
    parser.add_argument(
        "--run-secs",
        type=int,
        default=30,
        help="Run duration in seconds (0 = until Ctrl-C)",
    )
    parser.add_argument(
        "--config",
        type=str,
        default=None,
        help="Path to local YAML config override file",
    )
    parser.add_argument(
        "--with-ui",
        action="store_true",
        default=False,
        help="Start web UI alongside pipeline (Milestone 7)",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        default=False,
        help="Enable debug logging",
    )
    parser.add_argument(
        "--wav-file",
        type=str,
        default=None,
        help="Path to WAV file for audio source (replaces microphone)",
    )
    return parser.parse_args(argv)


def setup_logging(debug: bool = False) -> None:
    """Configure structlog/console logging from args."""
    level = logging.DEBUG if debug else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
    )


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    setup_logging(debug=args.debug)

    logger.info("Starting edge-voice %s", "in debug mode" if args.debug else "")

    settings = Settings.load()

    if args.wav_file:
        settings = Settings(
            source=SourceSettings(
                default_audio=args.wav_file,
                sample_rate=settings.source.sample_rate,
            ),
        )

    orchestrator = PipelineOrchestrator(settings)

    if args.run_secs > 0:
        orchestrator.run_with_timer(duration_s=args.run_secs)
    else:
        logger.info("Running until Ctrl-C...")
        try:
            orchestrator.build()
            orchestrator.start()
            orchestrator.wait()
        except KeyboardInterrupt:
            logger.info("Ctrl-C received, shutting down...")
        finally:
            orchestrator.stop()
            orchestrator.wait()

    status = orchestrator.get_status()
    logger.info("Final status: %s", status)


if __name__ == "__main__":
    main()
