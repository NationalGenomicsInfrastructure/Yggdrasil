import argparse
import asyncio

from lib.core_utils.config_loader import ConfigLoader

# import logging
from lib.core_utils.logging_utils import configure_logging, custom_logger
from lib.core_utils.ygg_session import YggSession
from lib.core_utils.yggdrasil_core import YggdrasilCore
from yggdrasil.logo_utils import print_logo

try:
    from yggdrasil import __version__
except ImportError:
    __version__ = "unknown"


def main():
    parser = argparse.ArgumentParser(prog="yggdrasil")
    # Global flags
    parser.add_argument(
        "--dev",
        action="store_true",
        help="Enable development mode (sets debug logging, dev-mode behavior)",
    )
    parser.add_argument(
        "--silent",
        action="store_true",
        help="Silent mode - log to file only, no console output",
    )
    parser.add_argument(
        "--version",
        action="store_true",
        help="Show version information",
    )

    sub = parser.add_subparsers(dest="mode", required=False)

    # Daemon mode
    sub.add_parser("daemon", help="Start the long-running service")

    # One‑off mode
    run = sub.add_parser(
        "run-doc",
        help="Process a single document and exit",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Create plan only (for external approval):
  yggdrasil run-doc <doc_id> --plan-only

  # Create and execute plan (auto_run=True by default, but blocking if approval is needed):
  yggdrasil run-doc <doc_id> --run-once

  # Overwrite existing plan:
  yggdrasil run-doc <doc_id> --plan-only --force
        """,
    )
    run.add_argument("doc_id", help="Project document ID to process")

    # Mode selection (mutually exclusive group)
    mode_group = run.add_mutually_exclusive_group(required=False)
    mode_group.add_argument(
        "-p",
        "--plan-only",
        action="store_true",
        help="Create plan only (no execution); sets execution_authority='daemon'",
    )
    mode_group.add_argument(
        "-r",
        "--run-once",
        action="store_true",
        help="Create and execute plan via scoped watcher",
    )

    # Other flags
    run.add_argument(
        "-f",
        "--force",
        action="store_true",
        help="Overwrite existing plan without confirmation",
    )
    run.add_argument(
        "-m",
        "--manual-submit",
        action="store_true",
        help="Force manual HPC submission for this run-doc invocation",
    )
    run.add_argument(
        "-t",
        "--timeout",
        type=int,
        default=1800,
        metavar="SECONDS",
        help="Timeout for approval wait in seconds (default: 1800)",
    )

    args = parser.parse_args()

    # Handle --version flag
    if args.version:
        print_logo(version=__version__)
        return

    # Handle case where no subcommand is provided (show help)
    if args.mode is None:
        parser.print_help()
        return

    # 1) Initialize dev mode early (affects config loader, logging, etc.)
    YggSession.init_dev_mode(args.dev)

    # 2) Configure logging based on flags
    if args.silent:
        # Silent mode: only log to file
        configure_logging(debug=False, console=False)
    elif args.dev:
        # Development mode: debug level to console + file
        configure_logging(debug=True, console=True)
    else:
        # Normal mode: info level to console + file
        configure_logging(debug=False, console=True)

    logging = custom_logger("Yggdrasil")

    # 3) Adjust root logger
    # logging.basicConfig(
    #     level=logging.DEBUG if args.dev else logging.INFO,
    #     format="[%(name)s] %(message)s",
    #     handlers=[RichHandler(show_time=True, show_level=True, markup=True)],
    # )
    # os.environ["PREFECT_LOGGING_LEVEL"] = "DEBUG" if args.dev else "INFO"

    logging.debug("Yggdrasil: Starting up...")

    # 4) Prepare core (load config, init core, discover realms)
    config = ConfigLoader().load_config("main.json")
    core = YggdrasilCore(config)
    core.setup_realms()

    if args.mode == "daemon":
        if getattr(args, "manual_submit", False):
            parser.error("The --manual-submit flag is only valid in run-doc mode.")

        # (future)Daemon: set up watchers and run forever
        core.setup_watchers()
        try:
            asyncio.run(core.start())
        except KeyboardInterrupt:
            logging.warning("[bold red blink] Shutting down Yggdrasil daemon... [/]")
            try:
                asyncio.run(core.stop())
            except (asyncio.CancelledError, RuntimeError) as e:
                # CancelledError: Tasks were cancelled during shutdown (expected)
                # RuntimeError: Event loop issues during cleanup (can be ignored)
                logging.debug(f"Shutdown exception (expected): {e}")
            logging.info("Yggdrasil daemon stopped.")

    elif args.mode == "run-doc":
        # Validate mode selection
        if not args.plan_only and not args.run_once:
            # Default to plan-only with notice
            logging.info(
                "No mode specified; defaulting to --plan-only. "
                "Use --run-once to execute immediately."
            )
            args.plan_only = True

        # Initialize session flags
        YggSession.init_manual_submit(args.manual_submit)

        # Dispatch to appropriate handler
        if args.plan_only:
            result = core.create_plan_from_doc(
                doc_id=args.doc_id,
                force_overwrite=args.force,
            )
            if result is None:
                # Plan creation failed or aborted
                raise SystemExit(1)
        else:  # --run-once
            exit_code = core.run_once_with_watcher(
                doc_id=args.doc_id,
                force_overwrite=args.force,
                timeout_seconds=args.timeout,
            )
            raise SystemExit(exit_code)


if __name__ == "__main__":
    main()
