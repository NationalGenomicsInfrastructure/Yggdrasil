import argparse
import asyncio
import signal
from pathlib import Path

from yggdrasil.config.loader import ConfigLoader
from yggdrasil.config.session import YggSession
from yggdrasil.daemon.core import YggdrasilCore
from yggdrasil.daemon.lock import DaemonLock, DaemonLockError
from yggdrasil.errors import (
    ExternalSystemUnavailableError,
    InternalStorageConfigurationError,
)
from yggdrasil.logging_utils import configure_logging, custom_logger
from yggdrasil.logo_utils import print_logo
from yggdrasil.watchers.config_validation import WatcherConfigurationError

try:
    from yggdrasil import __version__
except ImportError:
    __version__ = "unknown"


#: Signals that start the daemon's controlled shutdown.
_SHUTDOWN_SIGNALS = (signal.SIGTERM, signal.SIGINT)


async def _run_daemon(core: YggdrasilCore) -> None:
    """Run the daemon until SIGTERM/SIGINT, then stop it on the same event loop.

    The signals are handled by the event loop (``loop.add_signal_handler``),
    which runs the handler as an ordinary callback between task steps. Nothing
    is raised from a signal handler, so a signal never interrupts a coroutine
    or the loop's own code partway through. As PID 1 in a container a signal
    without an installed handler is ignored by the kernel, so without the
    SIGTERM handler every docker/systemd/k8s stop would end in SIGKILL.

    ``core.stop()`` runs on this loop, which owns the watchers' tasks and
    asyncio objects, whether a signal arrived or ``core.start()`` returned on
    its own. A ``core.start()`` still running afterwards is then cancelled:
    a watcher blocked in an idle ``_changes`` longpoll does not end its task
    when asked to stop, and cancelling it lets the daemon shut down without
    waiting for that task. The longpoll's worker thread itself cannot be
    cancelled; ``asyncio.run()`` still waits for it before returning.

    Args:
        core: The configured core, with realms and watchers set up.

    Raises:
        Exception: Whatever ``core.start()`` raised, after the stop has run.
    """
    logger = custom_logger(__name__)
    loop = asyncio.get_running_loop()
    stop_requested = asyncio.Event()

    def _request_stop() -> None:
        """Start the controlled shutdown (later signals change nothing)."""
        if not stop_requested.is_set():
            logger.warning("[bold red blink] Shutting down Yggdrasil daemon... [/]")
            stop_requested.set()

    for sig in _SHUTDOWN_SIGNALS:
        loop.add_signal_handler(sig, _request_stop)

    start_task = asyncio.create_task(core.start())
    stop_wait = asyncio.create_task(stop_requested.wait())
    try:
        await asyncio.wait({start_task, stop_wait}, return_when=asyncio.FIRST_COMPLETED)
    finally:
        stop_wait.cancel()
        await core.stop()
        if not start_task.done():
            start_task.cancel()
        try:
            await start_task
        except asyncio.CancelledError:
            if not start_task.cancelled():
                raise


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

    logger = custom_logger(__name__)

    # 3) Adjust root logger
    # logging.basicConfig(
    #     level=logging.DEBUG if args.dev else logging.INFO,
    #     format="[%(name)s] %(message)s",
    #     handlers=[RichHandler(show_time=True, show_level=True, markup=True)],
    # )
    # os.environ["PREFECT_LOGGING_LEVEL"] = "DEBUG" if args.dev else "INFO"

    logger.debug("Yggdrasil: Starting up...")

    # 4) Load config before dispatching to the selected mode
    config_loader = ConfigLoader()
    config = config_loader.load_config("main.json")
    config_path = config_loader.loaded_path
    if not isinstance(config_path, str | Path):
        config_path = None

    try:
        if args.mode == "daemon":
            if getattr(args, "manual_submit", False):
                parser.error("The --manual-submit flag is only valid in run-doc mode.")

            with DaemonLock.acquire(
                dev_mode=args.dev,
                config_path=config_path,
            ):
                if config_path is not None:
                    core = YggdrasilCore(config, config_path=config_path)
                else:
                    core = YggdrasilCore(config)
                core.setup_realms()
                core.setup_watchers()

                asyncio.run(_run_daemon(core))
                logger.info("Yggdrasil daemon stopped.")

        elif args.mode == "run-doc":
            if config_path is not None:
                core = YggdrasilCore(config, config_path=config_path)
            else:
                core = YggdrasilCore(config)
            core.setup_realms()

            # Validate mode selection
            if not args.plan_only and not args.run_once:
                # Default to plan-only with notice
                logger.info(
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
                if not result:
                    # Plan creation failed or aborted
                    raise SystemExit(1)
            else:  # --run-once
                exit_code = core.run_once_with_watcher(
                    doc_id=args.doc_id,
                    force_overwrite=args.force,
                    timeout_seconds=args.timeout,
                )
                raise SystemExit(exit_code)

    except (
        WatcherConfigurationError,
        ExternalSystemUnavailableError,
        InternalStorageConfigurationError,
    ) as e:
        # Environment/config problems, not bugs: one concise operator-facing
        # line; full traceback only at debug level (enabled by --dev).
        # WatcherConfigurationError embeds its config path itself; for
        # connectivity errors the resolved path is only known here.
        if isinstance(e, ExternalSystemUnavailableError) and config_path is not None:
            logger.error("%s (config file: %s)", e, config_path)
        else:
            logger.error("%s", e)
        logger.debug("Startup failure detail:", exc_info=True)
        raise SystemExit(1) from None
    except DaemonLockError as e:
        logger.error(
            "Another local Yggdrasil daemon is already running in %s mode. "
            "Lock path: %s. Existing metadata: %s. "
            "(Prod and dev daemons may run side by side; duplicates of the "
            "same mode may not.)",
            "dev" if args.dev else "prod",
            e.lock_path,
            e.existing_metadata,
        )
        raise SystemExit(1) from e


if __name__ == "__main__":
    main()
