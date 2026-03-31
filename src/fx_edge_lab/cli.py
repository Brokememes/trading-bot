from __future__ import annotations

import argparse
from pathlib import Path

from .crypto_analysis import summarize_crypto_database
from .crypto_dashboard import serve_crypto_dashboard
from .crypto_service import capture_crypto_research, dump_crypto_config
from .crypto_settings import load_crypto_settings
from .engine import BasisArbitrageEngine
from .notifiers import CompositeNotifier, ConsoleNotifier
from .replay import load_replay_rows, replay_rows
from .service import dump_config, monitor_live
from .settings import load_settings
from .storage import SQLiteStorage


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="fx-edge-lab",
        description="Futures-vs-spot arbitrage gap detector for CME futures and MT5 spot.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    monitor_parser = subparsers.add_parser("monitor", help="Run the live futures-vs-spot monitor.")
    monitor_parser.add_argument("--config", type=Path)
    monitor_parser.add_argument("--run-seconds", type=float)

    replay_parser = subparsers.add_parser("replay-csv", help="Replay paired futures and spot quotes.")
    replay_parser.add_argument("csv_path", type=Path)
    replay_parser.add_argument("--config", type=Path)

    config_parser = subparsers.add_parser("print-config", help="Print the merged runtime config.")
    config_parser.add_argument("--config", type=Path)

    crypto_capture = subparsers.add_parser(
        "crypto-capture",
        help="Capture Binance spot and Bybit perp microstructure data to SQLite.",
    )
    crypto_capture.add_argument("--config", type=Path)
    crypto_capture.add_argument("--run-seconds", type=float)

    crypto_analyze = subparsers.add_parser(
        "crypto-analyze",
        help="Summarize the crypto microstructure research database.",
    )
    crypto_analyze.add_argument("--config", type=Path)
    crypto_analyze.add_argument("--db", type=Path)

    crypto_config = subparsers.add_parser(
        "crypto-print-config",
        help="Print the merged crypto research config.",
    )
    crypto_config.add_argument("--config", type=Path)

    crypto_dashboard = subparsers.add_parser(
        "crypto-dashboard",
        help="Open a local dashboard for the crypto research database.",
    )
    crypto_dashboard.add_argument("--config", type=Path)
    crypto_dashboard.add_argument("--db", type=Path)
    crypto_dashboard.add_argument("--host", default="127.0.0.1")
    crypto_dashboard.add_argument("--port", type=int, default=8765)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    try:
        if args.command == "monitor":
            summary = monitor_live(str(args.config) if args.config else None, args.run_seconds)
            print(summary)
            return 0

        if args.command == "replay-csv":
            settings = load_settings(str(args.config) if args.config else None)
            storage = SQLiteStorage(settings.database_path)
            try:
                engine = BasisArbitrageEngine(settings, storage, CompositeNotifier(ConsoleNotifier()))
                replay_rows(load_replay_rows(args.csv_path), engine)
                print(storage.summary())
            finally:
                storage.close()
            return 0

        if args.command == "print-config":
            print(dump_config(str(args.config) if args.config else None))
            return 0

        if args.command == "crypto-capture":
            summary = capture_crypto_research(
                str(args.config) if args.config else None,
                args.run_seconds,
            )
            print(summary)
            return 0

        if args.command == "crypto-analyze":
            if args.db is not None:
                db_path = args.db
                settings = load_crypto_settings(str(args.config) if args.config else None)
            else:
                settings = load_crypto_settings(str(args.config) if args.config else None)
                db_path = Path(settings.database_path)
            print(summarize_crypto_database(db_path, settings))
            return 0

        if args.command == "crypto-print-config":
            print(dump_crypto_config(str(args.config) if args.config else None))
            return 0

        if args.command == "crypto-dashboard":
            settings = load_crypto_settings(str(args.config) if args.config else None)
            if args.db is not None:
                db_path = args.db
            else:
                db_path = Path(settings.database_path)
            serve_crypto_dashboard(db_path, settings, args.host, args.port)
            return 0
    except Exception as exc:
        parser.exit(1, f"Error: {exc}\n")

    parser.error(f"unknown command {args.command}")
    return 2
