from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path

from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.database import run_with_sqlite_lock_retry
from app.services.import_backlog_reconciliation import (
    ImportBacklogReconciliationReport,
    reconcile_import_backlog,
)


def _args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Dry-run or apply safe AudioHoard backlog repairs"
    )
    parser.add_argument("--database", type=Path, required=True)
    parser.add_argument(
        "--apply", action="store_true", help="Apply safe repairs; default is dry-run"
    )
    parser.add_argument("--acceptance-threshold", type=float, default=0.90)
    parser.add_argument("--skip-file-check", action="store_true")
    return parser.parse_args()


async def _main() -> None:
    args = _args()
    database = args.database.resolve()
    if not database.is_file():
        raise SystemExit(f"database not found: {database}")
    database_url = (
        f"sqlite+aiosqlite:///{database}"
        if args.apply
        else f"sqlite+aiosqlite:///file:{database}?mode=ro&uri=true"
    )
    engine = create_async_engine(database_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with factory() as db:
            report: ImportBacklogReconciliationReport | None = None
            if args.apply:

                async def apply_reconciliation() -> None:
                    nonlocal report
                    report = await reconcile_import_backlog(
                        db,
                        acceptance_threshold=args.acceptance_threshold,
                        apply=True,
                        require_existing_files=not args.skip_file_check,
                    )
                    await db.commit()

                await run_with_sqlite_lock_retry(
                    db, apply_reconciliation, attempts=6, delay_seconds=0.2
                )
            else:
                report = await reconcile_import_backlog(
                    db,
                    acceptance_threshold=args.acceptance_threshold,
                    apply=False,
                    require_existing_files=not args.skip_file_check,
                )
                await db.rollback()
            assert report is not None
            print(json.dumps(report.to_dict(), indent=2, sort_keys=True))
    finally:
        await engine.dispose()


if __name__ == "__main__":
    asyncio.run(_main())
