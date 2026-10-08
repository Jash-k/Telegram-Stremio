#!/usr/bin/env python3
"""Read-only GlobalDB → Stremio compatibility audit.

Scans your MongoDB for every data shape that makes Stremio render
"There is no information available", drop a catalog entry, or open the wrong
title. Writes nothing.

Usage
-----
    # uses config.env / environment (the same MONGO_URI + DB_NAME as the app)
    python3 tools/audit_stremio_meta.py

    # point at a specific database
    MONGO_URI="mongodb+srv://..." DB_NAME=dbFyvio python3 tools/audit_stremio_meta.py

    # machine-readable
    python3 tools/audit_stremio_meta.py --json

    # more examples per issue
    python3 tools/audit_stremio_meta.py --examples 10
"""
import argparse
import asyncio
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


async def _main(args) -> int:
    from app import config, db

    if not config.MONGO_URI:
        print("MONGO_URI is not set. Copy config.env.example to config.env, or "
              "export MONGO_URI / DB_NAME.", file=sys.stderr)
        return 2

    await db.connect()
    # db.connect() pings in the background; wait for the first successful one.
    for _ in range(60):
        if db.is_connected():
            break
        await asyncio.sleep(0.5)
    if not db.is_connected():
        print(f"Could not reach MongoDB at {config.MONGO_URI!r} "
              f"(database {config.DB_NAME!r}). Check the URI and the Atlas "
              f"network allow-list for this machine's IP.", file=sys.stderr)
        return 2

    from app.stremio_audit import format_report, run_audit

    print(f"Auditing {config.DB_NAME} …\n", file=sys.stderr)
    report = await run_audit(limit_examples=args.examples)

    if args.json:
        print(json.dumps(report, indent=2, default=str))
    else:
        print(format_report(report))
        print("VERDICT:", "clean" if report["ok"]
              else "critical issues present (see above)")

    await db.disconnect()
    return 0 if report["ok"] else 1


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--json", action="store_true", help="emit JSON instead of text")
    p.add_argument("--examples", type=int, default=5,
                   help="max example titles per issue (default 5)")
    args = p.parse_args()
    try:
        return asyncio.run(_main(args))
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    sys.exit(main())
