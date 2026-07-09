from __future__ import annotations

import argparse
from pathlib import Path

from job_matcher.pipeline import run_ingestion
from job_matcher.search import search_jobs_for_cv


def main() -> None:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("ingest")

    search_parser = subparsers.add_parser("search")
    search_parser.add_argument("cv_path", type=Path)
    search_parser.add_argument("--lookback-days", type=int, default=7)

    args = parser.parse_args()

    if args.command == "ingest":
        print(run_ingestion())
        return

    if args.command == "search":
        cv_text, cv_chunks, results = search_jobs_for_cv(
            args.cv_path.read_bytes(),
            lookback_hours=args.lookback_days * 24,
        )
        print({"cv_chars": len(cv_text), "cv_chunks": len(cv_chunks), "results": len(results)})
        for result in results:
            print(result)


if __name__ == "__main__":
    main()
