from __future__ import annotations

import argparse
import os
from pathlib import Path
import sys
from typing import Any

import psycopg
import requests
from dotenv import load_dotenv
from psycopg.types.json import Jsonb


ROOT_DIR = Path(__file__).resolve().parents[1]
PACKAGE_PARENT = ROOT_DIR.parent
if str(PACKAGE_PARENT) not in sys.path:
    sys.path.insert(0, str(PACKAGE_PARENT))
load_dotenv(ROOT_DIR / ".env")

from meetingai_shared.repositories.meeting_store import resolve_database_dsn


DEFAULT_DIRECTORY_URL = os.getenv("DIRECTORY_API_URL", "http://172.16.49.50:5010/api/query/26")
DEFAULT_TIMEOUT_SECONDS = 30


class DirectorySyncError(RuntimeError):
    pass


def compact_text(value: Any) -> str:
    return " ".join(str(value or "").split()).strip()


def extract_items(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]

    if isinstance(payload, dict):
        for key in ("items", "data", "results", "rows", "records"):
            nested = payload.get(key)
            if isinstance(nested, list):
                return [item for item in nested if isinstance(item, dict)]
            if isinstance(nested, dict):
                nested_items = extract_items(nested)
                if nested_items:
                    return nested_items

    raise DirectorySyncError("Directory API response does not contain a user list.")


def fetch_directory_users(url: str, token: str, timeout: int = DEFAULT_TIMEOUT_SECONDS) -> list[dict[str, Any]]:
    headers = {
        "Authorization": token,
        "Content-Type": "application/json",
    }
    response = requests.get(url, headers=headers, timeout=max(int(timeout), 1))
    response.raise_for_status()
    return extract_items(response.json())


def normalize_user(item: dict[str, Any]) -> dict[str, Any] | None:
    first_name = compact_text(item.get("ADI"))
    last_name = compact_text(item.get("SOYAD"))
    email = compact_text(item.get("EMAIL")).lower()
    job_title = compact_text(item.get("GOREV"))
    full_name = compact_text(f"{first_name} {last_name}")

    if not email:
        return None

    return {
        "first_name": first_name,
        "last_name": last_name,
        "full_name": full_name or email,
        "email": email,
        "job_title": job_title,
        "raw_payload": item,
    }


def upsert_users(dsn: str, items: list[dict[str, Any]], *, dry_run: bool = False) -> tuple[int, int]:
    processed = 0
    skipped = 0

    if dry_run:
        for item in items:
            normalized = normalize_user(item)
            if normalized is None:
                skipped += 1
                continue
            processed += 1
        return processed, skipped

    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            for item in items:
                normalized = normalize_user(item)
                if normalized is None:
                    skipped += 1
                    continue

                cur.execute(
                    """
                    INSERT INTO users (
                        first_name,
                        last_name,
                        full_name,
                        email,
                        job_title,
                        raw_payload,
                        updated_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, NOW())
                    ON CONFLICT (email)
                    DO UPDATE SET
                        first_name = EXCLUDED.first_name,
                        last_name = EXCLUDED.last_name,
                        full_name = EXCLUDED.full_name,
                        job_title = EXCLUDED.job_title,
                        raw_payload = EXCLUDED.raw_payload,
                        updated_at = NOW()
                    """,
                    (
                        normalized["first_name"],
                        normalized["last_name"],
                        normalized["full_name"],
                        normalized["email"],
                        normalized["job_title"],
                        Jsonb(normalized["raw_payload"]),
                    ),
                )
                processed += 1
        conn.commit()

    return processed, skipped


def main() -> None:
    parser = argparse.ArgumentParser(description="Sync company users from directory API into PostgreSQL.")
    parser.add_argument("--url", default=DEFAULT_DIRECTORY_URL, help="Directory API URL")
    parser.add_argument(
        "--token",
        default=os.getenv("DIRECTORY_API_TOKEN") or os.getenv("DIRECTORY_AUTH_TOKEN"),
        help="Directory API authorization token",
    )
    parser.add_argument("--dsn", default=None, help="Optional PostgreSQL DSN override")
    parser.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT_SECONDS, help="HTTP timeout in seconds")
    parser.add_argument("--dry-run", action="store_true", help="Fetch and normalize users without writing to DB")
    args = parser.parse_args()

    token = compact_text(args.token)
    if not token:
        raise DirectorySyncError("Directory API token is required. Use --token or set DIRECTORY_API_TOKEN.")

    users = fetch_directory_users(args.url, token, timeout=args.timeout)
    dsn = resolve_database_dsn(args.dsn)
    processed, skipped = upsert_users(dsn, users, dry_run=args.dry_run)

    action = "Would upsert" if args.dry_run else "Upserted"
    print(f"{action} {processed} users.")
    if skipped:
        print(f"Skipped {skipped} rows without email.")


if __name__ == "__main__":
    main()
