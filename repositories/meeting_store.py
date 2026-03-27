from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta
import os
from pathlib import Path
import threading
from typing import Any, Iterator, Sequence

import psycopg
from psycopg.rows import dict_row

from meetingai_shared.config import MEETINGAI_NOTES_DIR, MEETINGAI_TRANSCRIPTS_DIR
from meetingai_shared.repositories.store_notes_mail import StoreNoteMailParticipantMixin
from meetingai_shared.repositories.store_utils import (
    compact_text,
    current_timestamp,
    ensure_aware_datetime,
    format_datetime_value,
    format_display_datetime,
    infer_datetime_from_path,
    infer_started_datetime,
    infer_stopped_datetime,
    label_to_offset_ms,
    matching_log_path,
    meeting_can_generate_note,
    normalize_final_transcript_status,
    normalize_note_payload,
    normalize_owner_username,
    offset_ms_to_label,
    parse_datetime_value,
    parse_legacy_segments,
    read_text,
    resolve_database_dsn,
    resolve_owner_username,
    shorten,
    transcript_name_for_source,
)


class PostgresMeetingStore(StoreNoteMailParticipantMixin):
    def __init__(
        self,
        dsn: str | None = None,
        transcript_dir: str | Path = MEETINGAI_TRANSCRIPTS_DIR,
        notes_dir: str | Path = MEETINGAI_NOTES_DIR,
    ) -> None:
        self.dsn = resolve_database_dsn(dsn)
        self.transcript_dir = Path(transcript_dir)
        self.notes_dir = Path(notes_dir)
        self.default_owner_username = resolve_owner_username(
            os.getenv("MEETING_INTELLIGENCE_LEGACY_OWNER", "bt.stajyer")
        )
        self._init_lock = threading.RLock()
        self._initialized = False

    def ensure_ready(self) -> None:
        with self._init_lock:
            if self._initialized:
                return

            with self._connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT
                            to_regclass('public.app_users') AS app_users,
                            to_regclass('public.meetings') AS meetings,
                            to_regclass('public.transcript_segments') AS transcript_segments,
                            to_regclass('public.transcript_segment_versions') AS transcript_segment_versions,
                            to_regclass('public.meeting_note_versions') AS meeting_note_versions
                        """
                    )
                    row = cur.fetchone() or {}

                    required_meeting_columns = [
                        "final_transcript_status",
                        "final_transcript_text",
                        "final_transcript_error",
                        "final_transcript_model",
                        "final_transcript_generated_at",
                    ]
                    cur.execute(
                        """
                        SELECT column_name
                        FROM information_schema.columns
                        WHERE table_schema = 'public'
                          AND table_name = 'meetings'
                          AND column_name = ANY(%s)
                        """,
                        (required_meeting_columns,),
                    )
                    available_meeting_columns = {
                        str(item["column_name"])
                        for item in cur.fetchall()
                        if item.get("column_name")
                    }

            missing = [name for name, value in row.items() if value is None]
            if missing:
                names = ", ".join(sorted(missing))
                raise RuntimeError(
                    f"PostgreSQL schema is incomplete. Missing table(s): {names}."
                )

            missing_meeting_columns = [
                name
                for name in required_meeting_columns
                if name not in available_meeting_columns
            ]
            if missing_meeting_columns:
                names = ", ".join(sorted(missing_meeting_columns))
                raise RuntimeError(
                    "PostgreSQL schema is missing required meetings columns for final transcripts: "
                    f"{names}. Apply the SQL migration before starting the app."
                )
            self._initialized = True

    @contextmanager
    def connection(self) -> Iterator[psycopg.Connection]:
        self.ensure_ready()
        conn = self._connect()
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def recover_stale_live_meetings(self, grace_minutes: int = 10) -> int:
        self.ensure_ready()
        now = current_timestamp()
        threshold = now - timedelta(minutes=max(int(grace_minutes), 0))
        recovery_message = "Recovered after unexpected shutdown."

        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE meetings
                    SET status = 'error',
                        stopped_at = COALESCE(stopped_at, %s),
                        error_message = COALESCE(NULLIF(error_message, ''), %s),
                        updated_at = %s
                    WHERE source_type = 'live'
                      AND status IN ('starting', 'recording')
                      AND COALESCE(updated_at, started_at, created_at) <= %s
                    RETURNING id
                    """,
                    (now, recovery_message, now, threshold),
                )
                rows = cur.fetchall()
        return len(rows)

    def recover_stale_final_transcripts(self, grace_minutes: int = 10) -> int:
        self.ensure_ready()
        now = current_timestamp()
        threshold = now - timedelta(minutes=max(int(grace_minutes), 0))
        recovery_message = "Recovered after unexpected shutdown during final transcript generation."

        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE meetings
                    SET final_transcript_status = 'failed',
                        final_transcript_error = COALESCE(NULLIF(final_transcript_error, ''), %s),
                        updated_at = %s
                    WHERE final_transcript_status = 'processing'
                      AND COALESCE(updated_at, stopped_at, started_at, created_at) <= %s
                    RETURNING id
                    """,
                    (recovery_message, now, threshold),
                )
                rows = cur.fetchall()
        return len(rows)

    def create_live_meeting(
        self,
        owner_username: str | None = None,
        started_at: str | None = None,
        title: str | None = None,
    ) -> dict[str, Any]:
        self.ensure_ready()
        owner_key = self._resolved_owner_username(owner_username)
        start_dt = parse_datetime_value(started_at) if started_at else current_timestamp()
        start_dt = ensure_aware_datetime(start_dt) or current_timestamp()
        stamp = start_dt.strftime("%Y%m%d_%H%M%S")
        session_name = f"session_{stamp}"
        meeting_title = str(title or "").strip()

        with self.connection() as conn:
            user_id = self._ensure_user(conn, owner_key)
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO meetings (
                        owner_user_id, title, status, source_type, source_name,
                        started_at, created_at, updated_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING id
                    """,
                    (
                        user_id,
                        meeting_title,
                        "starting",
                        "live",
                        session_name,
                        start_dt,
                        start_dt,
                        start_dt,
                    ),
                )
                meeting_id = int(cur.fetchone()["id"])
        return self.get_meeting(meeting_id, owner_key)

    def update_meeting_state(
        self,
        meeting_id: int,
        *,
        status: str | None = None,
        stopped_at: str | None = None,
        input_device_name: str | None = None,
        input_device_index: int | None = None,
        error_message: str | None = None,
        title: str | None = None,
        final_transcript_status: str | None = None,
        final_transcript_text: str | None = None,
        final_transcript_error: str | None = None,
        final_transcript_model: str | None = None,
        final_transcript_generated_at: str | datetime | None = None,
        owner_username: str | None = None,
    ) -> None:
        self.ensure_ready()
        fields: list[str] = ["updated_at = %s"]
        params: list[Any] = [current_timestamp()]

        if status is not None:
            fields.append("status = %s")
            params.append(status)
        if stopped_at is not None:
            fields.append("stopped_at = %s")
            params.append(ensure_aware_datetime(parse_datetime_value(stopped_at)))
        if input_device_name is not None:
            fields.append("input_device_name = %s")
            params.append(input_device_name)
        if input_device_index is not None:
            fields.append("input_device_index = %s")
            params.append(input_device_index)
        if error_message is not None:
            fields.append("error_message = %s")
            params.append(error_message)
        if title is not None:
            fields.append("title = %s")
            params.append(title)
        if final_transcript_status is not None:
            fields.append("final_transcript_status = %s")
            params.append(normalize_final_transcript_status(final_transcript_status))
        if final_transcript_text is not None:
            fields.append("final_transcript_text = %s")
            params.append(str(final_transcript_text or "").strip())
        if final_transcript_error is not None:
            fields.append("final_transcript_error = %s")
            params.append(str(final_transcript_error or "").strip())
        if final_transcript_model is not None:
            fields.append("final_transcript_model = %s")
            params.append(str(final_transcript_model or "").strip())
        if final_transcript_generated_at is not None:
            fields.append("final_transcript_generated_at = %s")
            params.append(ensure_aware_datetime(parse_datetime_value(final_transcript_generated_at)))

        params.append(meeting_id)
        with self.connection() as conn:
            if owner_username is not None:
                self._assert_meeting_write_access(conn, meeting_id, owner_username)
            with conn.cursor() as cur:
                cur.execute(
                    f"UPDATE meetings SET {', '.join(fields)} WHERE id = %s",
                    params,
                )

    def append_segment(
        self,
        meeting_id: int,
        seq: int,
        start_label: str,
        end_label: str,
        text: str,
        created_at: str | None = None,
        *,
        draft_model: str | None = None,
        final_model: str | None = None,
    ) -> dict[str, Any] | None:
        clean_text = text.strip()
        if not clean_text:
            return None

        created_dt = ensure_aware_datetime(parse_datetime_value(created_at)) or current_timestamp()
        with self.connection() as conn:
            return self._insert_segment_with_version(
                conn,
                meeting_id=meeting_id,
                seq=seq,
                start_label=start_label,
                end_label=end_label,
                text=clean_text,
                version_type="final",
                model_name=final_model or draft_model,
                created_at=created_dt,
            )

    def create_draft_segment(
        self,
        meeting_id: int,
        seq: int,
        start_label: str,
        end_label: str,
        draft_text: str,
        *,
        draft_model: str | None = None,
        created_at: str | None = None,
    ) -> dict[str, Any] | None:
        clean_text = draft_text.strip()
        if not clean_text:
            return None

        created_dt = ensure_aware_datetime(parse_datetime_value(created_at)) or current_timestamp()
        with self.connection() as conn:
            return self._insert_segment_with_version(
                conn,
                meeting_id=meeting_id,
                seq=seq,
                start_label=start_label,
                end_label=end_label,
                text=clean_text,
                version_type="draft",
                model_name=draft_model,
                created_at=created_dt,
            )

    def finalize_segment(
        self,
        segment_id: int,
        final_text: str,
        *,
        final_model: str | None = None,
        finalized_at: str | None = None,
    ) -> dict[str, Any] | None:
        self.ensure_ready()
        updated_dt = ensure_aware_datetime(parse_datetime_value(finalized_at)) or current_timestamp()
        clean_text = final_text.strip()

        with self.connection() as conn:
            state = self._get_segment_state(conn, segment_id)
            if state is None:
                return None

            display_text = clean_text or (state["current_text"] or "").strip()
            if not display_text:
                return None

            next_version_no = self._next_segment_version_no(conn, segment_id)
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE transcript_segment_versions
                    SET is_current = FALSE
                    WHERE segment_id = %s AND is_current = TRUE
                    """,
                    (segment_id,),
                )
                cur.execute(
                    """
                    INSERT INTO transcript_segment_versions (
                        segment_id, version_no, text, version_type, model_name, is_current, created_at
                    )
                    VALUES (%s, %s, %s, %s, %s, TRUE, %s)
                    """,
                    (
                        segment_id,
                        next_version_no,
                        display_text,
                        "final",
                        final_model,
                        updated_dt,
                    ),
                )
                cur.execute(
                    """
                    UPDATE transcript_segments
                    SET updated_at = %s
                    WHERE id = %s
                    """,
                    (updated_dt, segment_id),
                )

        return {
            "id": int(state["id"]),
            "meeting_id": int(state["meeting_id"]),
            "seq": int(state["seq"]),
            "start_label": offset_ms_to_label(state["started_at"], state["start_offset_ms"]),
            "end_label": offset_ms_to_label(state["started_at"], state["end_offset_ms"]),
            "text": display_text,
            "draft_text": (state["current_text"] or "") if state["current_type"] == "draft" else display_text,
            "final_text": display_text,
            "status": "final",
            "draft_model": state["current_model"] if state["current_type"] == "draft" else None,
            "final_model": final_model,
            "created_at": format_datetime_value(state["created_at"]),
            "updated_at": format_datetime_value(updated_dt),
        }

    def list_meetings(self, owner_username: str | None = None) -> list[dict[str, Any]]:
        self.ensure_ready()
        owner_key = normalize_owner_username(owner_username)
        params: list[Any] = []
        where_clause = ""
        if owner_key:
            where_clause = "WHERE lower(u.username) = %s"
            params.append(owner_key)

        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        m.id,
                        m.title,
                        m.status,
                        m.source_type,
                        m.source_name,
                        m.started_at,
                        m.stopped_at,
                        m.input_device_name,
                        m.input_device_index,
                        m.error_message,
                        m.final_transcript_status,
                        m.final_transcript_text,
                        m.final_transcript_error,
                        m.final_transcript_model,
                        m.final_transcript_generated_at,
                        m.created_at,
                        m.updated_at,
                        u.username AS owner_username,
                        COUNT(DISTINCT s.id) AS segment_count,
                        COUNT(DISTINCT n.id) AS note_count
                    FROM meetings m
                    JOIN app_users u ON u.id = m.owner_user_id
                    LEFT JOIN transcript_segments s ON s.meeting_id = m.id
                    LEFT JOIN meeting_note_versions n ON n.meeting_id = m.id
                    """
                    + where_clause
                    + """
                    GROUP BY m.id, u.username
                    ORDER BY COALESCE(m.started_at, m.created_at) DESC, m.id DESC
                    """,
                    params,
                )
                rows = cur.fetchall()
            segments_by_meeting = self._fetch_current_segments(conn, [int(row["id"]) for row in rows])
        return [
            self._meeting_row_to_dict(row, segments_by_meeting.get(int(row["id"]), []))
            for row in rows
        ]

    def get_meeting(
        self,
        meeting_id: int,
        owner_username: str | None = None,
    ) -> dict[str, Any] | None:
        self.ensure_ready()
        owner_key = normalize_owner_username(owner_username)
        params: list[Any] = [meeting_id]
        owner_clause = ""
        if owner_key:
            owner_clause = " AND lower(u.username) = %s"
            params.append(owner_key)

        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        m.id,
                        m.title,
                        m.status,
                        m.source_type,
                        m.source_name,
                        m.started_at,
                        m.stopped_at,
                        m.input_device_name,
                        m.input_device_index,
                        m.error_message,
                        m.final_transcript_status,
                        m.final_transcript_text,
                        m.final_transcript_error,
                        m.final_transcript_model,
                        m.final_transcript_generated_at,
                        m.created_at,
                        m.updated_at,
                        u.username AS owner_username,
                        COUNT(DISTINCT s.id) AS segment_count,
                        COUNT(DISTINCT n.id) AS note_count
                    FROM meetings m
                    JOIN app_users u ON u.id = m.owner_user_id
                    LEFT JOIN transcript_segments s ON s.meeting_id = m.id
                    LEFT JOIN meeting_note_versions n ON n.meeting_id = m.id
                    WHERE m.id = %s
                    """
                    + owner_clause
                    + """
                    GROUP BY m.id, u.username
                    """,
                    params,
                )
                row = cur.fetchone()
            if row is None:
                return None
            segments_by_meeting = self._fetch_current_segments(conn, [int(row["id"])])
        return self._meeting_row_to_dict(row, segments_by_meeting.get(int(row["id"]), []))

    def import_transcript_file(
        self,
        raw_path: str | Path,
        owner_username: str | None = None,
    ) -> dict[str, Any]:
        self.ensure_ready()
        source_path = Path(raw_path)
        if not source_path.exists():
            raise FileNotFoundError(f"Transcript not found: {source_path}")

        owner_key = self._resolved_owner_username(owner_username)
        existing_id = self._find_imported_meeting_id(source_path.name, owner_key)
        if existing_id is not None:
            meeting = self.get_meeting(existing_id, owner_key)
            if meeting is not None:
                return meeting

        raw_text = read_text(source_path)
        log_path = matching_log_path(source_path)
        log_text = read_text(log_path) if log_path.exists() else ""
        created_dt = infer_datetime_from_path(source_path)
        started_dt = infer_started_datetime(log_text, created_dt)
        stopped_dt = infer_stopped_datetime(log_text, source_path, log_path, started_dt)
        segments = parse_legacy_segments(
            log_text,
            started_dt.date() if started_dt else created_dt.date(),
        )

        if not segments and raw_text:
            default_label = (ensure_aware_datetime(started_dt) or current_timestamp()).strftime("%H:%M:%S")
            segments = [
                {
                    "seq": 1,
                    "start_label": default_label,
                    "end_label": default_label,
                    "text": raw_text,
                    "created_at": ensure_aware_datetime(started_dt).isoformat(timespec="seconds"),
                }
            ]

        with self.connection() as conn:
            user_id = self._ensure_user(conn, owner_key)
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO meetings (
                        owner_user_id, title, status, source_type, source_name,
                        started_at, stopped_at, created_at, updated_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING id
                    """,
                    (
                        user_id,
                        "",
                        "imported",
                        "imported",
                        source_path.name,
                        ensure_aware_datetime(started_dt),
                        ensure_aware_datetime(stopped_dt),
                        ensure_aware_datetime(created_dt),
                        ensure_aware_datetime(stopped_dt),
                    ),
                )
                meeting_id = int(cur.fetchone()["id"])

            for segment in segments:
                self._insert_segment_with_version(
                    conn,
                    meeting_id=meeting_id,
                    seq=int(segment["seq"]),
                    start_label=str(segment["start_label"]),
                    end_label=str(segment["end_label"]),
                    text=str(segment["text"]),
                    version_type="final",
                    model_name="legacy-import",
                    created_at=ensure_aware_datetime(parse_datetime_value(segment["created_at"]))
                    or ensure_aware_datetime(created_dt),
                )

        meeting = self.get_meeting(meeting_id, owner_key)
        if meeting is None:
            raise RuntimeError(f"Meeting import failed: {source_path}")
        return meeting

    def delete_meeting(
        self,
        meeting_id: int,
        owner_username: str | None = None,
    ) -> dict[str, Any] | None:
        self.ensure_ready()
        params: list[Any] = [meeting_id]
        owner_clause = ""
        owner_key = normalize_owner_username(owner_username)
        if owner_key:
            owner_clause = " AND lower(u.username) = %s"
            params.append(owner_key)

        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        m.id,
                        m.source_name,
                        COUNT(DISTINCT s.id) AS segment_count,
                        COUNT(DISTINCT n.id) AS note_count
                    FROM meetings m
                    JOIN app_users u ON u.id = m.owner_user_id
                    LEFT JOIN transcript_segments s ON s.meeting_id = m.id
                    LEFT JOIN meeting_note_versions n ON n.meeting_id = m.id
                    WHERE m.id = %s
                    """
                    + owner_clause
                    + """
                    GROUP BY m.id
                    """,
                    params,
                )
                row = cur.fetchone()
                if row is None:
                    return None
                cur.execute("DELETE FROM meetings WHERE id = %s", (meeting_id,))

        return {
            "meeting_name": row["source_name"] or f"meeting_{meeting_id}",
            "note_count": int(row["note_count"] or 0),
            "segment_count": int(row["segment_count"] or 0),
        }

    def _connect(self) -> psycopg.Connection:
        return psycopg.connect(self.dsn, row_factory=dict_row, connect_timeout=10)

    def _ensure_user(self, conn: psycopg.Connection, owner_username: str) -> int:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO app_users (username)
                VALUES (%s)
                ON CONFLICT ((lower(username)))
                DO UPDATE SET username = EXCLUDED.username
                RETURNING id
                """,
                (owner_username,),
            )
            row = cur.fetchone()
        return int(row["id"])

    def _assert_meeting_write_access(
        self,
        conn: psycopg.Connection,
        meeting_id: int,
        owner_username: str | None,
    ) -> None:
        owner_key = normalize_owner_username(owner_username)
        if not owner_key:
            raise PermissionError("A valid user session is required.")

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT 1
                FROM meetings m
                JOIN app_users u ON u.id = m.owner_user_id
                WHERE m.id = %s
                  AND lower(u.username) = %s
                """,
                (meeting_id, owner_key),
            )
            row = cur.fetchone()

        if row is None:
            raise PermissionError("You cannot modify another user's meeting.")

    def _find_imported_meeting_id(self, source_name: str, owner_username: str) -> int | None:
        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT m.id
                    FROM meetings m
                    JOIN app_users u ON u.id = m.owner_user_id
                    WHERE lower(u.username) = %s
                      AND m.source_type IN ('imported', 'legacy')
                      AND m.source_name = %s
                    ORDER BY m.id DESC
                    LIMIT 1
                    """,
                    (owner_username, source_name),
                )
                row = cur.fetchone()
        return int(row["id"]) if row is not None else None

    def _insert_segment_with_version(
        self,
        conn: psycopg.Connection,
        *,
        meeting_id: int,
        seq: int,
        start_label: str,
        end_label: str,
        text: str,
        version_type: str,
        model_name: str | None,
        created_at: datetime,
    ) -> dict[str, Any]:
        meeting_started_at = self._get_meeting_started_at(conn, meeting_id)
        start_offset_ms = label_to_offset_ms(start_label, meeting_started_at)
        end_offset_ms = label_to_offset_ms(end_label, meeting_started_at)

        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO transcript_segments (
                    meeting_id, seq, start_offset_ms, end_offset_ms, created_at, updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s)
                RETURNING id
                """,
                (
                    meeting_id,
                    seq,
                    start_offset_ms,
                    end_offset_ms,
                    created_at,
                    created_at,
                ),
            )
            segment_id = int(cur.fetchone()["id"])
            cur.execute(
                """
                INSERT INTO transcript_segment_versions (
                    segment_id, version_no, text, version_type, model_name, is_current, created_at
                )
                VALUES (%s, 1, %s, %s, %s, TRUE, %s)
                """,
                (segment_id, text, version_type, model_name, created_at),
            )
            cur.execute(
                """
                UPDATE meetings
                SET updated_at = %s
                WHERE id = %s
                """,
                (created_at, meeting_id),
            )

        return {
            "id": segment_id,
            "meeting_id": meeting_id,
            "seq": seq,
            "start_label": start_label,
            "end_label": end_label,
            "text": text,
            "draft_text": text if version_type == "draft" else text,
            "final_text": text if version_type == "final" else "",
            "status": version_type,
            "draft_model": model_name if version_type == "draft" else None,
            "final_model": model_name if version_type == "final" else None,
            "created_at": format_datetime_value(created_at),
            "updated_at": format_datetime_value(created_at),
        }

    def _get_meeting_started_at(self, conn: psycopg.Connection, meeting_id: int) -> datetime | None:
        with conn.cursor() as cur:
            cur.execute("SELECT started_at FROM meetings WHERE id = %s", (meeting_id,))
            row = cur.fetchone()
        return ensure_aware_datetime(row["started_at"]) if row is not None else None

    def _get_segment_state(self, conn: psycopg.Connection, segment_id: int) -> dict[str, Any] | None:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    s.id,
                    s.meeting_id,
                    s.seq,
                    s.start_offset_ms,
                    s.end_offset_ms,
                    s.created_at,
                    m.started_at,
                    v.text AS current_text,
                    v.version_type AS current_type,
                    v.model_name AS current_model
                FROM transcript_segments s
                JOIN meetings m ON m.id = s.meeting_id
                JOIN transcript_segment_versions v
                  ON v.segment_id = s.id
                 AND v.is_current = TRUE
                WHERE s.id = %s
                """,
                (segment_id,),
            )
            return cur.fetchone()

    def _next_segment_version_no(self, conn: psycopg.Connection, segment_id: int) -> int:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT COALESCE(MAX(version_no), 0) + 1 AS next_version
                FROM transcript_segment_versions
                WHERE segment_id = %s
                """,
                (segment_id,),
            )
            row = cur.fetchone()
        return int(row["next_version"])

    def _fetch_current_segments(
        self,
        conn: psycopg.Connection,
        meeting_ids: Sequence[int],
    ) -> dict[int, list[dict[str, Any]]]:
        if not meeting_ids:
            return {}

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    s.id,
                    s.meeting_id,
                    s.seq,
                    s.start_offset_ms,
                    s.end_offset_ms,
                    s.created_at,
                    s.updated_at,
                    v.id AS version_id,
                    v.text,
                    v.version_type,
                    v.model_name,
                    v.created_at AS version_created_at
                FROM transcript_segments s
                JOIN transcript_segment_versions v
                  ON v.segment_id = s.id
                 AND v.is_current = TRUE
                WHERE s.meeting_id = ANY(%s)
                ORDER BY s.meeting_id ASC, s.seq ASC, s.id ASC
                """,
                (list(meeting_ids),),
            )
            rows = cur.fetchall()

        grouped: dict[int, list[dict[str, Any]]] = {}
        for row in rows:
            grouped.setdefault(int(row["meeting_id"]), []).append(row)
        return grouped

    def _meeting_row_to_dict(
        self,
        row: dict[str, Any],
        segments: list[dict[str, Any]],
    ) -> dict[str, Any]:
        started_at = ensure_aware_datetime(row["started_at"])
        raw_parts: list[str] = []
        log_lines: list[str] = []
        for segment in segments:
            text = (segment["text"] or "").strip()
            if not text:
                continue
            raw_parts.append(text)
            start_label = offset_ms_to_label(started_at, segment["start_offset_ms"])
            end_label = offset_ms_to_label(started_at, segment["end_offset_ms"])
            if start_label or end_label:
                log_lines.append(f"[{start_label} - {end_label}] {text}")
            else:
                log_lines.append(text)

        raw_text = " ".join(raw_parts).strip()
        final_transcript_text = compact_text(row.get("final_transcript_text"))
        final_transcript_status = normalize_final_transcript_status(row.get("final_transcript_status"))
        preferred_transcript_text = final_transcript_text or raw_text
        raw_filename = row["source_name"]
        transcript_filename = transcript_name_for_source(raw_filename)
        updated_text = row["updated_at"] or row["created_at"]
        return {
            "id": int(row["id"]),
            "owner_username": row["owner_username"] or self.default_owner_username,
            "name": raw_filename or transcript_filename or f"meeting_{row['id']}",
            "title": row["title"] or "",
            "status": row["status"],
            "created_at": format_datetime_value(row["created_at"]),
            "updated_at": format_datetime_value(updated_text),
            "started_at": format_datetime_value(row["started_at"]),
            "stopped_at": format_datetime_value(row["stopped_at"]),
            "modified_at": format_display_datetime(updated_text),
            "raw_text": raw_text,
            "transcript_log": "\n".join(log_lines).strip(),
            "preferred_transcript_text": preferred_transcript_text,
            "final_transcript_status": final_transcript_status,
            "final_transcript_ready": bool(final_transcript_text) and final_transcript_status == "completed",
            "final_transcript_text": final_transcript_text,
            "final_transcript_error": compact_text(row.get("final_transcript_error")),
            "final_transcript_model": compact_text(row.get("final_transcript_model")),
            "final_transcript_generated_at": format_datetime_value(row.get("final_transcript_generated_at")),
            "can_generate_note": meeting_can_generate_note(
                {
                    "final_transcript_status": final_transcript_status,
                    "final_transcript_text": final_transcript_text,
                }
            ),
            "raw_filename": raw_filename,
            "transcript_filename": transcript_filename,
            "preview": shorten(preferred_transcript_text),
            "size": len(preferred_transcript_text.encode("utf-8")),
            "segment_count": int(row["segment_count"] or 0),
            "note_count": int(row["note_count"] or 0),
            "input_device_name": row["input_device_name"],
            "input_device_index": row["input_device_index"],
            "error_message": row["error_message"],
        }

    def _resolved_owner_username(self, owner_username: str | None) -> str:
        resolved = normalize_owner_username(owner_username)
        if resolved:
            return resolved
        return self.default_owner_username


MeetingStore = PostgresMeetingStore


def transcript_name_for_source(source_name: str | None) -> str | None:
    if not source_name:
        return None
    if source_name.startswith("session_"):
        return None
    return matching_log_path(Path(source_name)).name


def matching_log_path(raw_path: Path) -> Path:
    name = raw_path.name
    if not name.startswith("raw_"):
        return raw_path.with_name(f"{raw_path.stem}_log.txt")
    return raw_path.with_name(f"transcript_{name.removeprefix('raw_')}")


def infer_datetime_from_path(path: Path) -> datetime:
    stamp = parse_stamp_from_name(path.name)
    if stamp is not None:
        return ensure_aware_datetime(stamp) or current_timestamp()
    return datetime.fromtimestamp(path.stat().st_mtime, tz=local_timezone())


def infer_started_datetime(log_text: str, fallback: datetime) -> datetime:
    for line in log_text.splitlines():
        normalized = line.strip().strip("-").strip()
        if "Transcript started at" not in normalized:
            continue
        value = normalized.split("Transcript started at", 1)[1].strip()
        parsed = try_parse_datetime(value, fallback.date())
        if parsed is not None:
            return ensure_aware_datetime(parsed) or fallback
    return ensure_aware_datetime(fallback) or current_timestamp()


def infer_stopped_datetime(
    log_text: str,
    raw_path: Path | None,
    log_path: Path | None,
    fallback: datetime,
) -> datetime:
    for line in reversed(log_text.splitlines()):
        normalized = line.strip().strip("-").strip()
        if "Transcript ended at" not in normalized:
            continue
        value = normalized.split("Transcript ended at", 1)[1].strip()
        parsed = try_parse_datetime(value, fallback.date())
        if parsed is not None:
            return ensure_aware_datetime(parsed) or fallback

    timestamps = [ensure_aware_datetime(fallback) or current_timestamp()]
    if raw_path is not None and raw_path.exists():
        timestamps.append(datetime.fromtimestamp(raw_path.stat().st_mtime, tz=local_timezone()))
    if log_path is not None and log_path.exists():
        timestamps.append(datetime.fromtimestamp(log_path.stat().st_mtime, tz=local_timezone()))
    return max(timestamps)


def parse_legacy_segments(log_text: str, reference_date: date) -> list[dict[str, str]]:
    segments: list[dict[str, str]] = []
    for line in log_text.splitlines():
        match = SEGMENT_PATTERN.match(line.strip())
        if not match:
            continue
        start_label = match.group(1).strip()
        end_label = match.group(2).strip()
        text = match.group(3).strip()
        if not text:
            continue
        created_at = try_parse_datetime(start_label, reference_date)
        segments.append(
            {
                "seq": len(segments) + 1,
                "start_label": start_label,
                "end_label": end_label,
                "text": text,
                "created_at": (
                    ensure_aware_datetime(created_at)
                    or ensure_aware_datetime(datetime.combine(reference_date, time.min))
                    or current_timestamp()
                ).isoformat(timespec="seconds"),
            }
        )
    return segments


def normalize_note_payload(data: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(data, dict):
        raise ValueError("Meeting note payload must be a JSON object.")

    note_data = resolve_note_payload_object(data)
    decision_details = normalize_decision_details(
        lookup_note_value(
            note_data,
            "decision_details",
            "important_decisions",
            "karar_detaylari",
            "decision_items",
        )
    )
    open_items = normalize_open_items(
        lookup_note_value(
            note_data,
            "open_items",
            "acik_konular",
            "open_topics",
            "pending_items",
        )
    )
    return {
        "title": coerce_note_text(note_data, "title", "baslik", "meeting_title", "note_title"),
        "summary": coerce_note_text(
            note_data,
            "summary",
            "ozet",
            "meeting_summary",
            "overall_summary",
            "abstract",
            "overview",
            "analysis",
            "aciklama",
            "content",
        ),
        "context_and_objective": coerce_note_text(
            note_data,
            "context_and_objective",
            "ana_amac_ve_baglam",
            "meeting_context",
            "purpose_and_context",
            "amac_ve_baglam",
        ),
        "main_topics": coerce_note_list(
            note_data,
            "main_topics",
            "gorusulen_ana_konular",
            "discussion_points",
            "topics_discussed",
            "ana_konular",
        ),
        "participant_contributions": normalize_participant_contributions(
            lookup_note_value(
                note_data,
                "participant_contributions",
                "katilimci_katkilari",
                "speaker_contributions",
                "contributions_by_participant",
            )
        ),
        "decisions": coerce_note_list(note_data, "decisions", "kararlar", "decision_points", "key_decisions")
        or [item["decision"] for item in decision_details if compact_text(item.get("decision"))],
        "decision_details": decision_details,
        "action_items": normalize_action_items(
            lookup_note_value(
                note_data,
                "action_items",
                "actions",
                "actionitems",
                "aksiyonlar",
                "aksiyon_maddeleri",
                "eylem_maddeleri",
                "tasks",
                "todos",
                "next_steps",
            )
        ),
        "risks": coerce_note_list(note_data, "risks", "riskler", "concerns", "issues"),
        "open_questions": coerce_note_list(
            note_data,
            "open_questions",
            "acik_sorular",
            "questions",
            "sorular",
            "open_items",
            "unknowns",
        )
        or [item["item"] for item in open_items if compact_text(item.get("item"))],
        "open_items": open_items,
        "tags": normalize_tags(lookup_note_value(note_data, "tags", "etiketler", "categories", "topics", "konular")),
    }


def normalize_note_source(source: str | None) -> str:
    normalized = str(source or "").strip().lower()
    if normalized in {"generated", "llm", ""}:
        return "llm"
    if normalized == "legacy":
        return "legacy"
    if normalized == "manual":
        return "manual"
    return "llm"


def normalize_final_transcript_status(value: Any) -> str:
    normalized = str(value or "").strip().lower()
    if normalized in {"recording", "processing", "completed", "failed"}:
        return normalized
    return "completed"


def meeting_can_generate_note(meeting: dict[str, Any] | None) -> bool:
    if not isinstance(meeting, dict):
        return False

    status = normalize_final_transcript_status(meeting.get("final_transcript_status"))
    if status != "completed":
        return False

    final_transcript_text = compact_text(meeting.get("final_transcript_text"))
    raw_text = compact_text(meeting.get("raw_text"))
    return bool(final_transcript_text or raw_text)


def ensure_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if value in (None, ""):
        return []
    return [value]


def resolve_note_payload_object(data: dict[str, Any]) -> dict[str, Any]:
    root = data if isinstance(data, dict) else {}
    for key in ("meeting_note", "note", "result", "data", "output"):
        nested = root.get(key)
        if isinstance(nested, dict) and nested:
            return nested
    return root


def normalize_note_field_name(value: Any) -> str:
    text = str(value or "").strip().translate(
        str.maketrans(
            {
                "ç": "c",
                "ğ": "g",
                "ı": "i",
                "ö": "o",
                "ş": "s",
                "ü": "u",
                "Ç": "c",
                "Ğ": "g",
                "İ": "i",
                "Ö": "o",
                "Ş": "s",
                "Ü": "u",
            }
        )
    )
    return "_".join(text.replace("-", " ").split()).lower()


def lookup_note_value(data: dict[str, Any], *aliases: str) -> Any:
    if not isinstance(data, dict) or not aliases:
        return None

    normalized_aliases = {normalize_note_field_name(alias) for alias in aliases}
    for key, value in data.items():
        if normalize_note_field_name(key) in normalized_aliases:
            return value
    return None


def coerce_note_text(data: dict[str, Any], *aliases: str) -> str:
    value = lookup_note_value(data, *aliases)
    if isinstance(value, list):
        return " ".join(ensure_string_list(value)).strip()
    return compact_text(value)


def coerce_note_list(data: dict[str, Any], *aliases: str) -> list[str]:
    return ensure_string_list(lookup_note_value(data, *aliases))


def compact_text(value: Any) -> str:
    return " ".join(str(value or "").split()).strip()


def ensure_string_list(value: Any) -> list[str]:
    items = ensure_list(value)
    normalized: list[str] = []
    for item in items:
        text = compact_text(item)
        if text:
            normalized.append(text)
    return normalized


def normalize_action_items(value: Any) -> list[dict[str, str]]:
    items = ensure_list(value)
    normalized: list[dict[str, str]] = []
    for item in items:
        if isinstance(item, dict):
            task = coerce_note_text(item, "task", "action", "aksiyon", "gorev", "description", "item", "todo")
            owner = coerce_note_text(item, "owner", "assignee", "sorumlu", "kisi", "person") or "unknown"
            due_date = normalize_due_date(
                lookup_note_value(item, "due_date", "due", "deadline", "son_tarih", "tarih", "target_date")
            )
            priority = normalize_priority(lookup_note_value(item, "priority", "oncelik", "urgency", "level"))
            status = normalize_status_text(lookup_note_value(item, "status", "durum", "state", "progress"))
        else:
            task = compact_text(item)
            owner = "unknown"
            due_date = "unknown"
            priority = "unknown"
            status = "unknown"

        if not task:
            continue

        normalized.append(
            {
                "task": task,
                "owner": owner,
                "due_date": due_date,
                "priority": priority,
                "status": status,
            }
        )
    return normalized


def normalize_decision_details(value: Any) -> list[dict[str, str]]:
    items = ensure_list(value)
    normalized: list[dict[str, str]] = []
    for item in items:
        if isinstance(item, dict):
            decision = coerce_note_text(item, "decision", "karar", "item", "text", "description")
            status = normalize_status_text(lookup_note_value(item, "status", "durum", "state", "progress"))
            priority = normalize_priority(lookup_note_value(item, "priority", "oncelik", "urgency", "level"))
        else:
            decision = compact_text(item)
            status = "unknown"
            priority = "unknown"

        if not decision:
            continue

        normalized.append(
            {
                "decision": decision,
                "status": status,
                "priority": priority,
            }
        )
    return normalized


def normalize_open_items(value: Any) -> list[dict[str, str]]:
    items = ensure_list(value)
    normalized: list[dict[str, str]] = []
    for item in items:
        if isinstance(item, dict):
            text = coerce_note_text(item, "item", "question", "topic", "konu", "acik_konu", "text", "description")
            status = normalize_status_text(lookup_note_value(item, "status", "durum", "state", "progress"))
        else:
            text = compact_text(item)
            status = "unknown"

        if not text:
            continue

        normalized.append(
            {
                "item": text,
                "status": status,
            }
        )
    return normalized


def normalize_participant_contributions(value: Any) -> list[dict[str, Any]]:
    items = ensure_list(value)
    normalized: list[dict[str, Any]] = []
    for item in items:
        if isinstance(item, dict):
            name = coerce_note_text(item, "name", "isim", "participant", "person")
            role = coerce_note_text(item, "role", "gorev", "job_title", "meeting_role")
            contributions = coerce_note_list(item, "contributions", "katkilar", "items", "notes", "highlights")
        else:
            name = compact_text(item)
            role = ""
            contributions = []

        if not name and not contributions:
            continue

        normalized.append(
            {
                "name": name or "unknown",
                "role": role,
                "contributions": contributions,
            }
        )
    return normalized


def normalize_due_date(value: Any) -> str:
    text = compact_text(value)
    if not text:
        return "unknown"
    if text.lower() == "unknown":
        return "unknown"
    try:
        return date.fromisoformat(text).isoformat()
    except ValueError:
        return "unknown"


def normalize_priority(value: Any) -> str:
    text = compact_text(value).lower()
    if text in {"p0", "p1", "p2", "p3"}:
        return text.upper()
    if text in {"low", "medium", "high"}:
        return text
    return "unknown"


def normalize_status_text(value: Any) -> str:
    text = compact_text(value)
    if not text:
        return "unknown"
    lowered = text.lower()
    if lowered in {"unknown", "bilinmiyor", "belirsiz"}:
        return "unknown"
    if lowered in {"in_progress", "devam", "devam ediyor", "surec devam ediyor", "süreç devam ediyor"}:
        return "Süreç Devam Ediyor"
    if lowered in {"done", "completed", "tamamlandi", "tamamlandı"}:
        return "Tamamlandı"
    if lowered in {"blocked", "engelli", "beklemede"}:
        return "Beklemede"
    return text


def normalize_tags(value: Any) -> list[str]:
    allowed = {
        "ik",
        "isg",
        "lojistik",
        "muhasebe",
        "satis",
        "genel",
        "dokumhane",
        "talasli_imalat",
        "montaj",
        "test",
        "kalite",
        "arge",
        "tasarim",
        "marine",
        "endustriyel",
        "satinalma",
        "bakim",
        "planlama",
        "uretim",
    }
    normalized: list[str] = []
    for item in ensure_list(value):
        text = compact_text(item).lower()
        if text and text in allowed and text not in normalized:
            normalized.append(text)
    return normalized


def shorten(text: str, limit: int = 140) -> str:
    compact = " ".join((text or "").split())
    if len(compact) <= limit:
        return compact
    return f"{compact[: limit - 3]}..."


def read_text(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8").strip()
    except UnicodeDecodeError:
        return path.read_text(encoding="cp1254").strip()


def parse_stamp_from_name(name: str) -> datetime | None:
    match = STAMP_PATTERN.search(name)
    if not match:
        return None
    try:
        return datetime.strptime(match.group(1), "%Y%m%d_%H%M%S")
    except ValueError:
        return None


def try_parse_datetime(value: str, reference_date: date | None) -> datetime | None:
    text = value.strip().strip("-").strip()
    for fmt in (
        "%Y-%m-%d %H:%M:%S.%f",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%S",
    ):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue

    if reference_date is not None:
        for fmt in ("%H:%M:%S.%f", "%H:%M:%S"):
            try:
                parsed_time = datetime.strptime(text, fmt).time()
                return datetime.combine(reference_date, parsed_time)
            except ValueError:
                continue
    return None


def parse_datetime_value(value: str | datetime | None) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    parsed = try_parse_datetime(str(value), None)
    if parsed is not None:
        return parsed
    try:
        return datetime.fromisoformat(str(value))
    except ValueError:
        return None


def normalize_owner_username(value: str | None) -> str:
    return str(value or "").strip().casefold()


def resolve_owner_username(value: str | None, fallback: str | None = None) -> str:
    normalized = normalize_owner_username(value)
    if normalized:
        return normalized

    fallback_normalized = normalize_owner_username(fallback)
    if fallback_normalized:
        return fallback_normalized

    return "bt.stajyer"


def resolve_database_dsn(dsn: str | None = None) -> str:
    if dsn:
        return dsn

    for key in ("MEETING_INTELLIGENCE_DATABASE_URL", "DATABASE_URL", "POSTGRES_DSN"):
        value = os.getenv(key)
        if value:
            return value

    raise RuntimeError(
        "PostgreSQL connection string is missing. Set MEETING_INTELLIGENCE_DATABASE_URL or DATABASE_URL."
    )


def local_timezone():
    return datetime.now().astimezone().tzinfo or timezone.utc


def current_timestamp() -> datetime:
    return datetime.now().astimezone()


def ensure_aware_datetime(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is not None:
        return value
    return value.replace(tzinfo=local_timezone())


def format_datetime_value(value: datetime | str | None) -> str | None:
    parsed = ensure_aware_datetime(parse_datetime_value(value))
    if parsed is None:
        return None
    return parsed.isoformat(timespec="seconds")


def format_display_datetime(value: datetime | str | None) -> str:
    parsed = ensure_aware_datetime(parse_datetime_value(value))
    if parsed is None:
        return ""
    return parsed.strftime("%Y-%m-%d %H:%M")


def parse_time_label(value: str | None) -> time | None:
    text = str(value or "").strip()
    if not text:
        return None
    for fmt in ("%H:%M:%S.%f", "%H:%M:%S"):
        try:
            return datetime.strptime(text, fmt).time()
        except ValueError:
            continue
    return None


def label_to_offset_ms(label: str | None, started_at: datetime | None) -> int | None:
    if started_at is None:
        return None
    parsed_time = parse_time_label(label)
    if parsed_time is None:
        return None
    candidate = datetime.combine(started_at.date(), parsed_time, tzinfo=started_at.tzinfo)
    if candidate < started_at and (started_at - candidate).total_seconds() > 43200:
        candidate += timedelta(days=1)
    offset_ms = int(round((candidate - started_at).total_seconds() * 1000))
    return max(offset_ms, 0)


def offset_ms_to_label(started_at: datetime | None, offset_ms: int | None) -> str:
    if started_at is None or offset_ms is None:
        return ""
    label_dt = started_at + timedelta(milliseconds=int(offset_ms))
    return label_dt.strftime("%H:%M:%S")


# Canonical shared helpers live in store_utils; rebind here so existing imports
# keep working while meeting_store is reduced incrementally.
from meetingai_shared.repositories import store_utils as _store_utils

compact_text = _store_utils.compact_text
current_timestamp = _store_utils.current_timestamp
ensure_aware_datetime = _store_utils.ensure_aware_datetime
format_datetime_value = _store_utils.format_datetime_value
format_display_datetime = _store_utils.format_display_datetime
infer_datetime_from_path = _store_utils.infer_datetime_from_path
infer_started_datetime = _store_utils.infer_started_datetime
infer_stopped_datetime = _store_utils.infer_stopped_datetime
label_to_offset_ms = _store_utils.label_to_offset_ms
matching_log_path = _store_utils.matching_log_path
meeting_can_generate_note = _store_utils.meeting_can_generate_note
normalize_final_transcript_status = _store_utils.normalize_final_transcript_status
normalize_note_field_name = _store_utils.normalize_note_field_name
normalize_note_payload = _store_utils.normalize_note_payload
normalize_owner_username = _store_utils.normalize_owner_username
offset_ms_to_label = _store_utils.offset_ms_to_label
parse_datetime_value = _store_utils.parse_datetime_value
parse_legacy_segments = _store_utils.parse_legacy_segments
read_text = _store_utils.read_text
resolve_database_dsn = _store_utils.resolve_database_dsn
resolve_owner_username = _store_utils.resolve_owner_username
shorten = _store_utils.shorten
transcript_name_for_source = _store_utils.transcript_name_for_source
