from __future__ import annotations

from datetime import datetime
import json
import os
from typing import Any, Sequence
from uuid import uuid4

import psycopg
from psycopg.types.json import Jsonb

from meetingai_shared.repositories.store_utils import (
    compact_text,
    current_timestamp,
    ensure_aware_datetime,
    ensure_list,
    format_datetime_value,
    format_display_datetime,
    normalize_note_payload,
    normalize_note_source,
    normalize_owner_username,
    parse_datetime_value,
    shorten,
)


class StoreNoteMailParticipantMixin:
    def create_note(
        self,
        meeting_id: int,
        data: dict[str, Any],
        *,
        created_at: str | None = None,
        source: str = "generated",
        owner_username: str | None = None,
    ) -> int:
        self.ensure_ready()
        payload = normalize_note_payload(data)
        created_dt = ensure_aware_datetime(parse_datetime_value(created_at)) or current_timestamp()
        note_source = normalize_note_source(source)

        with self.connection() as conn:
            if owner_username is not None:
                self._assert_meeting_write_access(conn, meeting_id, owner_username)
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT COALESCE(MAX(version_no), 0) + 1 AS next_version
                    FROM meeting_note_versions
                    WHERE meeting_id = %s
                    """,
                    (meeting_id,),
                )
                version_no = int(cur.fetchone()["next_version"])
                cur.execute(
                    """
                    UPDATE meeting_note_versions
                    SET is_current = FALSE
                    WHERE meeting_id = %s AND is_current = TRUE
                    """,
                    (meeting_id,),
                )
                cur.execute(
                    """
                    INSERT INTO meeting_note_versions (
                        meeting_id, version_no, title, summary, payload,
                        llm_model, source, is_current, created_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, TRUE, %s)
                    RETURNING id
                    """,
                    (
                        meeting_id,
                        version_no,
                        payload["title"],
                        payload["summary"],
                        Jsonb(
                            {
                                "context_and_objective": payload["context_and_objective"],
                                "main_topics": payload["main_topics"],
                                "participant_contributions": payload["participant_contributions"],
                                "decisions": payload["decisions"],
                                "decision_details": payload["decision_details"],
                                "action_items": payload["action_items"],
                                "risks": payload["risks"],
                                "open_questions": payload["open_questions"],
                                "open_items": payload["open_items"],
                                "tags": payload["tags"],
                            }
                        ),
                        os.getenv("OLLAMA_MODEL"),
                        note_source,
                        created_dt,
                    ),
                )
                note_id = int(cur.fetchone()["id"])

                if payload["title"]:
                    cur.execute(
                        """
                        UPDATE meetings
                        SET title = CASE WHEN COALESCE(title, '') = '' THEN %s ELSE title END,
                            updated_at = %s
                        WHERE id = %s
                        """,
                        (payload["title"], created_dt, meeting_id),
                    )
        return note_id

    def search_users(self, query: str, limit: int = 8) -> list[dict[str, Any]]:
        text = " ".join(str(query or "").split()).strip()
        if len(text) < 2:
            return []

        normalized_limit = max(1, min(int(limit), 20))
        lowered = text.casefold()
        prefix = f"{lowered}%"
        contains = f"%{lowered}%"

        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        id,
                        first_name,
                        last_name,
                        full_name,
                        email,
                        job_title
                    FROM users
                    WHERE
                        lower(full_name) LIKE %s
                        OR lower(email) LIKE %s
                        OR lower(job_title) LIKE %s
                    ORDER BY
                        CASE
                            WHEN lower(full_name) = %s THEN 0
                            WHEN lower(full_name) LIKE %s THEN 1
                            WHEN lower(email) LIKE %s THEN 2
                            ELSE 3
                        END,
                        full_name ASC,
                        email ASC
                    LIMIT %s
                    """,
                    (
                        contains,
                        contains,
                        contains,
                        lowered,
                        prefix,
                        prefix,
                        normalized_limit,
                    ),
                )
                rows = cur.fetchall()
        return [self._directory_user_row_to_dict(row) for row in rows]

    def replace_meeting_participants(
        self,
        meeting_id: int,
        user_ids: Sequence[int] | None,
        owner_username: str | None = None,
    ) -> list[dict[str, Any]]:
        participant_ids = sorted({int(user_id) for user_id in (user_ids or []) if int(user_id) > 0})

        with self.connection() as conn:
            self._assert_meeting_write_access(conn, meeting_id, owner_username)
            with conn.cursor() as cur:
                cur.execute("DELETE FROM meeting_participants WHERE meeting_id = %s", (meeting_id,))
                for user_id in participant_ids:
                    cur.execute(
                        """
                        INSERT INTO meeting_participants (meeting_id, user_id)
                        VALUES (%s, %s)
                        ON CONFLICT (meeting_id, user_id) DO NOTHING
                        """,
                        (meeting_id, user_id),
                    )
        return self.list_meeting_participants(meeting_id, owner_username)

    def list_meeting_participants(
        self,
        meeting_id: int,
        owner_username: str | None = None,
    ) -> list[dict[str, Any]]:
        with self.connection() as conn:
            self._assert_meeting_write_access(conn, meeting_id, owner_username)
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        u.id,
                        u.first_name,
                        u.last_name,
                        u.full_name,
                        u.email,
                        u.job_title
                    FROM meeting_participants mp
                    JOIN users u ON u.id = mp.user_id
                    WHERE mp.meeting_id = %s
                    ORDER BY u.full_name ASC, u.email ASC
                    """,
                    (meeting_id,),
                )
                rows = cur.fetchall()
        return [self._directory_user_row_to_dict(row) for row in rows]

    def record_mail_delivery_attempt(
        self,
        meeting_id: int,
        *,
        note_id: int | None,
        subject: str,
        recipients: Sequence[dict[str, Any] | str] | None,
        status: str,
        trigger_source: str = "analyze",
        requested_by: str | None = None,
        owner_username: str | None = None,
        error_message: str | None = None,
        attempt_key: str | None = None,
        attempted_at: str | datetime | None = None,
    ) -> dict[str, Any]:
        normalized_recipients: list[dict[str, str]] = []
        for recipient in recipients or []:
            if isinstance(recipient, dict):
                email = str(recipient.get("email") or "").strip().lower()
                name = str(recipient.get("name") or "").strip()
            else:
                email = str(recipient or "").strip().lower()
                name = ""
            if not email:
                continue
            normalized_recipients.append({"email": email, "name": name})

        attempt_token = str(attempt_key or uuid4().hex)
        attempted_dt = ensure_aware_datetime(parse_datetime_value(attempted_at)) or current_timestamp()
        normalized_status = str(status or "").strip().lower() or "failed"
        normalized_error = str(error_message or "").strip()
        normalized_requester = normalize_owner_username(requested_by or owner_username)

        if normalized_recipients:
            with self.connection() as conn:
                self._assert_meeting_write_access(conn, meeting_id, owner_username)
                with conn.cursor() as cur:
                    for recipient in normalized_recipients:
                        cur.execute(
                            """
                            INSERT INTO meeting_note_mail_deliveries (
                                meeting_id,
                                attempt_key,
                                recipient_email,
                                recipient_name,
                                status,
                                error_message,
                                mail_subject,
                                trigger_source,
                                requested_by,
                                created_at
                            )
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            """,
                            (
                                meeting_id,
                                attempt_token,
                                recipient["email"],
                                recipient["name"],
                                normalized_status,
                                normalized_error,
                                subject,
                                str(trigger_source or "analyze").strip().lower() or "analyze",
                                normalized_requester,
                                attempted_dt,
                            ),
                        )

        recipient_emails = [recipient["email"] for recipient in normalized_recipients]
        sent_count = len(recipient_emails) if normalized_status == "sent" else 0
        failed_count = len(recipient_emails) if normalized_status == "failed" else 0
        return {
            "attempt_key": attempt_token,
            "meeting_id": meeting_id,
            "note_id": note_id,
            "subject": subject,
            "trigger_source": str(trigger_source or "analyze").strip().lower() or "analyze",
            "requested_by": normalized_requester,
            "created_at": format_datetime_value(attempted_dt),
            "modified_at": format_display_datetime(attempted_dt),
            "status": normalized_status,
            "recipient_count": len(recipient_emails),
            "sent_count": sent_count,
            "failed_count": failed_count,
            "error_message": normalized_error,
            "recipients": recipient_emails,
        }

    def list_mail_delivery_batches(
        self,
        owner_username: str | None = None,
        *,
        meeting_id: int | None = None,
        limit: int = 20,
    ) -> list[dict[str, Any]]:
        self.ensure_ready()
        filters: list[str] = []
        params: list[Any] = []

        if meeting_id is not None:
            filters.append("l.meeting_id = %s")
            params.append(meeting_id)

        owner_key = normalize_owner_username(owner_username)
        if owner_key:
            filters.append("lower(u.username) = %s")
            params.append(owner_key)

        row_limit = max(20, min(max(int(limit), 1), 100) * 25)
        params.append(row_limit)

        query = """
            SELECT
                l.id,
                l.attempt_key,
                l.meeting_id,
                l.recipient_email,
                l.recipient_name,
                l.status,
                l.error_message,
                l.mail_subject,
                l.trigger_source,
                l.requested_by,
                l.created_at,
                COALESCE(NULLIF(m.title, ''), NULLIF(m.source_name, ''), 'Meeting ' || m.id::text) AS meeting_title
            FROM meeting_note_mail_deliveries l
            JOIN meetings m ON m.id = l.meeting_id
            JOIN app_users u ON u.id = m.owner_user_id
        """
        if filters:
            query += " WHERE " + " AND ".join(filters)
        query += " ORDER BY l.created_at DESC, l.id DESC LIMIT %s"

        with self.connection() as conn:
            with conn.cursor() as cur:
                try:
                    cur.execute(query, params)
                except psycopg.errors.UndefinedTable:
                    return []
                rows = cur.fetchall()

        return self._group_mail_delivery_rows(rows, limit=limit)

    def get_meeting_mail_summary(
        self,
        meeting_id: int,
        owner_username: str | None = None,
    ) -> dict[str, Any]:
        batches = self.list_mail_delivery_batches(
            owner_username,
            meeting_id=meeting_id,
            limit=10,
        )
        if not batches:
            return {
                "attempt_count": 0,
                "recipient_count": 0,
                "sent_count": 0,
                "failed_count": 0,
                "latest_created_at": None,
                "latest_modified_at": "",
                "latest_status": "none",
                "latest_recipient_count": 0,
                "latest_subject": "",
                "latest_note_id": None,
                "latest_note_title": "",
                "latest_trigger_source": "",
                "latest_error_message": "",
            }

        latest = batches[0]
        return {
            "attempt_count": len(batches),
            "recipient_count": sum(int(batch["recipient_count"] or 0) for batch in batches),
            "sent_count": sum(int(batch["sent_count"] or 0) for batch in batches),
            "failed_count": sum(int(batch["failed_count"] or 0) for batch in batches),
            "latest_created_at": latest["created_at"],
            "latest_modified_at": latest["modified_at"],
            "latest_status": latest["status"],
            "latest_recipient_count": latest["recipient_count"],
            "latest_subject": latest["subject"],
            "latest_note_id": latest["note_id"],
            "latest_note_title": latest["note_title"],
            "latest_trigger_source": latest["trigger_source"],
            "latest_error_message": latest["error_message"],
        }

    def list_notes(
        self,
        meeting_id: int | None = None,
        owner_username: str | None = None,
    ) -> list[dict[str, Any]]:
        self.ensure_ready()
        params: list[Any] = []
        filters: list[str] = []
        if meeting_id is not None:
            filters.append("n.meeting_id = %s")
            params.append(meeting_id)

        owner_key = normalize_owner_username(owner_username)
        if owner_key:
            filters.append("lower(u.username) = %s")
            params.append(owner_key)

        query = """
            SELECT
                n.id,
                n.meeting_id,
                n.title,
                n.summary,
                n.source,
                n.is_current,
                n.created_at
            FROM meeting_note_versions n
            JOIN meetings m ON m.id = n.meeting_id
            JOIN app_users u ON u.id = m.owner_user_id
        """
        if filters:
            query += " WHERE " + " AND ".join(filters)
        query += " ORDER BY n.created_at DESC, n.id DESC"

        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                rows = cur.fetchall()
        return [self._note_summary_row_to_dict(row) for row in rows]

    def get_note(
        self,
        note_id: int,
        owner_username: str | None = None,
    ) -> dict[str, Any] | None:
        self.ensure_ready()
        params: list[Any] = [note_id]
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
                        n.id,
                        n.meeting_id,
                        n.version_no,
                        n.title,
                        n.summary,
                        n.payload,
                        n.llm_model,
                        n.source,
                        n.is_current,
                        n.created_at
                    FROM meeting_note_versions n
                    JOIN meetings m ON m.id = n.meeting_id
                    JOIN app_users u ON u.id = m.owner_user_id
                    WHERE n.id = %s
                    """
                    + owner_clause,
                    params,
                )
                row = cur.fetchone()
        if row is None:
            return None
        return self._note_row_to_dict(row)

    def _note_summary_row_to_dict(self, row: dict[str, Any]) -> dict[str, Any]:
        return {
            "id": int(row["id"]),
            "meeting_id": int(row["meeting_id"]),
            "name": f"note_{row['id']}",
            "title": row["title"] or f"Note {row['id']}",
            "summary": shorten(row["summary"] or ""),
            "modified_at": format_display_datetime(row["created_at"]),
            "created_at": format_datetime_value(row["created_at"]),
            "source": row["source"],
            "is_current": bool(row["is_current"]),
        }

    def _directory_user_row_to_dict(self, row: dict[str, Any]) -> dict[str, Any]:
        return {
            "id": int(row["id"]),
            "first_name": row.get("first_name") or "",
            "last_name": row.get("last_name") or "",
            "full_name": row.get("full_name") or "",
            "email": row.get("email") or "",
            "job_title": row.get("job_title") or "",
        }

    def _group_mail_delivery_rows(
        self,
        rows: Sequence[dict[str, Any]],
        *,
        limit: int,
    ) -> list[dict[str, Any]]:
        batches: dict[str, dict[str, Any]] = {}
        max_batches = max(int(limit), 1)

        for row in rows:
            attempt_key = str(row.get("attempt_key") or f"mail-{row.get('id')}")
            batch = batches.get(attempt_key)
            if batch is None:
                if len(batches) >= max_batches:
                    break
                created_at = row.get("created_at")
                batch = {
                    "attempt_key": attempt_key,
                    "meeting_id": int(row["meeting_id"]),
                    "note_id": None,
                    "meeting_title": row.get("meeting_title") or f"Meeting {row['meeting_id']}",
                    "note_title": "",
                    "subject": row.get("mail_subject") or "",
                    "trigger_source": row.get("trigger_source") or "analyze",
                    "requested_by": row.get("requested_by") or "",
                    "created_at": format_datetime_value(created_at),
                    "modified_at": format_display_datetime(created_at),
                    "recipient_count": 0,
                    "sent_count": 0,
                    "failed_count": 0,
                    "status": "sent",
                    "error_message": "",
                    "recipients": [],
                }
                batches[attempt_key] = batch

            recipient_email = str(row.get("recipient_email") or "").strip().lower()
            batch["recipient_count"] += 1
            if recipient_email and recipient_email not in batch["recipients"]:
                batch["recipients"].append(recipient_email)

            if str(row.get("status") or "").strip().lower() == "sent":
                batch["sent_count"] += 1
            else:
                batch["failed_count"] += 1
                if not batch["error_message"]:
                    batch["error_message"] = str(row.get("error_message") or "").strip()

        for batch in batches.values():
            if batch["sent_count"] and batch["failed_count"]:
                batch["status"] = "partial"
            elif batch["failed_count"]:
                batch["status"] = "failed"
            elif batch["sent_count"]:
                batch["status"] = "sent"
            else:
                batch["status"] = "skipped"

        return list(batches.values())

    def _note_row_to_dict(self, row: dict[str, Any]) -> dict[str, Any]:
        payload = row["payload"]
        if isinstance(payload, str):
            payload = json.loads(payload or "{}")
        if not isinstance(payload, dict):
            payload = {}

        return {
            "id": int(row["id"]),
            "meeting_id": int(row["meeting_id"]),
            "title": row["title"] or "",
            "summary": row["summary"] or "",
            "context_and_objective": compact_text(payload.get("context_and_objective")),
            "main_topics": ensure_list(payload.get("main_topics")),
            "participant_contributions": ensure_list(payload.get("participant_contributions")),
            "decisions": ensure_list(payload.get("decisions")),
            "decision_details": ensure_list(payload.get("decision_details")),
            "action_items": ensure_list(payload.get("action_items")),
            "risks": ensure_list(payload.get("risks")),
            "open_questions": ensure_list(payload.get("open_questions")),
            "open_items": ensure_list(payload.get("open_items")),
            "tags": ensure_list(payload.get("tags")),
            "created_at": format_datetime_value(row["created_at"]),
            "modified_at": format_display_datetime(row["created_at"]),
            "source": row["source"],
            "version_no": int(row["version_no"]),
            "is_current": bool(row["is_current"]),
            "llm_model": row["llm_model"],
        }
