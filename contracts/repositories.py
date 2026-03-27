from __future__ import annotations

from datetime import datetime
from typing import Any, Protocol, Sequence


class MeetingRepositoryProtocol(Protocol):
    def create_live_meeting(
        self,
        owner_username: str | None = None,
        started_at: str | None = None,
        title: str | None = None,
    ) -> dict[str, Any]: ...

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
    ) -> None: ...

    def get_meeting(self, meeting_id: int, owner_username: str | None = None) -> dict[str, Any] | None: ...
    def list_meetings(self, owner_username: str | None = None) -> list[dict[str, Any]]: ...
    def create_note(
        self,
        meeting_id: int,
        data: dict[str, Any],
        *,
        created_at: str | None = None,
        source: str = "generated",
        owner_username: str | None = None,
    ) -> int: ...

    def list_notes(
        self,
        meeting_id: int | None = None,
        owner_username: str | None = None,
    ) -> list[dict[str, Any]]: ...

    def get_note(self, note_id: int, owner_username: str | None = None) -> dict[str, Any] | None: ...
    def search_users(self, query: str, limit: int = 8) -> list[dict[str, Any]]: ...
    def replace_meeting_participants(
        self,
        meeting_id: int,
        user_ids: Sequence[int] | None,
        owner_username: str | None = None,
    ) -> list[dict[str, Any]]: ...
