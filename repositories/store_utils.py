from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone
import os
from pathlib import Path
import re
from typing import Any


SEGMENT_PATTERN = re.compile(r"^\[(.*?)\s*-\s*(.*?)\]\s*(.*)$")
STAMP_PATTERN = re.compile(r"(\d{8}_\d{6})")
NOTE_FIELD_CHAR_REPLACEMENTS: tuple[tuple[str, str], ...] = (
    ("ç", "c"),
    ("ğ", "g"),
    ("ı", "i"),
    ("ö", "o"),
    ("ş", "s"),
    ("ü", "u"),
    ("Ç", "c"),
    ("Ğ", "g"),
    ("İ", "i"),
    ("Ö", "o"),
    ("Ş", "s"),
    ("Ü", "u"),
    ("Ã§", "c"),
    ("ÄŸ", "g"),
    ("Ä±", "i"),
    ("Ã¶", "o"),
    ("ÅŸ", "s"),
    ("Ã¼", "u"),
    ("Ã‡", "c"),
    ("Ä", "g"),
    ("Ä°", "i"),
    ("Ã–", "o"),
    ("Å", "s"),
    ("Ãœ", "u"),
    ("ÃƒÂ§", "c"),
    ("Ã„Å¸", "g"),
    ("Ã„Â±", "i"),
    ("ÃƒÂ¶", "o"),
    ("Ã…Å¸", "s"),
    ("ÃƒÂ¼", "u"),
    ("Ãƒâ€¡", "c"),
    ("Ã„Â", "g"),
    ("Ã„Â°", "i"),
    ("Ãƒâ€“", "o"),
    ("Ã…Â", "s"),
    ("ÃƒÅ“", "u"),
)


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
                "Ã§": "c",
                "ÄŸ": "g",
                "Ä±": "i",
                "Ã¶": "o",
                "ÅŸ": "s",
                "Ã¼": "u",
                "Ã‡": "c",
                "Ä": "g",
                "Ä°": "i",
                "Ã–": "o",
                "Å": "s",
                "Ãœ": "u",
            }
        )
    )
    return "_".join(text.replace("-", " ").split()).lower()


def _normalize_note_field_name_safe(value: Any) -> str:
    text = str(value or "").strip()
    for source, target in NOTE_FIELD_CHAR_REPLACEMENTS:
        text = text.replace(source, target)
    return "_".join(text.replace("-", " ").split()).lower()


normalize_note_field_name = _normalize_note_field_name_safe


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
