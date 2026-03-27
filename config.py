import os
from pathlib import Path
from dotenv import load_dotenv

_cwd_dotenv = Path.cwd() / ".env"
if _cwd_dotenv.exists():
    load_dotenv(_cwd_dotenv)
else:
    load_dotenv()


def env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None:
        return default
    try:
        return int(value)
    except ValueError:
        return default


def env_float(name: str, default: float) -> float:
    value = os.getenv(name)
    if value is None:
        return default
    try:
        return float(value)
    except ValueError:
        return default


def env_path(name: str, default: str) -> Path:
    value = os.getenv(name)
    resolved = Path(value) if value else Path(default)
    if not resolved.is_absolute():
        resolved = Path.cwd() / resolved
    return resolved.resolve()


SECRET_KEY = os.getenv("FLASK_SECRET_KEY") or os.getenv("SECRET_KEY") or "meeting-intelligence-dev-key"
if SECRET_KEY == "meeting-intelligence-dev-key":
    import logging as _logging

    _logging.getLogger(__name__).warning(
        "Using default dev SECRET_KEY. Set FLASK_SECRET_KEY or SECRET_KEY for production."
    )
LDAP_HOST = os.getenv("LDAP_HOST")
LDAP_PORT = env_int("LDAP_PORT", 389)
LDAP_BASE_DN = os.getenv("LDAP_BASE_DN")
LDAP_USER_DN_FORMAT = os.getenv("LDAP_USER_DN_FORMAT")

OLLAMA_BASE_URL = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "qwen2.5:14b")
OLLAMA_TIMEOUT = int(os.getenv("OLLAMA_TIMEOUT", "300"))
TRANSCRIPTION_WORKER_BASE_URL = os.getenv("TRANSCRIPTION_WORKER_BASE_URL", "http://localhost:5052").rstrip("/")
NOTE_WORKER_BASE_URL = os.getenv("NOTE_WORKER_BASE_URL", "http://localhost:5053").rstrip("/")
WORKER_INTERNAL_TOKEN = os.getenv("WORKER_INTERNAL_TOKEN", "").strip()
WORKER_REQUEST_TIMEOUT = env_int("WORKER_REQUEST_TIMEOUT", max(OLLAMA_TIMEOUT, 300))
SMTP_HOST = os.getenv("SMTP_HOST", "").strip()
SMTP_PORT = env_int("SMTP_PORT", 25)
SMTP_USER = os.getenv("SMTP_USER", "").strip()
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD", "")
MAIL_FROM = os.getenv("MAIL_FROM", "").strip()
MAIL_TO_DIKKAN = os.getenv("MAIL_TO_DIKKAN", "").strip()
SMTP_TIMEOUT = env_int("SMTP_TIMEOUT", 20)
MEETINGAI_DATA_DIR = env_path("MEETINGAI_DATA_DIR", "data")
MEETINGAI_TRANSCRIPTS_DIR = env_path("MEETINGAI_TRANSCRIPTS_DIR", str(MEETINGAI_DATA_DIR / "transcripts"))
MEETINGAI_NOTES_DIR = env_path("MEETINGAI_NOTES_DIR", str(MEETINGAI_DATA_DIR / "notes"))
MEETINGAI_MOBILE_AUDIO_DIR = env_path("MEETINGAI_MOBILE_AUDIO_DIR", str(MEETINGAI_DATA_DIR / "mobile_audio"))

AUDIO_FRAME_SEC = float(os.getenv("AUDIO_FRAME_SEC", "0.2"))
AUDIO_RMS_THRESHOLD = float(os.getenv("AUDIO_RMS_THRESHOLD", "0.001"))
AUDIO_PRE_BUFFER_SEC = float(os.getenv("AUDIO_PRE_BUFFER_SEC", "0.5"))
AUDIO_END_SILENCE_SEC = float(os.getenv("AUDIO_END_SILENCE_SEC", "0.8"))
AUDIO_MIN_SPEECH_SEC = float(os.getenv("AUDIO_MIN_SPEECH_SEC", "0.7"))
AUDIO_MAX_SEGMENT_SEC = float(os.getenv("AUDIO_MAX_SEGMENT_SEC", "20.0"))
AUDIO_VAD_BACKEND = os.getenv("AUDIO_VAD_BACKEND", "auto")
AUDIO_VAD_AGGRESSIVENESS = int(os.getenv("AUDIO_VAD_AGGRESSIVENESS", "2"))
AUDIO_VAD_VOTE_RATIO = float(os.getenv("AUDIO_VAD_VOTE_RATIO", "0.4"))
AUDIO_MIN_SEGMENT_RMS = env_float("AUDIO_MIN_SEGMENT_RMS", max(AUDIO_RMS_THRESHOLD * 1.2, 0.0012))
AUDIO_SHORT_SEGMENT_SEC = env_float("AUDIO_SHORT_SEGMENT_SEC", 1.0)
AUDIO_SHORT_SEGMENT_MIN_RMS = env_float("AUDIO_SHORT_SEGMENT_MIN_RMS", max(AUDIO_RMS_THRESHOLD * 1.8, 0.0018))
AUDIO_SHORT_SEGMENT_MIN_VOICED_RATIO = env_float("AUDIO_SHORT_SEGMENT_MIN_VOICED_RATIO", 0.58)

TRANSCRIPT_MIN_CHARS = int(os.getenv("TRANSCRIPT_MIN_CHARS", "3"))
TRANSCRIPT_FILLER_WORDS = {
    word.strip().lower()
    for word in os.getenv("TRANSCRIPT_FILLER_WORDS", "eee,Ä±Ä±Ä±,ee,aa,hmm,mmm,Ä±Ä±,eee").split(",")
    if word.strip()
}
TRANSCRIPT_SHORT_WHITELIST = {
    word.strip().lower()
    for word in os.getenv("TRANSCRIPT_SHORT_WHITELIST", "evet,hayÄ±r,tamam,olur,yok,var,tabi,tabii,aynen").split(",")
    if word.strip()
}
TRANSCRIPT_BLOCKED_PHRASES = {
    phrase.strip().lower()
    for phrase in os.getenv("TRANSCRIPT_BLOCKED_PHRASES", "altyazÄ±,altyazÄ± m k,m.k.,m k,mk,hm,hmm").split(",")
    if phrase.strip()
}
TRANSCRIPT_MAX_REPEAT_WORD_RUN = int(os.getenv("TRANSCRIPT_MAX_REPEAT_WORD_RUN", "4"))
TRANSCRIPT_MAX_REPEAT_PHRASE_RUN = int(os.getenv("TRANSCRIPT_MAX_REPEAT_PHRASE_RUN", "3"))
TRANSCRIPT_DOMINANT_TOKEN_RATIO = float(os.getenv("TRANSCRIPT_DOMINANT_TOKEN_RATIO", "0.45"))
TRANSCRIPT_MIN_UNIQUE_TOKEN_RATIO = float(os.getenv("TRANSCRIPT_MIN_UNIQUE_TOKEN_RATIO", "0.35"))

STT_DEVICE = os.getenv("STT_DEVICE", "auto")
STT_COMPUTE_TYPE = os.getenv("STT_COMPUTE_TYPE", "int8")
STT_BEAM_SIZE = int(os.getenv("STT_BEAM_SIZE", "2"))
STT_TEMPERATURE = float(os.getenv("STT_TEMPERATURE", "0.0"))
STT_VAD_FILTER = env_bool("STT_VAD_FILTER", True)
STT_CONDITION_ON_PREVIOUS_TEXT = env_bool("STT_CONDITION_ON_PREVIOUS_TEXT", False)
STT_REPETITION_PENALTY = float(os.getenv("STT_REPETITION_PENALTY", "1.1"))
STT_NO_REPEAT_NGRAM_SIZE = int(os.getenv("STT_NO_REPEAT_NGRAM_SIZE", "3"))
STT_ENABLE_REFINEMENT = env_bool("STT_ENABLE_REFINEMENT", True)
STT_FINAL_MODEL = os.getenv("STT_FINAL_MODEL", "large-v3")
STT_FINAL_BEAM_SIZE = int(os.getenv("STT_FINAL_BEAM_SIZE", "5"))

RECORDING_STALE_MINUTES = env_int("RECORDING_STALE_MINUTES", 10)
MOBILE_AUTO_STOP_NO_SPEECH_SECONDS = env_int("MOBILE_AUTO_STOP_NO_SPEECH_SECONDS", 120)
