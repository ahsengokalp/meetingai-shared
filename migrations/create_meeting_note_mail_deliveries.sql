CREATE TABLE IF NOT EXISTS meeting_note_mail_deliveries (
    id BIGSERIAL PRIMARY KEY,
    meeting_id BIGINT NOT NULL REFERENCES meetings(id) ON DELETE CASCADE,
    attempt_key TEXT NOT NULL,
    recipient_email TEXT NOT NULL,
    recipient_name TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL,
    error_message TEXT NOT NULL DEFAULT '',
    mail_subject TEXT NOT NULL DEFAULT '',
    trigger_source TEXT NOT NULL DEFAULT 'analyze',
    requested_by TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_meeting_note_mail_deliveries_meeting_id
    ON meeting_note_mail_deliveries (meeting_id);

CREATE INDEX IF NOT EXISTS idx_meeting_note_mail_deliveries_attempt_key
    ON meeting_note_mail_deliveries (attempt_key);

CREATE INDEX IF NOT EXISTS idx_meeting_note_mail_deliveries_created_at
    ON meeting_note_mail_deliveries (created_at DESC);
