ALTER TABLE meetings
    ADD COLUMN IF NOT EXISTS final_transcript_status TEXT;

ALTER TABLE meetings
    ADD COLUMN IF NOT EXISTS final_transcript_text TEXT;

ALTER TABLE meetings
    ADD COLUMN IF NOT EXISTS final_transcript_error TEXT;

ALTER TABLE meetings
    ADD COLUMN IF NOT EXISTS final_transcript_model TEXT;

ALTER TABLE meetings
    ADD COLUMN IF NOT EXISTS final_transcript_generated_at TIMESTAMPTZ;

UPDATE meetings
SET
    final_transcript_status = COALESCE(NULLIF(final_transcript_status, ''), 'completed'),
    final_transcript_text = COALESCE(final_transcript_text, ''),
    final_transcript_error = COALESCE(final_transcript_error, ''),
    final_transcript_model = COALESCE(final_transcript_model, '')
WHERE
    final_transcript_status IS NULL
    OR final_transcript_text IS NULL
    OR final_transcript_error IS NULL
    OR final_transcript_model IS NULL
    OR final_transcript_status = '';

ALTER TABLE meetings
    ALTER COLUMN final_transcript_status SET DEFAULT 'completed';

ALTER TABLE meetings
    ALTER COLUMN final_transcript_status SET NOT NULL;

ALTER TABLE meetings
    ALTER COLUMN final_transcript_text SET DEFAULT '';

ALTER TABLE meetings
    ALTER COLUMN final_transcript_text SET NOT NULL;

ALTER TABLE meetings
    ALTER COLUMN final_transcript_error SET DEFAULT '';

ALTER TABLE meetings
    ALTER COLUMN final_transcript_error SET NOT NULL;

ALTER TABLE meetings
    ALTER COLUMN final_transcript_model SET DEFAULT '';

ALTER TABLE meetings
    ALTER COLUMN final_transcript_model SET NOT NULL;

CREATE INDEX IF NOT EXISTS idx_meetings_final_transcript_status
    ON meetings (final_transcript_status);
