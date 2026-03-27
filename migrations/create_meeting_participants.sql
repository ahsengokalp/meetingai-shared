CREATE TABLE IF NOT EXISTS meeting_participants (
    meeting_id BIGINT NOT NULL REFERENCES meetings(id) ON DELETE CASCADE,
    user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (meeting_id, user_id)
);

CREATE INDEX IF NOT EXISTS idx_meeting_participants_user_id
    ON meeting_participants (user_id);

CREATE INDEX IF NOT EXISTS idx_meeting_participants_meeting_id
    ON meeting_participants (meeting_id);
