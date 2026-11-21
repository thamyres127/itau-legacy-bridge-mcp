CREATE TABLE IF NOT EXISTS event_store (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_id UUID NOT NULL UNIQUE,
    aggregate_id UUID NOT NULL,
    aggregate_type VARCHAR(100) NOT NULL,
    aggregate_version INTEGER NOT NULL,
    event_type VARCHAR(150) NOT NULL,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    occurred_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT event_store_version_positive
        CHECK (aggregate_version > 0),

    CONSTRAINT event_store_payload_object
        CHECK (jsonb_typeof(payload) = 'object'),

    CONSTRAINT event_store_metadata_object
        CHECK (jsonb_typeof(metadata) = 'object'),

    CONSTRAINT event_store_aggregate_version_unique
        UNIQUE (aggregate_id, aggregate_version)
);

CREATE INDEX IF NOT EXISTS idx_event_store_aggregate_id
    ON event_store (aggregate_id);

CREATE INDEX IF NOT EXISTS idx_event_store_aggregate_type
    ON event_store (aggregate_type);

CREATE INDEX IF NOT EXISTS idx_event_store_event_type
    ON event_store (event_type);

CREATE INDEX IF NOT EXISTS idx_event_store_occurred_at
    ON event_store (occurred_at);

CREATE INDEX IF NOT EXISTS idx_event_store_payload
    ON event_store USING GIN (payload);

CREATE INDEX IF NOT EXISTS idx_event_store_metadata
    ON event_store USING GIN (metadata);
