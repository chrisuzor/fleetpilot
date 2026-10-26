-- Desired fleet. Actual instance state lives in the provider, not here.

CREATE TABLE IF NOT EXISTS instances (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    image TEXT NOT NULL,
    size TEXT NOT NULL,
    region TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

-- At most one pending or running job per instance name.
CREATE TABLE IF NOT EXISTS jobs (
    id TEXT PRIMARY KEY,
    instance_name TEXT NOT NULL,
    op TEXT NOT NULL,
    image TEXT NOT NULL DEFAULT '',
    size TEXT NOT NULL DEFAULT '',
    region TEXT NOT NULL DEFAULT '',
    provider_id TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL,
    attempts INT NOT NULL DEFAULT 0,
    run_after TIMESTAMPTZ NOT NULL,
    last_error TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS jobs_one_active_per_instance
    ON jobs (instance_name)
    WHERE status IN ('pending', 'running');

-- Row lock taken for the duration of one provider call.
CREATE TABLE IF NOT EXISTS instance_locks (
    name TEXT PRIMARY KEY
);
