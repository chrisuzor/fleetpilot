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

-- Stored health checks. The newest checked_at for an instance is the latest result.
CREATE TABLE IF NOT EXISTS health_checks (
    id TEXT PRIMARY KEY,
    instance_id TEXT NOT NULL,
    status TEXT NOT NULL,
    detail TEXT NOT NULL DEFAULT '',
    checked_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS health_checks_instance_checked
    ON health_checks (instance_id, checked_at DESC, id DESC);

-- One running rollout. instance_names is fixed when the rollout starts.
CREATE TABLE IF NOT EXISTS rollouts (
    id TEXT PRIMARY KEY,
    image TEXT NOT NULL,
    size TEXT NOT NULL,
    region TEXT NOT NULL,
    status TEXT NOT NULL,
    pause_reason TEXT NOT NULL DEFAULT '',
    instance_names TEXT[] NOT NULL,
    next_index INT NOT NULL,
    phase TEXT NOT NULL,
    job_id TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS rollouts_one_running
    ON rollouts (status)
    WHERE status = 'running';
