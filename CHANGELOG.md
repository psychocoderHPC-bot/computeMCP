# Changelog

All notable changes to computeMCP are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project uses
semantic versioning.

## Unreleased

### Added

- Durable multi-job Slurm targets. A target with a `[targets.X.slurm]` block
  and a `[targets.X.bundle]` block separates the login route from its jobs.
- `target-open`, `target-shared`, and `target-sbatch` operator commands, plus
  `job-list`, `job-status`, `job-wait`, and `job-stop`.
- `computeMCP_job_start`, `computeMCP_job_list`, `computeMCP_job_status`,
  `computeMCP_job_wait`, and `computeMCP_job_stop` MCP tools. Exec, PTY, and
  file tools accept an optional `job_id`.
- Per-client, per-target private-job quotas through
  `[clients.<id>.job_limits]` and repeated
  `approve --job-limit TARGET=N` entries. The default is one private job per
  authorized Slurm target. A value of `0` disables private submissions.
- Root mode selection (`fakeroot` or `clean`) and home selection for private
  jobs. A request-level `clean_home` wins over the target-level default, and
  both fall back to the private home root. `home_base` and `clean_home` are
  forwarded to the bundle as `COMPUTEMCP_HOME_BASE` and
  `COMPUTEMCP_CLEAN_HOME`.
- Implicit `sbatch` default emissions for calculated fields that no mapping
  names, and the nested `[slurm.sbatch.mapping]` spelling merged with the
  legacy `sbatch-map` table.
- A login-node mux (`computemcp-mux.py`) that carries every job over one
  authenticated outer channel and one stream per job.
- Durable job records under `<state-root>/jobs/<job_id>/` with atomic writes,
  quota reservations, idempotency keys, and crash reconciliation. A job whose
  submission acknowledgement is lost stays `submission_unknown` and is never
  resubmitted automatically.

### Changed and compatibility breaks

These changes apply to a durable multi-job Slurm target unless stated
otherwise.

- `auto_connect = true` on a durable multi-job Slurm target opens the route
  only. It never submits a job or starts a shared container.
- `target-connect` on a durable multi-job Slurm target composes route open and
  shared-job start. It can return `pending` and no longer promises that
  container SSH is immediately available.
- Gateway shutdown, `target-stop`, and `target-refresh` detach a durable
  multi-job Slurm target without cancelling jobs. Cancellation requires an
  explicit `job-stop`.
- The `sbatch` stage emits implicit defaults for calculated fields. A manual
  option that owns a resource family suppresses that default.

### Fixed

- `target-sbatch --2fa` forwards the factor to route opening.
- `gatewayctl approve --job-limit` writes the map into the new client block,
  reloads, and returns the applied `job_limits`.
- The preview route returns `root_mode`, `home`, `clean_home`,
  `shared_reuse`, `settings_conflict`, and `quota_available`.
- The bundle documentation now names five helper files: provision, container,
  job, relay, and mux.
