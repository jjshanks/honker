# Honker — Implementation Spec

A spec for building Honker from scratch. It states **what must be true**
and leaves **how** to the implementer. Where it is prescriptive — the
on-disk schema and the SQL function surface — it is because those are the
cross-language interop contract, and drifting from them silently breaks
the product's central promise.

Read this as a target, not a set of instructions. If you find a better
internal structure, take it. If you find a reason one of the fixed
contracts below is wrong, say so before changing it.

---

## 1. The product

Honker adds Postgres-style `NOTIFY`/`LISTEN` semantics to SQLite, plus a
durable task queue, durable event streams, a cron scheduler, named locks,
and rate limits — with **no daemon, no broker, and no second datastore**.

Everything lives in the user's existing SQLite file. The defining property:

```python
with db.transaction() as tx:
    tx.execute("INSERT INTO orders (user_id) VALUES (?)", [42])
    emails.enqueue({"to": "alice@example.com"}, tx=tx)
```

The business write and the queue write commit together, or neither does.
That eliminates the dual-write problem that Redis+Celery deployments have
by construction. This is the transactional outbox pattern, done in one
file.

A worker in a **different process** must wake within single-digit
milliseconds of that commit, without polling a queue table.

### Who it's for

Projects where SQLite is already the primary datastore and the answer to
"we need background jobs" would otherwise be "add Redis." Single machine,
one database file.

### Explicit non-goals

Do not build these, even if they seem natural:

- Workflow DAGs, task chains/groups/chords
- Multi-writer replication, distributed locking across machines
- Framework plugins (Django app, Rails engine, etc.) — Honker ships SQL
  functions and thin bindings; framework integration is documentation
- A supervisor/daemon process. Users run their own workers.

Honker is single-machine and file-backed. Two servers writing the same
`.db` over NFS is not a supported deployment.

---

## 2. Core design invariants

These are the properties that make the system correct. Every design
decision should be checkable against them.

**I1 — Everything is a row in the user's database.** Enqueue, publish, and
notify are plain `INSERT`s. They participate in whatever transaction the
caller has open. Rollback drops the work.

**I2 — Wake without polling the queue.** Workers must not poll queue
tables to find work. See §5.

**I3 — Overtrigger, never miss.** A spurious wake costs one indexed
`SELECT`. A missed wake is a correctness bug. When in doubt, wake.

**I4 — One schema, many languages.** A job enqueued by Ruby must be
claimable by Go. The schema and SQL functions are defined once (§3, §4);
bindings are thin wrappers over them. No binding may invent its own
tables or its own column meanings.

**I5 — At-least-once, with visibility timeouts.** A worker that crashes
mid-job must not lose the job. A job that exhausts its attempts must land
somewhere visible, not vanish and not loop forever.

**I6 — Idle costs nothing.** An idle listener runs zero queue or
notification `SELECT`s. 100 subscribers on one database share one watcher.

**I7 — File-backed only.** `:memory:` databases cannot be observed
cross-process. Bindings should make this a clear error rather than a
mystery.

---

## 3. Storage contract (fixed)

This schema is the interop contract. Table names, column names, and
column meanings are fixed. Index definitions are strongly recommended
(they are what makes claim O(pending), not O(history)) but are a
performance contract, not a compatibility one.

All tables are `CREATE TABLE IF NOT EXISTS`; bootstrap must be idempotent
and safe to run concurrently from multiple processes.

```sql
-- Ephemeral pub/sub
_honker_notifications(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  channel TEXT NOT NULL,
  payload TEXT NOT NULL,
  created_at INTEGER NOT NULL DEFAULT (unixepoch())
)
INDEX (channel, id)

-- Live queue
_honker_live(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  queue TEXT NOT NULL,
  payload TEXT NOT NULL,              -- JSON text
  state TEXT NOT NULL DEFAULT 'pending',   -- 'pending' | 'processing'
  priority INTEGER NOT NULL DEFAULT 0,     -- higher claims first
  run_at INTEGER NOT NULL DEFAULT (unixepoch()),
  worker_id TEXT,
  claim_expires_at INTEGER,           -- visibility timeout deadline
  attempts INTEGER NOT NULL DEFAULT 0,
  max_attempts INTEGER NOT NULL DEFAULT 3,
  created_at INTEGER NOT NULL DEFAULT (unixepoch()),
  expires_at INTEGER                  -- NULL = never expires
)
PARTIAL INDEX (queue, priority DESC, run_at, id)
  WHERE state IN ('pending','processing')   -- the claim index
PARTIAL INDEX (queue, run_at)         WHERE state = 'pending'
PARTIAL INDEX (queue, claim_expires_at) WHERE state = 'processing'

-- Dead letter
_honker_dead(
  id INTEGER PRIMARY KEY,             -- preserves the original job id
  queue TEXT NOT NULL, payload TEXT NOT NULL,
  priority INTEGER NOT NULL DEFAULT 0,
  run_at INTEGER NOT NULL DEFAULT 0,
  attempts INTEGER NOT NULL DEFAULT 0,
  max_attempts INTEGER NOT NULL DEFAULT 0,
  last_error TEXT,
  created_at INTEGER NOT NULL DEFAULT (unixepoch()),
  died_at INTEGER NOT NULL DEFAULT (unixepoch())
)

_honker_locks(name TEXT PRIMARY KEY, owner TEXT NOT NULL, expires_at INTEGER NOT NULL)

_honker_rate_limits(name TEXT, window_start INTEGER, count INTEGER NOT NULL DEFAULT 0,
                    PRIMARY KEY (name, window_start))

_honker_scheduler_tasks(
  name TEXT PRIMARY KEY, queue TEXT NOT NULL, cron_expr TEXT NOT NULL,
  payload TEXT NOT NULL, priority INTEGER NOT NULL DEFAULT 0,
  expires_s INTEGER, next_fire_at INTEGER NOT NULL,
  enabled INTEGER NOT NULL DEFAULT 1, max_attempts INTEGER NOT NULL DEFAULT 3
)

_honker_results(job_id INTEGER PRIMARY KEY, value TEXT,
                created_at INTEGER NOT NULL DEFAULT (unixepoch()), expires_at INTEGER)

-- Durable streams
_honker_stream(offset INTEGER PRIMARY KEY AUTOINCREMENT, topic TEXT NOT NULL,
               key TEXT, payload TEXT NOT NULL,
               created_at INTEGER NOT NULL DEFAULT (unixepoch()))
INDEX (topic, offset)

_honker_stream_consumers(name TEXT, topic TEXT, offset INTEGER NOT NULL DEFAULT 0,
                         PRIMARY KEY (name, topic))
```

Notes and rationale worth preserving:

- **Payloads are JSON text.** The engine never interprets them. Bindings
  serialize/deserialize at the edge.
- **Timestamps are unix seconds** (`unixepoch()`), not ISO strings.
- **`_honker_dead` keeps the original id**, so a result row or an external
  reference to a job id still resolves after death.
- **Adding a column later needs a migration path.** Bootstrap should
  detect and `ALTER TABLE ADD COLUMN` missing columns on older databases,
  tolerating the "duplicate column" error from a concurrent bootstrap.

### Connection defaults

Bindings open connections with WAL, `synchronous=NORMAL`, a busy timeout
(~5s), foreign keys on, a page cache larger than SQLite's 2 MB default,
`temp_store=MEMORY`, and a relaxed `wal_autocheckpoint`. These are
defaults, not requirements: correctness — including cross-process wake —
must not depend on WAL. Honker must work on a DELETE-journal database.

---

## 4. SQL function surface (fixed)

Every operation is a SQLite scalar function, so any client that can
`load_extension` gets the full feature set with no binding at all. Names
and argument orders below are the contract. Return values are scalars or
JSON text — SQLite scalar functions cannot return rows.

```
honker_bootstrap()                          -- install schema; idempotent
notify(channel, payload) -> id              -- INSERT into _honker_notifications

-- queue
honker_enqueue(queue, payload, run_at, delay, priority, max_attempts, expires) -> id
honker_claim_batch(queue, worker_id, n, timeout_s) -> JSON array
honker_ack(job_id, worker_id) -> 0|1
honker_ack_batch(ids_json, worker_id) -> count
honker_retry(job_id, worker_id, delay_s, error) -> 0|1
honker_fail(job_id, worker_id, error) -> 0|1
honker_cancel(job_id) -> 0|1
honker_get_job(job_id) -> JSON object or ''
honker_heartbeat(job_id, worker_id, extend_s) -> 0|1
honker_sweep_expired(queue) -> count
honker_queue_next_claim_at(queue) -> unix ts or 0

-- locks / rate limits
honker_lock_acquire(name, owner, ttl_s) -> 0|1
honker_lock_renew(name, owner, ttl_s) -> 0|1
honker_lock_release(name, owner) -> 0|1
honker_rate_limit_try(name, limit, per) -> 0|1
honker_rate_limit_sweep(older_than_s) -> count

-- scheduler
honker_cron_next_after(expr, from_unix) -> unix ts
honker_scheduler_register(name, queue, expr, payload, priority, expires_s, max_attempts)
honker_scheduler_update(...) / honker_scheduler_unregister(name)
honker_scheduler_pause(name) / honker_scheduler_resume(name)
honker_scheduler_tick(now_unix) -> JSON array of fires
honker_scheduler_soonest() -> min next_fire_at, or 0
honker_scheduler_list() -> JSON array

-- results
honker_result_save(job_id, value, ttl_s) / honker_result_get(job_id) / honker_result_sweep()

-- streams
honker_stream_publish(topic, key, payload) -> offset
honker_stream_read_since(topic, offset, limit) -> JSON array
honker_stream_save_offset(consumer, topic, offset)   -- monotonic upsert
honker_stream_get_offset(consumer, topic) -> offset or 0
```

### Semantics that must hold

**Claim** is a single `UPDATE ... RETURNING` through the claim index. It
selects rows that are (`pending` and due) or (`processing` with an expired
visibility timeout), orders by `priority DESC, run_at, id`, limits to `n`,
and in one statement sets `state='processing'`, `worker_id`,
`claim_expires_at = now + timeout_s`, and increments `attempts`. It must
skip rows that have already used their attempt budget and rows past
`expires_at`.

Claiming and reclaiming are the same operation — that is what makes
crash recovery free. A worker that dies holding a claim loses it when the
visibility timeout lapses, and the next claimer picks it up.

**Attempt exhaustion.** Before claiming, move reclaimable rows that have
already spent `max_attempts` into `_honker_dead`. Without this, a worker
that dies after its final allowed attempt leaves a row that is reclaimed
forever, incrementing `attempts` past the limit with no dead-letter path.
Rows with a still-valid claim are left alone so the holder can still ack.

**Ack** deletes the row, but only if the caller still holds a valid claim
(`worker_id` matches and `claim_expires_at >= now`). Ack after timeout
returns 0 — the job may already be running elsewhere. Deleting rather
than tombstoning is why claim speed depends on live work, not history.

**Retry** flips back to `pending` with `run_at = now + delay_s` if
attempts remain; otherwise moves the row to `_honker_dead` with
`last_error`. Both branches return 1; an invalid claim returns 0.

**Enqueue scheduling precedence:** `delay` beats explicit `run_at`, which
beats "now." `expires` is relative seconds from now; NULL means never.

**Enqueue writes no notification row.** The `_honker_live` INSERT already
advances `data_version` on commit, which is the wake path (§5). Writing a
synthetic wake row per enqueue grows an unbounded table on high-throughput
queues. Same for `stream_publish`.

**`queue_next_claim_at`** returns the earliest future moment at which a
claim could newly succeed — a pending row's `run_at`, or one second past a
processing row's `claim_expires_at`. It is how a sleeping worker knows how
long it may sleep. Ignore attempt-exhausted rows. Return 0 when there is
no such deadline.

**Rate limit** is a fixed window: `window_start = (now / per) * per`,
`INSERT ... ON CONFLICT DO UPDATE SET count = count + 1 WHERE count <
limit`. Returns whether the increment landed. Fixed windows allow up to
2× `limit` across a boundary; that is an accepted trade for being one
statement with no background state.

**Locks** are TTL leases. Acquire deletes an expired row, then
`INSERT OR IGNORE`, then checks who owns it — so re-acquiring your own
lock does *not* refresh the TTL. Long holders must call renew explicitly.
This is deliberate: silent TTL extension makes a hung holder
indistinguishable from a live one.

**Scheduler** expressions support three forms: 5-field cron
(`m h dom mon dow`), 6-field cron (with leading seconds), and
`@every <n><unit>` (`@every 5s`, `@every 1h`). Cron arithmetic runs in the
**system local timezone**, matching standard cron; interval expressions
are deterministic second steps. Get DST right — a schedule at 2:30am must
behave sanely on both the spring-forward and fall-back days, and this
deserves tests against a fixed timezone rather than the runner's.

`scheduler_tick(now)` fires every enabled task whose `next_fire_at <=
now`, advancing boundary by boundary so an outage backfills. Cap the
catch-up per task per tick (64 is a reasonable number) and skip forward
to the next boundary after `now` when the cap is hit, so a scheduler that
was down for a week doesn't enqueue ten thousand jobs. Users who don't
want backfill at all set `expires_s` so stale fires get swept.

At most one scheduler may fire, enforced by a named lock as leader
election with a heartbeat during long sleeps. A leader that loses its
lease must stop firing before it can do so again.

**Registering or updating a schedule must wake a sleeping leader** — the
write itself advances `data_version`, so this comes free, but a leader
that computed a one-hour sleep before a one-minute task was registered
must observe the change and recompute.

---

## 5. The wake mechanism

SQLite has no server-side push channel. This is the central engineering
problem, and how you solve it defines the product.

The required behavior:

- A committed write to the database file wakes listeners in other
  processes within a few milliseconds
- A rolled-back write wakes nobody
- After every wake, subscribers re-read state with indexed `SELECT`s —
  the wake carries no payload and is never trusted as data
- One watcher per database file, fanning out to N subscribers
- Idle subscribers issue no queries

The known-good mechanism is **`PRAGMA data_version`**: a per-connection
counter that changes when *another* connection commits to the file.
Reading it is a single-digit-microsecond operation with no I/O, so a
1 ms poll loop is cheap and gives push-like latency. Crucially, it does
not increment on rollback — I3 and the transactional guarantee both fall
out of that for free. It also survives WAL checkpoints and works in every
journal mode.

Make the polling interval configurable. 1 ms is the right default for
latency; deployments that care more about idle CPU should be able to
raise it.

Alternative backends (kernel filesystem events, mmap reads of the WAL
shared-memory index) are legitimate optimizations but are **experimental**
until they prove equivalence against the polling path. If you offer them:

- An omitted backend, and the names `"polling"`/`"poll"`, select the
  default
- Unknown names are errors everywhere
- An explicit request for an unavailable experimental backend **fails
  loudly**. Never silently fall back to polling — a user who asked for
  kernel events and got polling has no way to know their latency
  assumptions are wrong.

Everything downstream — listen, queue claim loops, stream subscriptions,
scheduler sleep — should be built on the same wake primitive plus a
fallback poll interval as a backstop. The fallback exists so a dropped
wake degrades latency instead of hanging forever.

**Watcher lifecycle matters more than it looks.** Subscriber
registration must be race-free against a commit landing mid-subscribe,
dropped subscribers must be pruned so the watcher doesn't leak, and a
watcher thread that dies must fail waiting consumers with a clear error
rather than hanging them forever. These are the bugs that will eat your
time; budget for them.

---

## 6. Feature semantics

### Notify / listen (ephemeral pub/sub)

`notify(channel, payload)` inserts a row and returns its id. Listeners
track the last id they saw and, on each wake, select rows on their channel
with a higher id. Delivery is to *every* listener on the channel, and a
listener that starts later does not see earlier notifications — this is
`pg_notify` semantics, not a queue.

The notifications table grows. Pruning is an explicit user-callable
operation (by age and/or max rows kept), not a background timer. No magic
threads the user didn't ask for.

### Queues

Covered in §4. The binding-level surface should give at minimum:
transactional enqueue, an async iterator that yields claimed jobs and
wakes on commit, per-job `ack`/`retry`/`fail`/`heartbeat`, batch claim and
ack, cancel, job lookup, dead-letter inspection, and expired sweeping.

Optional result storage with a TTL, plus a way to await a job's result
from the enqueueing process, turns the queue into an RPC mechanism. Keep
it opt-in-shaped (on by default with a short TTL is reasonable) so
fire-and-forget users don't pay for it.

### Streams

Append-only log per topic with a monotonic global offset and per-consumer
saved offsets. A subscriber resumes from its saved offset, so restarting a
consumer replays exactly what it missed — the durable counterpart to
notify/listen. Offset saves must be monotonic (never move an offset
backwards). Save the offset *before* yielding an event, not after, or a
crash mid-handler replays it.

### Transactional outbox

A helper that pairs a business write with an enqueue in one transaction
and provides the drain worker. This is really a documented pattern over
the queue rather than new machinery, and it is the single most important
thing to make easy — it is the reason the project exists.

### Task decorators (per-binding sugar)

Optional, and worth it for ergonomics in languages where it's idiomatic:
a decorator that registers a function under a name, so calling it enqueues
a job carrying `{task, args, kwargs}` in a recognizable envelope, and a
worker resolves the name back to the function.

Two behaviors matter: a raw payload without the envelope must still be
dispatchable (or explicitly skipped, not crashed on), and a job naming a
task the worker doesn't have registered must go to dead-letter with a
diagnosable error — not retry forever. Renaming a task orphans in-flight
jobs; document it, don't try to solve it.

---

## 7. Architecture and bindings

The shape that works: **one implementation of the SQL and the watcher,
consumed three ways.**

1. A **core library** holding the schema DDL, every operation's SQL, cron
   parsing, and the watcher. Rust is a good fit — it compiles to a
   loadable extension, embeds in Python/Node/JVM/.NET native modules, and
   exposes a C ABI — but the requirement is only that one artifact can
   serve all three consumption paths below.
2. A **SQLite loadable extension** wrapping the core, so any client can
   `SELECT load_extension(...)` and get everything.
3. **Native bindings** that link the core directly rather than loading the
   extension, for languages where a native module is the natural
   distribution unit.

Bindings that go through the extension's C ABI and bindings that link
natively must be indistinguishable at the database level. That is
testable, and should be tested (§8).

### What a binding owes

A binding is thin. It should not reimplement operations in its own SQL —
if it needs behavior the core doesn't expose, add it to the core. What
each binding does own:

- Idiomatic types and error handling for its language
- JSON encode/decode at the edge
- Its runtime's async story — an async iterator, a channel, a `Flow`, a
  callback, whatever a user of that language expects
- Transaction integration, including working inside the host ORM's
  transaction rather than demanding its own connection
- Connection lifecycle and the watcher subscription

Bindings will not reach parity simultaneously, and that's fine — but the
gaps must be **written down and true**. A parity table naming what each
binding supports, what its wake path is, and what CI actually proves is
part of the deliverable, not documentation polish. "Notify yes, listen
no" is an honest state; a table that claims parity CI doesn't prove is
not.

### ORM and framework integration

Ship no plugins. Users load the extension on their existing ORM
connection, call bootstrap, and call the SQL functions inside the ORM's
transaction. That has to actually work — against SQLAlchemy, Django,
Drizzle, sqlx, ActiveRecord, Ecto, Hibernate, and friends — which mostly
means never assuming Honker owns the connection.

---

## 8. Testing

The correctness claims here are all concurrency claims, so unit tests are
necessary and nowhere near sufficient. What must be proven:

**Transactional atomicity.** Enqueue inside a transaction that rolls back
leaves no job and wakes no worker. Enqueue that commits does both.

**Cross-process wake.** A worker in a *separate OS process* wakes on a
commit from another process, measured. Assert a latency bound, and keep a
benchmark that reports the real distribution.

**Crash recovery.** Kill a worker holding a claim (`SIGKILL`, not a clean
shutdown). The job must be reclaimed after the visibility timeout, exactly
once, and must eventually reach dead-letter rather than looping.

**Cross-language interop.** A job enqueued by binding A is claimed and
acked by binding B, over the same file. At least one native↔extension pair
and one extension↔extension pair. Full N×N is not worth the CI time;
representative pairs are.

**Attempt-budget edges.** A job that dies on its final allowed attempt.
A job whose claim expires while the worker is still alive. Ack after
timeout. Concurrent claims from many workers with no double-delivery of a
live claim.

**Scheduler boundaries.** Missed boundaries during downtime; the catch-up
cap; DST transitions in a fixed non-UTC timezone; leader failover mid-fire.

**Resource bounds.** Subscriber churn does not leak file descriptors,
threads, or memory. A soak run that watches all three over time.

**Fault injection.** Disk full, database locked, corrupted payload,
watcher thread death. Each should produce a clear error, not a hang.

Structure the suite so the fast path (a few minutes) is what runs on every
change, and slow/soak/platform-gated tests are marked and run nightly. A
test suite people skip is worth nothing.

---

## 9. Packaging and release

Published packages must be usable without a Rust toolchain — precompiled
per-platform artifacts, verified by installing into a **clean throwaway
consumer project outside the repo** and running a smoke test. "It works in
the monorepo" has never been evidence that a package works.

CI should cover the core on Linux, macOS, and Windows, and each binding on
the platforms it claims. Keep the parity table (§7) honest about which of
those are actually proven.

Bindings version independently. The core's schema is the compatibility
surface, so schema changes need a migration path (§3) and a changelog
entry, and old databases must keep working.

---

## 10. Suggested build order

Not binding, but each step is independently demonstrable:

1. Schema + bootstrap + `notify()`, one language, one process.
2. The watcher: `data_version` polling, shared across subscribers, with
   subscribe/unsubscribe races handled. Prove cross-process wake with two
   processes and a stopwatch. **This is the riskiest step — do it early.**
3. Queue: enqueue, claim, ack, retry, dead-letter, visibility timeouts.
   Prove crash recovery with a real `SIGKILL`.
4. Transactional enqueue and the outbox helper — the headline feature.
5. Streams with per-consumer offsets.
6. Locks, rate limits, results.
7. Cron parsing and the scheduler, with leader election.
8. Extract everything into the loadable extension. Prove a raw `sqlite3`
   shell session can drive the whole feature set.
9. Second binding, in a different language, through a different path
   (extension C ABI vs native link). Prove interop. Bugs in the
   abstraction boundary surface here, so don't defer it to the end.
10. Additional bindings, packaging proofs, soak tests.

---

## 11. Decisions left open

Deliberately unspecified — make the call, write down why:

- Core implementation language, module layout, and error taxonomy
- Whether bindings link the core natively or load the extension (both are
  valid; the parity table records which)
- Concurrency model inside each binding (threads, async runtime, actors)
- Whether to ship experimental watcher backends at all
- Batch sizes, default timeouts, TTLs, catch-up caps — the numbers in this
  spec are reasonable defaults, not requirements
- CLI surface, if any
- How much task-decorator sugar each binding grows

## 12. Where it goes wrong

Failure modes worth naming up front, because each has a tempting wrong
answer:

- **Polling the queue table "just as a fallback"** until it becomes the
  real path and idle CPU goes to the moon. Fallback polls should be tens
  of seconds, not tens of milliseconds.
- **Trusting the wake signal as data.** The wake says "something
  committed," never "your job is ready." Always re-read.
- **Refreshing a lock TTL on re-acquire**, which makes a hung holder look
  alive forever.
- **A synthetic notification row per enqueue**, which grows without bound
  under load.
- **Silent fallback** when an explicitly requested backend is
  unavailable — the user's latency assumptions break with no signal.
- **Ack that doesn't check claim validity**, which deletes a job another
  worker is currently running.
- **Unbounded scheduler catch-up**, which turns a scheduler outage into a
  thundering herd on recovery.
- **Bindings drifting their own SQL**, which breaks interop in ways that
  only show up cross-language and are miserable to debug.
