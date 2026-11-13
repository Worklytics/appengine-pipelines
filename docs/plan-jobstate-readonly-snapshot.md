<!-- Assisted-by: Claude Code claude-opus-5-5 -->
# Plan: read ShardedJob state in a read-only snapshot (handoff)

Branch: `s235-jobstate-readonly-snapshot`, based on `main` after PR #101 (f67ddda). Library version 14.5.

## Problem
Production (eval-engin) logs many Datastore warnings:
`too much contention on these datastore entities ... entity group key: /MR-ShardedJob/<jobId>`.

Cause: in `ShardedJobRunner.runTask`, every slice's lock-acquisition transaction (read-write) reads the job-state
entity (`MR-ShardedJob/<jobId>`), which is shared by all shards. `completeShard` writes that same entity. So all
concurrent slices of a job contend with each other and with shard completions. The transactions retry and
succeed eventually, so the warnings are mostly noise. This is not a regression from 5fd2474; it predates it.

## Fix
Read the job state in a **read-only transaction**, outside the lock-acquisition read-write transaction. Read-only
transactions take no locks, so they don't contend with writers and aren't aborted by them.

- `txn/PipelineBackendTransaction.java`: adds the static `newReadOnlyInstance(datastore, taskQueue)`.
- `txn/PipelineBackendTransactionImpl.java`: adds a package-private constructor that takes `TransactionOptions`;
  the lazy `dsTransaction` creation uses those options when present.
- `mapreduce/impl/shardedjob/ShardedJobRunner.java`:
  - adds `lookupJobStateSnapshot(jobId)`. It uses a read-only txn because job state can span several entities
    (value shards), so a consistent snapshot is still needed.
  - in `runTask`, the job-state lookup and the "job is gone" check move before the lock-acquisition txn is created.
- Safety argument: only the job's status changes after creation. An abort that races with the snapshot read is
  picked up by the next slice, the same way it already is for a slice that's mid-run.

## Tests
`LockRecoveryTest`:
- New: `lockAcquisitionDoesNotReadJobStateInReadWriteTransaction`. The datastore proxy counts job-state reads in
  read-only vs read-write txns. This test fails on the old code and passes on the new code.
- Adjusted: the post-run-update fault predicate now targets "the 2nd txn to read task state", replacing "a task
  state read with no job state read before it". The lock-acquisition txn no longer reads job state, so the old
  predicate stopped matching.

Results so far:
- `LockRecoveryTest`: 4/4 pass.
- Full mapreduce + txn suite: 202 tests, 1 failure, `EndToEndTest.testSlicingJob`. It also fails on the base commit
  (GCS 416), so it's pre-existing.
  - `MapReduceServletTest.ignoredTestCommandError` passes. It only failed on a build that still had classes compiled
    from the Micronaut branch in `target/`. Run `mvn clean` after switching branches.

## Remaining steps
Done: rebased onto `main`, pre-existing failures confirmed, `<revision>` bumped to `0.3+worklytics.14.5`, PR opened.

1. After publishing, update `google.appengine.pipeline.version` in worklytics-alt `pom.xml` (currently
   `0.3+worklytics.14.4-20261008.194953-1`, line ~203) to the new 14.5 build. The user does this; don't change other repos.

## Related issues (not part of this change, not approved)
These were found during the same investigation. Each was offered to the user and not approved, so don't do them
without asking.
- **GDrive retries exhausted by OOM kills.** In worklytics-alt, `FetchGDriveItemsV2Job.getDefaultSettingsBuilder`
  sets `.maxSliceRetries(0)`.
  - `jobs` instances get OOM-killed (3072 MiB) 30–70 times a day. Each kill leaves an abandoned slice lock, which
    becomes a shard retry.
  - A shard retry restarts from `getInitialTask()`, and `ShardRetryState.retryCount` never resets.
  - Result: nubank MR `60793b52…_20261014_091943` failed when task-5 exhausted `maxShardRetries(6)`.
  - Proposed fix: set `maxSliceRetries` to about 3.
- **Theoretical issue in 5fd2474 (not seen in logs).** If the post-run commit is applied but reports failure, the
  retry skips the update and the next slice task is never enqueued.
