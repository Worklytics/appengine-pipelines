# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

Worklytics' fork of Google's App Engine Pipelines Framework — a Java library for orchestrating asynchronous workflows on Google App Engine. All source lives under `java/`; the root contains only docs and the GitHub workflow.

## Commands

All Maven commands must be run from `java/`:

```shell
# Run all tests
cd java && mvn test

# Run a single test class
cd java && mvn test -Dtest=PipelineTest

# Run a test by package pattern (as CI does)
cd java && mvn test -Dtest="com.google.appengine.tools.pipeline.**"

# Build JAR
cd java && mvn package

# Publish to GitHub Packages (requires ~/.m2/settings.xml with GitHub token)
cd java && mvn deploy
```

Tests require a Datastore emulator; `DatastoreExtension` starts it automatically. The env var `GOOGLE_CLOUD_PROJECT=test-project` is injected by `maven-surefire-plugin` in `pom.xml`. Failing tests are retried up to 3 times.

## Architecture

The library has three distinct roles mixed into one artifact:

- **Client** — `PipelineService` / `PipelineOrchestrator` / `PipelineRunner`: API surface for starting pipelines, polling status, submitting promised values, and cancelling jobs.
- **Runner** — `PipelineServlet` + `TaskHandler` + `AppEngineBackEnd`: servlet-based execution engine that processes task-queue callbacks to advance pipeline state.
- **Admin UI** — JSON handlers (`JsonTreeHandler`, `JsonListHandler`, `JsonClassFilterHandler`) that serve pipeline state to the bundled web console.

### Job model

Users define pipelines by subclassing `Job0`…`Job6` (or `Job<E>` directly) and implementing `run()`. Jobs are `Serializable`; instances are stored in Datastore and reconstituted on each execution attempt.

Inside `run()`, child jobs are scheduled via `futureCall(new ChildJob(), arg1, arg2, ...)`, which returns a `FutureValue<T>`. Passing a `FutureValue` as an argument to another `futureCall` declares a data dependency — the child won't run until its inputs are ready. `PromisedValue` is the mechanism for values provided by external agents; filled via `PipelineService.submitPromisedValue()`.

`JobRunId` is a tuple `(project, database, namespace, name)` encoded as a colon-delimited string. `:` was chosen over `/` to avoid URL-encoding issues in some clients.

### Persistence

`AppEngineBackEnd` is the only production backend. It stores pipeline state in Google Cloud Datastore as `pipeline-job`, `pipeline-slot`, `pipeline-barrier`, and related entity kinds. All entities include a TTL field set 90 days in the future; you must create matching [Datastore TTL policies](https://cloud.google.com/datastore/docs/ttl) per entity kind for automatic cleanup.

Datastore operations are retried with exponential backoff (up to 5 attempts) on `DatastoreException` or `IOException`.

### Task queues

Two implementations of `PipelineTaskQueue`:
- `CloudTasksTaskQueue` (default in production) — uses the Cloud Tasks v2 API. Caches queue location lookups.
- `AppEngineTaskQueue` (legacy, used in tests and when `USE_LEGACY_QUEUES` is set) — uses the old GAE SDK task queue API.

Pipeline state-management tasks (`HandleSlotFilled`, `FinalizeJob`, etc.) are always sent to the default queue; only `RunJob` tasks respect `JobSetting.OnQueue`.

### Dependency injection

The framework uses Dagger 2 with custom scopes:

- `JobRunServiceComponent` (`@Singleton`) — one per process; top-level component for the runner. Built from `AppEngineHostModule`.
- `StepExecutionComponent` (`@StepExecutionScoped`) — one per task-queue callback invocation. Created via `JobRunServiceComponent.stepExecutionComponent(StepExecutionModule)`. Provides `PipelineManager`, `PipelineService`, `PipelineRunner`, `ShardedJobRunner`.
- `MultiTenantComponent` / `TenantComponent` — used by the *client* side to obtain a `PipelineService` scoped to specific Datastore options (project/namespace/credentials).

Job classes that need injected dependencies are annotated `@Injectable(DaggerMyContainer.class)`; `PipelineManager` calls the container's `inject()` via reflection before invoking `run()`.

Servlets use `@AllArgsConstructor(onConstructor_ = @Inject)` so Dagger can construct them with dependencies — see `docs/servlet-di.md` for the rationale.

### Multi-tenancy

`AppEngineBackEnd.Options` carries `projectId`, `credentials`, and `DatastoreOptions` (which includes namespace). Pass tenant-specific options to `PipelineService.getInstance(options)` or via `JobSetting` to isolate pipeline data per tenant.

### MapReduce / ShardedJob

`MapReduceJob` and `MapJob` are `Job` subclasses that orchestrate sharded parallel work via `ShardedJobRunner`. Each shard runs as an `IncrementalTask`; state is stored as `IncrementalTaskState` in Datastore. `ShardedJobRunId` extends the pipeline `JobRunId` concept.

## Testing patterns

Test infrastructure is wired with JUnit 5 extensions:

- `DatastoreExtension` — starts a `LocalDatastoreHelper` emulator before all tests, resets it between each test.
- `PipelineComponentsExtension` — builds `AppEngineBackEnd` + `StepExecutionComponent` against the emulator.
- `@PipelineSetupExtensions` — meta-annotation that applies both extensions; the standard setup for pipeline integration tests.

Tests that extend `PipelineTest` get `pipelineService`, `pipelineManager`, and `appEngineBackend` injected. The `AppEngineTaskQueue` (not `CloudTasksTaskQueue`) is used in tests, backed by a `LocalTaskQueue`.

## Local development environment variables

| Variable | Effect |
|---|---|
| `USE_LEGACY_QUEUES` | Use GAE SDK task queues instead of Cloud Tasks API |
| `USE_LOCAL_SERVICE` | Treat all services as running on `localhost` as `default/v1` |
| `GAE_SERVICE_HOST_SUFFIX` | Override dynamic AppEngine service host lookup |
| `CLOUDTASKS_QUEUE_LOCATION` | Override dynamic Cloud Tasks queue location lookup |