# App Engine Pipelines

This project is an advanced, modernized implementation of Google App Engine Pipelines. Originally an execution framework to run complex, multi-step asynchronous algorithms on App Engine, this fork extends the concepts and capabilities to be compatible with newer Java versions, modern GCP Datastore API clients (Cloud Datastore / Firestore in Datastore mode), and Cloud Tasks.

## Project Architecture
- **Datastore Client**: Uses modern `google-cloud-datastore` APIs rather than the legacy `appengine-api-1.0-sdk`.
- **Task Queues**: Abstracts task enqueuing to work across `AppEngineTaskQueue` (legacy standard App Engine push queues) and `CloudTasksTaskQueue` (modern Google Cloud Tasks).
- **Settings Propagation**: Inherits pipeline settings such as retry counts, worker services, worker versions, and custom Datastore boundaries (Namespace and Database ID) down to sub-jobs and async worker callbacks.

## Development Rules
- When modifying datastore interactions, always ensure you respect potential overrides for `databaseId` and `namespace`, which are typically configured at the `JobSetting` level.
- When passing parameters to new Pipeline Tasks, leverage `PipelineTask.toProperties()` and augment `QueueSettings` if those parameters must be inherited across jobs.
- Validate Cloud Datastore constraints carefully (e.g., namespace regex restrictions).
