
TL;DR; Datastore can no longer be magic singleton, bc complicates testing and various
settings (namespace for now, potentially databaseId, host, and even project) cannot be
shared across calls. SO this requires that we:

1. modify all interfaces to take either `Datastore` client, `DatastoreOptions`, or some abstract 
   `Options` class that incorporates the configuration information contained in those
2. make all classes dependent on `Datastore` not singletons; and take it as a constructor parameter

Current choice is (2), because limits scope of changes for users; will just have to change how they
get `PipelineService`/`PipelineManager` instances for launching jobs; and initially, for case of 
running everything within default project/database/namespace, it can remain a singleton for them.


## What is the right pattern long-term?

Passing options around allows classes to be self-evidently threadsafe; no risk that one thread
handling request A, meant to work against namespace `A`, uses a instance of `PipelineService` 
created with configuration pointing at namespace `B`.

Passing options around also makes override cases more clear - should we ever want/need to support
pipelines with subjobs running in different namespaces/databases/project (former being potentially
interesting in the short-term to support some types of migrations, although almost all migrations 
are orchestrated on per-tenant basis, so maybe there's not such a case).

## One database and namespace per pipeline

A pipeline's jobs, slots, and barriers are stored in a single Firestore database and a single
namespace. Pass `JobSetting.DatastoreDatabase` and `JobSetting.DatastoreNamespace` when starting
the pipeline, or start it on a `PipelineService` already bound to that partition. Null, empty, and
`(default)` mean the default database. Child jobs inherit the root partition. A child setting that
names a different database or namespace fails at runtime with `IllegalArgumentException`.

Reads and writes follow the entity key. The Cloud Datastore client sends commits and lookups to
`DatastoreOptions.getDatabaseId()`, and queries use the client namespace, so `AppEngineBackEnd`
opens a client for the key's database and namespace. The first save of a new pipeline uses that
client, not only the client injected into the backend.

Crossing partitions is a separate pipeline, not a child job. Create a promise in the parent
pipeline and pass its handle to the other pipeline. `submitPromisedValue` writes the slot named by
that handle, back into the parent's database and namespace.




