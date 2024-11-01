
## Data Model

`JobRunId` tuple of `(project, database id, namespace, name)`
`ShardedJobRunId` - extends `JobId`; a pipeline job that is split (sharded) so that can be
executed in parallel, as `IncrementalTask`s (not an explicit datastore entity; state of each
represented as `IncrementalTaskState`)

`IncrementalTaskId` - identifies a task that's executed incrementally, eg, a slice of a sharded job.

`SlotId` to identifiy slots; eg promises/futures - values to be filled async after job excecution.

Note, called `Run`, to be more analogous to  how other frameworks, namely Spring, refer to a job execution.

  - a `Job` is the logic - a java class implementation
  - a `JobRun` is the actually run of that job.
  - a `JobRun` may be attempted multiple times (eg, re-tried); we don't explicitly model attempts, but coule



## Components
How a datastore key for a job is constructed:

### project

### database id

### namespace

### name
 - `job id` UUID.
 - `sub-job id`-  if any; jobId on parent
 - `shard id` - if any; integer; uniquely identifies shared job run within a job/sub-job





### IncrementalTaskState

Sharded job gets split into tasks (shards); each of which has a number that identifies it.

`IncrementalTaskState` represent state of each shard, which may be retryed multiple times; as well 
as incrementally executed (eg, slice executed, which make some progress towards completion of task,
serializing that state back onto `IncrementalTaskState`).

### ShardedJobStateImpl

Aggregate state of sharded job; eg aggregate of `IncrementalTaskState`.



`JobInstanceRecord` v `JobRecord` - what's the distinction??

## Encoding to String
Needs:
   - reference in web apps
   - reference in logs
   - reference in other entities/systems

Considerations:
    - avoid coupling to datastore too much
    - human-readable, to ease debugging and reading logs/tests
    - avoid special characters that may need escaping, or not play nice with clients

Current approach is `:` as delimiter, and require none of those in string. `/` was being encoded in some contexts, 
and confusing JS libs.

## Design decisions

 - don't utilize parent/entity groups for now; each job/subjob at top-level
 - for transport, simple serialization with `:` delimiter; possibly base64-url-safe encoding would be better?1