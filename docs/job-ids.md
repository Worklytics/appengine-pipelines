
## Data Model

`JobId`
`ShardedJobId` - extends `JobId`?? a pipeline job that is split (sharded) so that can be
executed in parallel, as `IncrementalTask`s (not an explicit datastore entity; state of each
represented as `IncrementalTaskState`)

`IncrementalTaskId`?



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

## Design decisions

 - don't utilize parent/entity groups for now; each job/subjob at top-level
 - 