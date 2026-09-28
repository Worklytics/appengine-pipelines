## Change Log

### 0.3+worklytics.14
 - Fix: `DatastoreOptions.toBuilder()` drops the emulator host in `google-cloud-datastore` ≥ 2.40.0
   (Google Cloud Java BOM ≥ 26.83.0). Replaced all `toBuilder()` / `getDefaultInstance().toBuilder()`
   calls with explicit `DatastoreOptions.newBuilder()` copies that preserve the `host` field.
   Affected: `RequestUtils`, `ShardedJobAbstractSettings`, `MapReduceJob` (Sort/Merge/Reduce stages
   and `getDatastoreOptions`), `FinalizeShardsInfos`, `DeleteShardedJob`.

### 0.3+worklytics.7 (March 2025)
 - Handle cross-services transactionality
 - Fixes: serialization

### 0.3+worklytics.2
 - all job/promise handles format has changed; should not be used with any existing jobs
 - dependency on GAE SDK's Datastore client removed
 - you can now configure Datastore to be used to back pipeline, by specifying a project/service account

Planned:
 - remove GAE Tasks client dependency
 - remove rest of GAE SDK dependencies (module service, SystemProperty, etc)

### v0.10
  - migrates to java8, guava 20+
  - adds lombok, mockito
  
  
