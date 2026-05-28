# Fix: DatastoreOptions.toBuilder() drops host in google-cloud-datastore 2.40.0+

## Root Cause

`DatastoreOptions` in `google-cloud-datastore` 2.40.0 (shipped in Google Cloud Java BOM 26.83.0)
introduced a **duplicate `host` field** inside `DatastoreOptions.Builder` that is separate from the
inherited `ServiceOptions.Builder.host` field.

The `DatastoreOptions$Builder` setHost() correctly writes to **both** fields:

```java
// Builder.setHost() bytecode:
putfield host              // DatastoreOptions.Builder-own host (#69)
invokespecial ServiceOptions$Builder.setHost()   // parent field
```

But the copy constructor used by `toBuilder()` only copies the DatastoreOptions-specific state
(namespace, databaseId, openTelemetryOptions, channelProvider) and **does NOT transfer the local
`host` field**:

```java
// Builder copy-constructor references: access$000 (namespace), access$100 (databaseId),
// access$200 (openTelemetryOptions), access$300 (channelProvider) — no host access method.
```

Additionally, `Builder.build()` checks the local `host` field first:

```java
// build() bytecode:
getfield host              // local field #69
ifnonnull → use it
// else: fall back to GrpcTransportOptions.getDefaultEndpoint() → real Cloud Datastore
```

**Result:** When `DATASTORE_EMULATOR_HOST` is set, `getDefaultInstance()` correctly configures the
emulator host. Calling `.toBuilder()` on that instance silently loses the host. The rebuilt options
then fall back to the real Cloud Datastore gRPC endpoint — so any entity written via the emulator
cannot be found by code that went through `toBuilder()`.

This is exactly why pipeline slot lookups fail: the slot is written (job submission path) using
correct emulator `DatastoreOptions`, but the task-handler read path rebuilds options via
`getDefaultInstance().toBuilder()`, loses the host, and queries the real Cloud Datastore instead.

---

## Pattern of the Fix

Replace every `DatastoreOptions.getDefaultInstance().toBuilder()` (and any `.toBuilder()` call on
an existing `DatastoreOptions`) with an explicit `DatastoreOptions.newBuilder()` that copies each
field manually, including the host:

```java
// BEFORE (broken in 2.40.0+):
DatastoreOptions.Builder builder = DatastoreOptions.getDefaultInstance().toBuilder();

// AFTER:
DatastoreOptions defaultInstance = DatastoreOptions.getDefaultInstance();
DatastoreOptions.Builder builder = DatastoreOptions.newBuilder()
    .setProjectId(defaultInstance.getProjectId())
    .setCredentials(defaultInstance.getCredentials())
    .setTransportOptions(defaultInstance.getTransportOptions());
Optional.ofNullable(defaultInstance.getHost()).ifPresent(builder::setHost);
```

For calls where the source is an existing `DatastoreOptions` instance (not `getDefaultInstance()`):

```java
// BEFORE:
DatastoreOptions rebuilt = someOptions.toBuilder().build();

// AFTER:
DatastoreOptions rebuilt = DatastoreOptions.newBuilder()
    .setProjectId(someOptions.getProjectId())
    .setCredentials(someOptions.getCredentials())
    .setTransportOptions(someOptions.getTransportOptions())
    .setHost(someOptions.getHost())  // must always set explicitly
    .build();
```

---

## Files to Fix

### 1. `java/src/main/java/com/google/appengine/tools/mapreduce/impl/util/RequestUtils.java` — **MOST CRITICAL**

Lines 70–72. This is the task-handler entry point: every pipeline task call rebuilds `DatastoreOptions`
here. Losing the host means the task handler hits the real Cloud Datastore instead of the emulator.

```java
// BEFORE (lines 70-72):
DatastoreOptions defaultInstance = DatastoreOptions.getDefaultInstance();
DatastoreOptions.Builder builder = defaultInstance.toBuilder();

// AFTER:
DatastoreOptions defaultInstance = DatastoreOptions.getDefaultInstance();
DatastoreOptions.Builder builder = DatastoreOptions.newBuilder()
    .setProjectId(defaultInstance.getProjectId())
    .setCredentials(defaultInstance.getCredentials())
    .setTransportOptions(defaultInstance.getTransportOptions());
Optional.ofNullable(defaultInstance.getHost()).ifPresent(builder::setHost);
```

Also add `import java.util.Optional;` if not already present (it already is in this file).

---

### 2. `java/src/main/java/com/google/appengine/tools/mapreduce/ShardedJobAbstractSettings.java`

Line 53. `getDatastoreOptions()` already overrides host with `getDatastoreHost()` afterwards, so
this only manifests when `getDatastoreHost()` returns null — but the base copy is still wrong.

```java
// BEFORE (line 53):
DatastoreOptions.Builder optionsBuilder = DatastoreOptions.getDefaultInstance().toBuilder();

// AFTER:
DatastoreOptions defaultInstance = DatastoreOptions.getDefaultInstance();
DatastoreOptions.Builder optionsBuilder = DatastoreOptions.newBuilder()
    .setProjectId(defaultInstance.getProjectId())
    .setCredentials(defaultInstance.getCredentials())
    .setTransportOptions(defaultInstance.getTransportOptions());
Optional.ofNullable(defaultInstance.getHost()).ifPresent(optionsBuilder::setHost);
```

---

### 3. `java/src/main/java/com/google/appengine/tools/mapreduce/MapReduceJob.java`

Four locations, all the same pattern. Lines 244, 348, and 483 are identical `getDatastore()` methods
in different inner classes; line 610 is `getDatastoreOptions()`.

**Lines 244, 348, 483** (identical pattern in three inner classes):

```java
// BEFORE:
datastore = DatastoreOptions.getDefaultInstance().toBuilder()
    .setNamespace(settings.getNamespace())
    .build().getService();

// AFTER:
DatastoreOptions defaultInstance = DatastoreOptions.getDefaultInstance();
DatastoreOptions.Builder b = DatastoreOptions.newBuilder()
    .setProjectId(defaultInstance.getProjectId())
    .setCredentials(defaultInstance.getCredentials())
    .setTransportOptions(defaultInstance.getTransportOptions());
Optional.ofNullable(defaultInstance.getHost()).ifPresent(b::setHost);
Optional.ofNullable(settings.getNamespace()).ifPresent(b::setNamespace);
datastore = b.build().getService();
```

**Line 610** (`getDatastoreOptions(MapReduceSettings)`):

```java
// BEFORE:
DatastoreOptions.Builder builder = DatastoreOptions.getDefaultInstance().toBuilder();

// AFTER:
DatastoreOptions defaultInstance = DatastoreOptions.getDefaultInstance();
DatastoreOptions.Builder builder = DatastoreOptions.newBuilder()
    .setProjectId(defaultInstance.getProjectId())
    .setCredentials(defaultInstance.getCredentials())
    .setTransportOptions(defaultInstance.getTransportOptions());
Optional.ofNullable(defaultInstance.getHost()).ifPresent(builder::setHost);
```

---

### 4. `java/src/main/java/com/google/appengine/tools/mapreduce/impl/shardedjob/pipeline/FinalizeShardsInfos.java`

Line 34. The `datastoreOptions` field here is passed in from the caller. If the caller constructed
it correctly (using the explicit copy pattern), this is safe — but `.toBuilder().build()` is still a
no-op round-trip that can lose the host if the incoming options have it stored only in the
DatastoreOptions-level field.

```java
// BEFORE (line 34):
Datastore datastore = datastoreOptions.toBuilder().build().getService();

// AFTER:
Datastore datastore = DatastoreOptions.newBuilder()
    .setProjectId(datastoreOptions.getProjectId())
    .setCredentials(datastoreOptions.getCredentials())
    .setTransportOptions(datastoreOptions.getTransportOptions())
    .setHost(datastoreOptions.getHost())
    .build().getService();
```

Or, since this is a round-trip with no changes, just use the instance directly:

```java
Datastore datastore = datastoreOptions.getService();
```

---

### 5. `java/src/main/java/com/google/appengine/tools/mapreduce/impl/shardedjob/pipeline/DeleteShardedJob.java`

Line 24. Same concern as #4 — the `toBuilder().build()` round-trip is used only to pass a
"fresh copy" to `DeleteShardsInfos`. Since `DatastoreOptions` is already immutable and
`DatastoreOptions` is serializable, just pass it directly:

```java
// BEFORE (line 24):
return new DeleteShardsInfos(datastoreOptions.toBuilder().build(), getJobId(), start, end);

// AFTER:
return new DeleteShardsInfos(datastoreOptions, getJobId(), start, end);
```

---

## Suggested Helper (Optional)

To avoid repeating the copy pattern, consider adding a package-private utility:

```java
// e.g., in RequestUtils or a new DatastoreUtils class
static DatastoreOptions.Builder copyOf(DatastoreOptions source) {
    DatastoreOptions.Builder b = DatastoreOptions.newBuilder()
        .setProjectId(source.getProjectId())
        .setCredentials(source.getCredentials())
        .setTransportOptions(source.getTransportOptions());
    Optional.ofNullable(source.getHost()).ifPresent(b::setHost);
    Optional.ofNullable(source.getNamespace()).ifPresent(b::setNamespace);
    Optional.ofNullable(source.getDatabaseId()).ifPresent(b::setDatabaseId);
    return b;
}
```

Then each call site becomes:
```java
DatastoreOptions.Builder builder = copyOf(DatastoreOptions.getDefaultInstance());
```

---

## Context

This fix is required when consuming this library from a project using
**Google Cloud Java BOM ≥ 26.83.0** (which brings in `google-cloud-datastore` ≥ 2.40.0).
The library's own BOM (26.53.0 at time of writing) is not affected, but the host project's
BOM takes precedence at runtime.
