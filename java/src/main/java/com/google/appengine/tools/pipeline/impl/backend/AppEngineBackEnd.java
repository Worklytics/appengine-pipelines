// Copyright 2011 Google Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy of
// the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations under
// the License.

package com.google.appengine.tools.pipeline.impl.backend;

import com.github.rholder.retry.*;
import com.google.appengine.tools.mapreduce.RetryUtils;
import com.google.appengine.tools.pipeline.JobRunId;
import com.google.appengine.tools.pipeline.NoSuchObjectException;
import com.google.appengine.tools.pipeline.impl.QueueSettings;
import com.google.appengine.tools.pipeline.impl.model.*;
import com.google.appengine.tools.pipeline.impl.tasks.FanoutTask;
import com.google.appengine.tools.pipeline.impl.tasks.Task;
import com.google.appengine.tools.pipeline.impl.util.SerializationUtils;
import com.google.appengine.tools.pipeline.impl.util.TestUtils;
import com.google.appengine.tools.pipeline.util.Pair;
import com.google.auth.Credentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.datastore.*;
import com.google.common.base.Predicate;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.google.common.collect.Streams;
import com.google.datastore.v1.QueryResultBatch;
import lombok.Builder;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.java.Log;

import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import static com.google.appengine.tools.pipeline.impl.model.JobRecord.IS_ROOT_JOB_PROPERTY;
import static com.google.appengine.tools.pipeline.impl.model.JobRecord.ROOT_JOB_DISPLAY_NAME;
import static com.google.appengine.tools.pipeline.impl.model.PipelineModelObject.ROOT_JOB_KEY_PROPERTY;
import static com.google.appengine.tools.pipeline.impl.util.TestUtils.throwHereForTesting;

/**
 * @author rudominer@google.com (Mitch Rudominer)
 *
 */
@Log
@RequiredArgsConstructor
public class AppEngineBackEnd implements PipelineBackEnd, SerializationStrategy {

  public static final int MAX_RETRY_ATTEMPTS = 5;
  public static final int RETRY_BACKOFF_MULTIPLIER = 300;
  public static final int RETRY_MAX_BACKOFF_MS = 5000;

  // TODO: RetryUtils is in mapreduce package, so duplicated to not mix on purpose
  // TODO: consider moving to a shared package
  // TODO: possibly we should inspect error code in more detail? see https://cloud.google.com/datastore/docs/concepts/errors#Error_Codes
  @SuppressWarnings("DuplicatedCode")
  public static Predicate<Throwable> handleDatastoreExceptionRetry() {
    return t -> {
      Iterator<DatastoreException> datastoreExceptionIterator = Iterables.filter(Throwables.getCausalChain(t), DatastoreException.class).iterator();
      if (datastoreExceptionIterator.hasNext()) {
        DatastoreException de = datastoreExceptionIterator.next();
        return de.isRetryable() ||
          (de.getMessage() != null && de.getMessage().toLowerCase().contains("retry the transaction"));
      }
      return false;
    };
  }

  private <E> Retryer<E> withDefaults(RetryerBuilder<E> builder) {
      return builder
              .withWaitStrategy(
                  WaitStrategies.exponentialWait(RETRY_BACKOFF_MULTIPLIER, RETRY_MAX_BACKOFF_MS, TimeUnit.MILLISECONDS))
              .retryIfException(handleDatastoreExceptionRetry())
              .retryIfExceptionOfType(IOException.class) //q: can this happen?
              .withStopStrategy(StopStrategies.stopAfterAttempt(MAX_RETRY_ATTEMPTS))
              .withRetryListener(new RetryListener() {
                @Override
                public <V> void onRetry(Attempt<V> attempt) {
                  if (attempt.getAttemptNumber() > 1 || attempt.hasException()) {
                    String className = AppEngineBackEnd.class.getName();
                    if (attempt.hasException()) {
                      log.log(Level.WARNING, "%s, Attempt #%d. Retrying...".formatted(className, attempt.getAttemptNumber()), attempt.getExceptionCause());
                    } else {
                      log.log(Level.WARNING, "%s, Attempt #%d OK, wait: %s".formatted(className, attempt.getAttemptNumber(), Duration.ofMillis(attempt.getDelaySinceFirstAttempt())));
                    }
                  }
                }
              }
              )
              .build();

  }

  private static final Logger logger = Logger.getLogger(AppEngineBackEnd.class.getName());

  // @see https://cloud.google.com/datastore/docs/concepts/limits
  // actually, 1,048,572 bytes
  private static final int MAX_BLOB_BYTE_SIZE = 1_000_000;

  private final Datastore datastore;
  private final AppEngineTaskQueue taskQueue;

  public AppEngineBackEnd(AppEngineBackEnd.Options options) {
    this(options.getDatastoreOptions().getService(), new AppEngineTaskQueue());
  }


  @Builder
  @lombok.Value
  public static class Options implements PipelineBackEnd.Options {

    private String projectId;

    //q: good idea? risk here that we're copying / passing around sensitive info; although really
    // in prod ppl should depend on application-default credentials and I think this will be null
    private Credentials credentials;

    private DatastoreOptions datastoreOptions;

    @SneakyThrows
    public static AppEngineBackEnd.Options defaults() {
      return Options.builder()
        .datastoreOptions(DatastoreOptions.getDefaultInstance())
        .credentials(GoogleCredentials.getApplicationDefault())
        .projectId(DatastoreOptions.getDefaultProjectId())
        .build();
    }

  }

  @Override
  public PipelineBackEnd.Options getOptions() {
    return Options.builder()
      .datastoreOptions(datastore.getOptions())
      .projectId(datastore.getOptions().getProjectId())
      .credentials(datastore.getOptions().getCredentials())
      .build();
  }

  @Override
  public SerializationStrategy getSerializationStrategy() {
    return this;
  }

  private void putAll(DatastoreBatchWriter batchWriter, Collection<? extends PipelineModelObject> objects) {
    objects.stream()
      .map(PipelineModelObject::toEntity)
      //extra logging for debug
      //.peek(e -> logger.info("putting entity: " + e.getKey().toString()))
      .forEach(batchWriter::putWithDeferredIdAllocation);
  }



  // transactional save all
  private void saveAll(Transaction txn, UpdateSpec.Group group) {
    putAll(txn, group.getBarriers());
    putAll(txn, group.getJobs());
    putAll(txn, group.getSlots());
    putAll(txn, group.getJobInstanceRecords());
    putAll(txn, group.getFailureRecords());
  }

  /**
   * non-transactional save all
   * @param group
   * @throws DatastoreException if any datastore failure
   * @return generated keys, if any
   */
  private List<Key> saveAll(UpdateSpec.Group group) {
    // collect into batches of 500
    List<PipelineModelObject> toSave = Streams.concat(group.getBarriers().stream(),
      group.getJobs().stream(),
      group.getSlots().stream(),
      group.getJobInstanceRecords().stream(),
      group.getFailureRecords().stream()
    ).toList();

    List<Key> keys = new ArrayList<>(toSave.size());
    final int MAX_BATCH_SIZE = 500; // limit from Datastore API
    int batchIndex = 0;
    do {
      Batch batch = datastore.newBatch();
      int batchOffset = batchIndex * MAX_BATCH_SIZE;
      putAll(batch, toSave.subList(batchOffset, batchOffset + Math.min(MAX_BATCH_SIZE, toSave.size() - batchOffset)));
      keys.addAll(batch.submit().getGeneratedKeys());
    } while (++batchIndex * MAX_BATCH_SIZE < toSave.size());

    return keys;
  }

  private boolean transactionallySaveAll(UpdateSpec.Transaction transactionSpec,
      QueueSettings queueSettings, Key rootJobKey, Key jobKey, JobRecord.State... expectedStates) {
    Transaction transaction = datastore.newTransaction();
    try {
      if (jobKey != null && expectedStates != null) {
        Entity entity = null;
        try {
          entity = transaction.get(jobKey);
        } catch (DatastoreException e) {
          if (e.getCode() == 404) {
            throw new RuntimeException(
              "Fatal Pipeline corruption error. No JobRecord found with key = " + jobKey);
          } else {
            throw e;
          }
        }
        if (entity == null) {
          //don't believe new datastore lib throws exceptions here anymore
          throw new RuntimeException(
            "Fatal Pipeline corruption error. No JobRecord found with key = " + jobKey);
        }

        JobRecord jobRecord = new JobRecord(entity);
        JobRecord.State state = jobRecord.getState();
        boolean stateIsExpected = false;
        for (JobRecord.State expectedState : expectedStates) {
          if (state == expectedState) {
            stateIsExpected = true;
            break;
          }
        }
        if (!stateIsExpected) {
          logger.info("Job " + jobRecord + " is not in one of the expected states: "
              + Arrays.asList(expectedStates)
              + " and so transactionallySaveAll() will not continue.");
          return false;
        }
      }
      saveAll(transaction, transactionSpec);
      if (transactionSpec instanceof UpdateSpec.TransactionWithTasks) {
        UpdateSpec.TransactionWithTasks transactionWithTasks =
            (UpdateSpec.TransactionWithTasks) transactionSpec;
        Collection<Task> tasks = transactionWithTasks.getTasks();
        if (tasks.size() > 0) {
          byte[] encodedTasks = FanoutTask.encodeTasks(tasks);
          FanoutTaskRecord ftRecord = new FanoutTaskRecord(rootJobKey, encodedTasks);
          // Store FanoutTaskRecord outside of any transaction, but before
          // the FanoutTask is enqueued. If the put succeeds but the
          // enqueue fails then the FanoutTaskRecord is orphaned. But
          // the Pipeline is still consistent.
          datastore.put(ftRecord.toEntity());
          ftRecord.toEntity().getKey().getKind();
          FanoutTask fanoutTask = new FanoutTask(ftRecord.getKey(), queueSettings);

          //TODO: should this enqueue be in context of transaction??
          taskQueue.enqueue(fanoutTask);
        }
      }
      transaction.commit();
    } finally {
      if (transaction.isActive()) {
        transaction.rollback();
      }
    }
    return true;
  }

  @RequiredArgsConstructor
  private abstract class Operation<R> implements Callable<R> {

    @Getter
    private final String name;
  }

  private <R> R tryFiveTimes(final Operation<R> operation) {
    try {
      return withDefaults(RetryerBuilder.<R>newBuilder()).call(operation);
    } catch (ExecutionException e) {
      logger.log(Level.INFO, "Non-retryable exception during " + operation.getName(), e.getCause());
      throw new RuntimeException(e.getCause());
    } catch (RetryException e) {
      if (e.getCause() instanceof RuntimeException) {
        logger.info(e.getCause().getMessage() + " during " + operation.getName()
            + " throwing after multiple attempts ");
        throw (RuntimeException) e.getCause();
      } else {
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public void enqueue(Task task) {
    taskQueue.enqueue(task);
  }

  @Override
  public boolean saveWithJobStateCheck(final UpdateSpec updateSpec,
      final QueueSettings queueSettings, final Key jobKey,
      final JobRecord.State... expectedStates) {
    tryFiveTimes(new Operation<Void>("save") {
      @Override
      public Void call() {
        saveAll(updateSpec.getNonTransactionalGroup());
        return null;
      }
    });
    // TODO(user): Replace this with plug-able hooks that could be used by tests,
    // if needed could be restricted to package-scoped tests.
    // If a unit test requests us to do so, fail here.
    throwHereForTesting(TestUtils.BREAK_AppEngineBackEnd_saveWithJobStateCheck_beforeFinalTransaction);

    for (final UpdateSpec.Transaction transactionSpec : updateSpec.getTransactions()) {
      tryFiveTimes(new Operation<Void>("save") {
        @Override
        public Void call() {
          transactionallySaveAll(transactionSpec, queueSettings, updateSpec.getRootJobKey(), null);
          return null;
        }
      });
    }

    final AtomicBoolean wasSaved = new AtomicBoolean(true);
    tryFiveTimes(new Operation<Void>("save") {
      @Override
      public Void call() {
        wasSaved.set(transactionallySaveAll(updateSpec.getFinalTransaction(), queueSettings,
            updateSpec.getRootJobKey(), jobKey, expectedStates));
        return null;
      }
    });
    return wasSaved.get();
  }

  @Override
  public void save(UpdateSpec updateSpec, QueueSettings queueSettings) {
    saveWithJobStateCheck(updateSpec, queueSettings, null);
  }

  @Override
  public JobRecord queryJob(final Key jobKey, final JobRecord.InflationType inflationType)
      throws NoSuchObjectException {
    Entity entity = getEntity("queryJob", jobKey);
    JobRecord jobRecord = new JobRecord(entity);
    Barrier runBarrier = null;
    Barrier finalizeBarrier = null;
    Slot outputSlot = null;
    JobInstanceRecord jobInstanceRecord = null;
    ExceptionRecord failureRecord = null;
    switch (inflationType) {
      case FOR_RUN:
        runBarrier = queryBarrier(jobRecord.getRunBarrierKey(), true);
        finalizeBarrier = queryBarrier(jobRecord.getFinalizeBarrierKey(), false);
        jobInstanceRecord =
            new JobInstanceRecord(getEntity("queryJob", jobRecord.getJobInstanceKey()), getSerializationStrategy());
        outputSlot = querySlot(jobRecord.getOutputSlotKey(), false);
        break;
      case FOR_FINALIZE:
        finalizeBarrier = queryBarrier(jobRecord.getFinalizeBarrierKey(), true);
        outputSlot = querySlot(jobRecord.getOutputSlotKey(), false);
        break;
      case FOR_OUTPUT:
        outputSlot = querySlot(jobRecord.getOutputSlotKey(), false);
        Key failureKey = jobRecord.getExceptionKey();
        failureRecord = queryFailure(failureKey);
        break;
      default:
    }
    jobRecord.inflate(runBarrier, finalizeBarrier, outputSlot, jobInstanceRecord, failureRecord);
    logger.finest("Query returned: " + jobRecord);
    return jobRecord;
  }

  /**
   * {@code inflate = true} means that {@link Barrier#getWaitingOnInflated()}
   * will not return {@code null}.
   */
  private Barrier queryBarrier(Key barrierKey, boolean inflate) throws NoSuchObjectException {
    Entity entity = getEntity("queryBarrier", barrierKey);
    Barrier barrier = new Barrier(entity);
    if (inflate) {
      Collection<Barrier> barriers = new ArrayList<>(1);
      barriers.add(barrier);
      inflateBarriers(barriers);
    }
    logger.finest("Querying returned: " + barrier);
    return barrier;
  }

  /**
   * Given a {@link Collection} of {@link Barrier Barriers}, inflate each of the
   * {@link Barrier Barriers} so that {@link Barrier#getWaitingOnInflated()}
   * will not return null;
   *
   * @param barriers
   */
  private void inflateBarriers(Collection<Barrier> barriers) {
    // Step 1. Build the set of keys corresponding to the slots.
    Set<Key> keySet = new HashSet<>(barriers.size() * 5);
    for (Barrier barrier : barriers) {
      for (Key key : barrier.getWaitingOnKeys()) {
        keySet.add(key);
      }
    }
    // Step 2. Query the datastore for the Slot entities
    Map<Key, Entity> entityMap = getEntities("inflateBarriers", keySet);

    // Step 3. Convert into map from key to Slot
    Map<Key, Slot> slotMap = new HashMap<>(entityMap.size());
    for (Key key : keySet) {
      Slot s = new Slot(entityMap.get(key), this);
      slotMap.put(key, s);
    }
    // Step 4. Inflate each of the barriers
    for (Barrier barrier : barriers) {
      barrier.inflate(slotMap);
    }
  }

  @Override
  public Slot querySlot(Key slotKey, boolean inflate) throws NoSuchObjectException {
    Entity entity = getEntity("querySlot", slotKey);
    Slot slot = new Slot(entity, this);
    if (inflate) {
      Map<Key, Entity> entities = getEntities("querySlot", new HashSet<>(slot.getWaitingOnMeKeys()));
      Map<Key, Barrier> barriers = new HashMap<>(entities.size());
      for (Map.Entry<Key, Entity> entry : entities.entrySet()) {
        barriers.put(entry.getKey(), new Barrier(entry.getValue()));
      }
      slot.inflate(barriers);
      inflateBarriers(barriers.values());
    }
    return slot;
  }

  @Override
  public ExceptionRecord queryFailure(Key failureKey) throws NoSuchObjectException {
    if (failureKey == null) {
      return null;
    }
    Entity entity = getEntity("ReadExceptionRecord", failureKey);
    return new ExceptionRecord(entity);
  }

  //TODO: change return value to some sort of DatastoreValue type?
  @Override
  public Object serializeValue(PipelineModelObject model, Object value) throws IOException {
    byte[] bytes = SerializationUtils.serialize(value);
    if (bytes.length < MAX_BLOB_BYTE_SIZE) {
      // fits in a datastore blob, OK.
      return Blob.copyFrom(bytes);
    } else {
      // split it into multiple datastore entities, and store it that way
      int shardId = 0;
      int offset = 0;
      final List<Entity> shardedValues = new ArrayList<>(bytes.length / MAX_BLOB_BYTE_SIZE + 1);
      while (offset < bytes.length) {
        int limit = offset + MAX_BLOB_BYTE_SIZE;
        byte[] chunk = Arrays.copyOfRange(bytes, offset, Math.min(limit, bytes.length));
        offset = limit;
        shardedValues.add(new ShardedValue(model, shardId++, chunk).toEntity());
      }
      return tryFiveTimes(new Operation<List<Key>>("serializeValue") {
        @Override
        public List<Key> call() {
          Transaction tx = datastore.newTransaction();
          List<Key> keys = new ArrayList<>();
          try {
            for (Entity v : shardedValues) {
              tx.put(v);
              keys.add(v.getKey());
            }
            tx.commit();
          } finally {
            if (tx.isActive()) {
              tx.rollback();
            }
          }
          return keys;
        }
      });
    }
  }

  @Override
  public Object deserializeValue(PipelineModelObject model, Object serializedVersion)
    throws IOException, ClassNotFoundException {
    if (serializedVersion instanceof Blob) {
      return SerializationUtils.deserialize(((Blob) serializedVersion).toByteArray());
    } else {
      @SuppressWarnings("unchecked")
      List<Key> keys = (List<Key>) serializedVersion;
      Map<Key, Entity> entities = getEntities("deserializeValue", keys);
      ShardedValue[] shardedValues = new ShardedValue[entities.size()];
      int totalSize = 0;
      int index = 0;
      for (Key key : keys) {
        Entity entity = entities.get(key);
        ShardedValue shardedValue = new ShardedValue(entity);
        shardedValues[index++] = shardedValue;
        totalSize += shardedValue.getValue().length;
      }
      byte[] totalBytes = new byte[totalSize];
      int offset = 0;
      for (ShardedValue shardedValue : shardedValues) {
        byte[] shardBytes = shardedValue.getValue();
        System.arraycopy(shardBytes, 0, totalBytes, offset, shardBytes.length);
        offset += shardBytes.length;
      }
      return SerializationUtils.deserialize(totalBytes);
    }
  }

  private Map<Key, Entity> getEntities(String logString, final Collection<Key> keys) {
    Map<Key, Entity> result = tryFiveTimes(new Operation<>(logString) {
      @Override
      public Map<Key, Entity> call() {
        //NOTE: this read is strongly consistent now, bc backed by Firestore in Datastore-mode; this library was
        // designed thinking this read was only event
        return keys.stream()
          .parallel()
          .map(datastore::get)
          .filter(Objects::nonNull)
          .collect(Collectors.toMap(Entity::getKey, Function.identity()));
      }
    });
    if (keys.size() != result.size()) {
      List<Key> missing = new ArrayList<>(keys);
      missing.removeAll(result.keySet());
      throw new RuntimeException("Missing entities for keys: " + missing);
    }
    return result;
  }

  private Entity getEntity(String logString, final Key key) throws NoSuchObjectException {
    Entity entity = tryFiveTimes(new Operation<>("getEntity_" + logString) {
      @Override
      public Entity call() throws Exception {
        return datastore.get(key);
      }
    });

    if (entity == null) {
      throw new NoSuchObjectException(key.toString());
    }
    return entity;
  }

  @Override
  public void handleFanoutTask(FanoutTask fanoutTask) throws NoSuchObjectException {
    Key fanoutTaskRecordKey = fanoutTask.getRecordKey();
    // Fetch the fanoutTaskRecord outside of any transaction
    Entity entity = getEntity("handleFanoutTask", fanoutTaskRecordKey);
    FanoutTaskRecord ftRecord = new FanoutTaskRecord(entity);
    byte[] encodedBytes = ftRecord.getPayload();
    taskQueue.enqueue(FanoutTask.decodeTasks(encodedBytes));
  }

  public List<Entity> queryAll(final String kind, final Key rootJobKey) {
    return tryFiveTimes(new Operation<>("queryFullPipeline") {
      @Override
      public List<Entity> call() {
        EntityQuery.Builder query = Query.newEntityQueryBuilder()
          .setKind(kind)
          .setFilter(StructuredQuery.PropertyFilter.eq(ROOT_JOB_KEY_PROPERTY, rootJobKey));

        List<Entity> entities = new ArrayList<>();
        QueryResults<Entity> queryResults;
        long lastPageCount;
        do {
          //TODO: set chunkSize? does concept exist in this API client library?
          queryResults = datastore.run(query.build());
          List<Entity> page = Streams.stream(queryResults)
            .collect(Collectors.toList());
          lastPageCount = page.size();
          entities.addAll(page);
          query = query.setStartCursor(queryResults.getCursorAfter());
        } while (
          queryResults.getMoreResults() != QueryResultBatch.MoreResultsType.NO_MORE_RESULTS
          && lastPageCount > 0 // unclear why, but at least in tests prev check doesn't work as moreResults is always MORE_RESULTS_AFTER_LIMIT
        );

        return entities;
      }
    });
  }

  @Override
  public Pair<? extends Iterable<JobRecord>, String> queryRootPipelines(String classFilter,
      String cursor, final int limit) {
    EntityQuery.Builder query = Query.newEntityQueryBuilder()
      .setKind(JobRecord.DATA_STORE_KIND);

    if (Strings.isNullOrEmpty(classFilter)) {
      query.setFilter(StructuredQuery.PropertyFilter.eq(IS_ROOT_JOB_PROPERTY, true));
    } else {
      query.setFilter(StructuredQuery.PropertyFilter.eq(ROOT_JOB_DISPLAY_NAME, classFilter));
    }

    if (limit > 0) {
      query.setLimit(limit + 1);
    }
    if (cursor != null) {
      query.setStartCursor(Cursor.fromUrlSafe(cursor));
    }
    return tryFiveTimes(
        new Operation<>("queryRootPipelines") {
          @Override
          public Pair<? extends Iterable<JobRecord>, String> call() {
            QueryResults<Entity> entities = datastore.run(query.build());
            Cursor dsCursor = null;
            List<JobRecord> roots = new LinkedList<>();
            while (entities.hasNext()) {
              if (limit > 0 && roots.size() >= limit) {
                dsCursor = entities.getCursorAfter();
                break;
              }
              JobRecord jobRecord = new JobRecord(entities.next());
              roots.add(jobRecord);
            }
            return Pair.of(roots, dsCursor == null ? null : dsCursor.toUrlSafe());
          }
        });
  }

  @Override
  public Set<String> getRootPipelinesDisplayName() {

    return tryFiveTimes(new Operation<>("getRootPipelinesDisplayName") {
      @Override
      public Set<String> call() {
        ProjectionEntityQuery.Builder query = Query.newProjectionEntityQueryBuilder()
          .setKind(JobRecord.DATA_STORE_KIND)
          .addProjection(JobRecord.ROOT_JOB_DISPLAY_NAME)
          .addDistinctOn(JobRecord.ROOT_JOB_DISPLAY_NAME);

        QueryResults<ProjectionEntity> queryResults;
        Set<String> pipelines = new LinkedHashSet<>();
        List<String> page;
        do {
          //TODO: set chunkSize? does concept exist in this API client library?
          queryResults = datastore.run(query.build());
          page = Streams.stream(queryResults)
            .map(entity -> entity.getString(ROOT_JOB_DISPLAY_NAME))
            .collect(Collectors.toList());
          pipelines.addAll(page);
          query = query.setStartCursor(queryResults.getCursorAfter());
        } while (
            page.size() > 0 && // unclear why, but at least in tests prev check doesn't work as moreResults is always MORE_RESULTS_AFTER_LIMIT
            queryResults.getMoreResults() != QueryResultBatch.MoreResultsType.NO_MORE_RESULTS);

        return pipelines;
      }
    });
  }

  @Override
  public PipelineObjects queryFullPipeline(final Key rootJobKey) {
    final Map<Key, JobRecord> jobs = new HashMap<>();
    final Map<Key, Slot> slots = new HashMap<>();
    final Map<Key, Barrier> barriers = new HashMap<>();
    final Map<Key, JobInstanceRecord> jobInstanceRecords = new HashMap<>();
    final Map<Key, ExceptionRecord> failureRecords = new HashMap<>();

    //TODO: parallelize these all
    for (Entity entity : queryAll(Barrier.DATA_STORE_KIND, rootJobKey)) {
      barriers.put(entity.getKey(), new Barrier(entity));
    }
    for (Entity entity : queryAll(Slot.DATA_STORE_KIND, rootJobKey)) {
      slots.put(entity.getKey(), new Slot(entity, this, true));
    }
    for (Entity entity : queryAll(JobRecord.DATA_STORE_KIND, rootJobKey)) {
      jobs.put(entity.getKey(), new JobRecord(entity));
    }
    for (Entity entity : queryAll(JobInstanceRecord.DATA_STORE_KIND, rootJobKey)) {
      jobInstanceRecords.put(entity.getKey(), new JobInstanceRecord(entity, getSerializationStrategy()));
    }
    for (Entity entity : queryAll(ExceptionRecord.DATA_STORE_KIND, rootJobKey)) {
      failureRecords.put(entity.getKey(), new ExceptionRecord(entity));
    }
    return new PipelineObjects(
        rootJobKey, jobs, slots, barriers, jobInstanceRecords, failureRecords);
  }

  private void deleteAll(final String kind, final Key rootJobKey) {
    logger.info("Deleting all " + kind + " with rootJobKey=" + rootJobKey);
    tryFiveTimes(new Operation<Void>("delete") {
      @Override
      public Void call() {
        int batchesToAttempt = 5;
        int batchSize = 100;
        KeyQuery.Builder queryBuilder = Query.newKeyQueryBuilder()
          .setKind(kind)
          .setFilter(StructuredQuery.PropertyFilter.eq(ROOT_JOB_KEY_PROPERTY, rootJobKey))
          .setLimit(batchSize);

        QueryResults<Key> queryResults;
        List<Key> keys;

        do {
          Query query = queryBuilder.build();
          queryResults = datastore.run(query);
          keys = Streams.stream(queryResults)
            .collect(Collectors.toList());
          if (keys.size() > 0) {
            logger.info("Deleting " + keys.size() + " " + kind + "s with rootJobKey=" + rootJobKey);
            Batch batch = datastore.newBatch();
            keys.forEach(batch::delete);
            batch.submit();
          }
          queryBuilder = queryBuilder.setStartCursor(queryResults.getCursorAfter());
        } while (
          queryResults.getMoreResults() != QueryResultBatch.MoreResultsType.NO_MORE_RESULTS
          && keys.size() > 0  // unclear why, but in tests prev check doesn't work as moreResults is always MORE_RESULTS_AFTER_LIMIT
          && batchesToAttempt-- > 0 // avoid infinite loop
        );
        return null;
      }
    });
  }

  /**
   * Delete all datastore entities corresponding to the given pipeline.
   *
   * @param pipelineRunId The root job key identifying the pipeline
   * @param force         If this parameter is not {@code true} then this method will
   *                      throw an {@link IllegalStateException} if the specified pipeline is not in the
   *                      {@link JobRecord.State#FINALIZED} or
   *                      {@link JobRecord.State#STOPPED} state.
   * @throws IllegalStateException If {@code force = false} and the specified
   *                               pipeline is not in the
   *                               {@link com.google.appengine.tools.pipeline.impl.model.JobRecord.State#FINALIZED} or
   *                               {@link com.google.appengine.tools.pipeline.impl.model.JobRecord.State#STOPPED} state.
   */
  @Override
  public void deletePipeline(JobRunId pipelineRunId, boolean force)
      throws IllegalStateException {

    Key pipelineKey = JobRecord.keyFromPipelineHandle(pipelineRunId);

    if (!force) {
      try {
        JobRecord rootJobRecord = queryJob(pipelineKey, JobRecord.InflationType.NONE);
        switch (rootJobRecord.getState()) {
          case FINALIZED:
          case STOPPED:
            break;
          default:
            throw new IllegalStateException("Pipeline is still running: " + rootJobRecord);
        }
      } catch (NoSuchObjectException ex) {
        // Consider missing rootJobRecord as a non-active job and allow further delete
      }
    }

    deleteAll(JobRecord.DATA_STORE_KIND, pipelineKey);
    deleteAll(Slot.DATA_STORE_KIND, pipelineKey);
    deleteAll(ShardedValue.DATA_STORE_KIND, pipelineKey);
    deleteAll(Barrier.DATA_STORE_KIND, pipelineKey);
    deleteAll(JobInstanceRecord.DATA_STORE_KIND, pipelineKey);
    deleteAll(FanoutTaskRecord.DATA_STORE_KIND, pipelineKey);
  }
}
