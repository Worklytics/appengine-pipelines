package com.google.appengine.tools.mapreduce.impl.shardedjob;

import com.google.appengine.api.taskqueue.dev.QueueStateInfo.TaskStateInfo;
import com.google.appengine.tools.mapreduce.EndToEndTestCase;
import com.google.appengine.tools.mapreduce.PipelineSetupExtensions;
import com.google.appengine.tools.pipeline.impl.backend.AppEngineServicesService;
import com.google.appengine.tools.pipeline.impl.backend.AppEngineTaskQueue;
import com.google.appengine.tools.txn.PipelineBackendTransaction;
import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.DatastoreException;
import com.google.cloud.datastore.FullEntity;
import com.google.cloud.datastore.IncompleteKey;
import com.google.cloud.datastore.Key;
import com.google.cloud.datastore.Transaction;
import com.google.apphosting.api.ApiProxy;
import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiPredicate;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Regression tests for slice locks left held when a transaction fails (or reports failure) around a slice run.
 */
@PipelineSetupExtensions
public class LockRecoveryTest extends EndToEndTestCase {

  private static final String QUEUE_NAME = "default";

  enum FailureMode {
    /** commit is not applied, and fails with a retryable error (eg, contention) */
    FAIL_WITHOUT_COMMIT,
    /** commit is applied, but the client still gets a retryable error (eg, response lost) */
    FAIL_AFTER_COMMIT,
  }

  /**
   * fails, once, the commit of a transaction that writes a task state matching {@code writes}
   */
  record CommitFault(Predicate<FullEntity<?>> writes, FailureMode mode, AtomicBoolean fired) {
    CommitFault(Predicate<FullEntity<?>> writes, FailureMode mode) {
      this(writes, mode, new AtomicBoolean(false));
    }
  }

  /**
   * aborts, once, a transaction when it reads a key matching {@code reads} (given the keys it read before); as with a
   * real abort, that transaction then fails every later read, write and commit
   */
  record ReadFault(BiPredicate<List<Key>, Key> reads, AtomicBoolean fired) {
    ReadFault(BiPredicate<List<Key>, Key> reads) {
      this(reads, new AtomicBoolean(false));
    }
  }

  private static final Set<String> CALLS_ALLOWED_AFTER_ABORT = Set.of("rollback", "isActive", "getTransactionId",
    "getDatastore", "equals", "hashCode", "toString");

  private final AtomicReference<CommitFault> fault = new AtomicReference<>();

  private final AtomicReference<ReadFault> readFault = new AtomicReference<>();

  // runner whose datastore transactions fail as set in fault
  private ShardedJobRunner runner;

  @Test
  public void postRunUpdateRetryReleasesLock() throws Exception {
    ShardedJobRunId jobId = startJob();
    TaskStateInfo workerTask = grabNextTaskFromQueue(QUEUE_NAME);
    IncrementalTaskId taskId = getTaskId(workerTask);

    // post-run update is the write that unlocks the task state
    CommitFault commitFault = new CommitFault(entity -> isTaskState(entity) && !isLocked(entity), FailureMode.FAIL_WITHOUT_COMMIT);
    fault.set(commitFault);

    runner.runTask(jobId, taskId, 0, "test-operation");

    assertTrue(commitFault.fired().get(), "fault was not injected");
    assertSliceCompletedOnce(taskId);
  }

  @Test
  public void lockCommitReportedAsFailedStillRunsSlice() throws Exception {
    ShardedJobRunId jobId = startJob();
    TaskStateInfo workerTask = grabNextTaskFromQueue(QUEUE_NAME);
    IncrementalTaskId taskId = getTaskId(workerTask);

    // lock acquisition is the write that locks the task state
    CommitFault commitFault = new CommitFault(entity -> isTaskState(entity) && isLocked(entity), FailureMode.FAIL_AFTER_COMMIT);
    fault.set(commitFault);

    runner.runTask(jobId, taskId, 0, "test-operation");

    assertTrue(commitFault.fired().get(), "fault was not injected");
    assertSliceCompletedOnce(taskId);
  }

  @Test
  public void postRunUpdateAbortedReadRetriesWithNewTransaction() throws Exception {
    ShardedJobRunId jobId = startJob();
    TaskStateInfo workerTask = grabNextTaskFromQueue(QUEUE_NAME);
    IncrementalTaskId taskId = getTaskId(workerTask);

    // post-run update reads the task state without reading the job state first (lock acquisition reads job state first)
    ReadFault fault = new ReadFault((readBefore, key) -> isTaskState(key) && readBefore.stream().noneMatch(LockRecoveryTest::isJobState));
    readFault.set(fault);

    // retrying on the aborted transaction would go on for ~30 min (SYMBOLIC_FOREVER attempts), so bound it
    ApiProxy.Environment environment = ApiProxy.getCurrentEnvironment();
    assertTimeoutPreemptively(Duration.ofSeconds(60), () -> {
      ApiProxy.setEnvironmentForCurrentThread(environment);
      runner.runTask(jobId, taskId, 0, "test-operation");
    }, "post-run update kept retrying on the aborted transaction");

    assertTrue(fault.fired().get(), "fault was not injected");
    assertSliceCompletedOnce(taskId);
  }

  private ShardedJobRunId startJob() {
    ShardedJobRunId jobId = shardedJobId("job1");
    // two slices, so after the first one the shard is still active and next worker task is scheduled
    TestTask task = new TestTask(0, 1, 1, 2);
    getPipelineOrchestrator().startJob(jobId, ImmutableList.of(task),
      new TestController(getDatastore().getOptions(), 1, getPipelineService(), false),
      ShardedJobSettings.builder().build());
    return jobId;
  }

  private void assertSliceCompletedOnce(IncrementalTaskId taskId) {
    IncrementalTaskState<TestTask> taskState = lookupTaskState(taskId);
    assertEquals(1, taskState.getSequenceNumber(), "slice result not written");
    assertFalse(taskState.getLockInfo().isLocked(), "lock left held");
    assertEquals(0, taskState.getRetryCount());
    assertEquals(1, taskState.getTask().getResult(), "slice should have run exactly once");

    List<String> scheduledSequenceNumbers = getTasks(QUEUE_NAME).stream()
      .map(t -> decodeParameter(t.getBody(), ShardedJobHandler.SEQUENCE_NUMBER_PARAM))
      .collect(Collectors.toList());
    assertEquals(List.of("1"), scheduledSequenceNumbers, "expected only the worker task for the next slice");
  }

  private IncrementalTaskState<TestTask> lookupTaskState(IncrementalTaskId taskId) {
    PipelineBackendTransaction tx = PipelineBackendTransaction.newInstance(getDatastore());
    try {
      return runner.lookupTaskState(tx, taskId);
    } finally {
      tx.rollbackIfActive();
    }
  }

  @BeforeEach
  public void setUpRunner() {
    AppEngineServicesService appEngineServicesService = new AppEngineServicesService() {
      @Override
      public String getLocation() {
        return "us-central1";
      }

      @Override
      public String getDefaultService() {
        return "default";
      }

      @Override
      public String getDefaultVersion(String service) {
        return "1";
      }

      @Override
      public String getWorkerServiceHostName(String service, String version) {
        return "1.default.localhost";
      }
    };
    runner = new ShardedJobRunner(this::getPipelineService, faultInjecting(getDatastore()),
      appEngineServicesService, new AppEngineTaskQueue(appEngineServicesService));
  }

  private Datastore faultInjecting(Datastore delegate) {
    return (Datastore) Proxy.newProxyInstance(Datastore.class.getClassLoader(), new Class<?>[]{Datastore.class},
      (proxy, method, args) -> {
        Object result = invoke(delegate, method, args);
        return result instanceof Transaction transaction ? faultInjecting(transaction) : result;
      });
  }

  private Transaction faultInjecting(Transaction delegate) {
    List<FullEntity<?>> written = new ArrayList<>();
    List<Key> read = new ArrayList<>();
    AtomicBoolean aborted = new AtomicBoolean(false);
    return (Transaction) Proxy.newProxyInstance(Transaction.class.getClassLoader(), new Class<?>[]{Transaction.class},
      (proxy, method, args) -> {
        if (aborted.get() && !CALLS_ALLOWED_AFTER_ABORT.contains(method.getName())) {
          throw abortedException();
        }
        switch (method.getName()) {
          case "get", "fetch" -> {
            List<Key> keys = args[0] instanceof Key[] array ? Arrays.asList(array) : List.of((Key) args[0]);
            ReadFault fault = readFault.get();
            if (fault != null && keys.stream().anyMatch(key -> fault.reads().test(read, key))
              && fault.fired().compareAndSet(false, true)) {
              aborted.set(true);
              throw abortedException();
            }
            read.addAll(keys);
          }
          case "put", "add" -> {
            if (args[0] instanceof FullEntity<?>[] entities) {
              written.addAll(Arrays.asList(entities));
            } else {
              written.add((FullEntity<?>) args[0]);
            }
          }
          case "commit" -> {
            CommitFault commitFault = fault.get();
            if (commitFault != null && written.stream().anyMatch(commitFault.writes())
              && commitFault.fired().compareAndSet(false, true)) {
              if (commitFault.mode() == FailureMode.FAIL_AFTER_COMMIT) {
                invoke(delegate, method, args);
                throw new DatastoreException(14, "UNAVAILABLE: injected after commit", "UNAVAILABLE", true, null);
              }
              throw abortedException();
            }
          }
          default -> { }
        }
        return invoke(delegate, method, args);
      });
  }

  private static Object invoke(Object target, Method method, Object[] args) throws Throwable {
    try {
      return method.invoke(target, args);
    } catch (InvocationTargetException e) {
      throw e.getCause();
    }
  }

  private static DatastoreException abortedException() {
    return new DatastoreException(10, "ABORTED: injected contention", "ABORTED", true, null);
  }

  private static boolean isTaskState(FullEntity<?> entity) {
    return isTaskState(entity.getKey());
  }

  private static boolean isTaskState(IncompleteKey key) {
    return key != null && IncrementalTaskState.DATASTORE_KIND.equals(key.getKind());
  }

  private static boolean isJobState(IncompleteKey key) {
    return ShardedJobStateImpl.DATASTORE_KIND.equals(key.getKind());
  }

  private static boolean isLocked(FullEntity<?> entity) {
    return entity.contains("sliceStartTime");
  }

  private static String decodeParameter(String body, String name) {
    return Arrays.stream(body.split("&"))
      .map(param -> param.split("=", 2))
      .filter(pair -> pair[0].equals(name))
      .map(pair -> URLDecoder.decode(pair[1], StandardCharsets.UTF_8))
      .findFirst()
      .orElse(null);
  }
}
