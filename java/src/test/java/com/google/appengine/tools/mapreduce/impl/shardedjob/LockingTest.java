package com.google.appengine.tools.mapreduce.impl.shardedjob;

import com.google.appengine.api.taskqueue.dev.QueueStateInfo.TaskStateInfo;
import com.google.appengine.tools.mapreduce.EndToEndTestCase;
import com.google.appengine.tools.mapreduce.PipelineSetupExtensions;
import com.google.appengine.tools.txn.PipelineBackendTransaction;
import com.google.apphosting.api.ApiProxy;
import com.google.apphosting.api.ApiProxy.Environment;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.SettableFuture;
import lombok.Getter;
import lombok.Setter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.UnsupportedEncodingException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.appengine.tools.mapreduce.impl.shardedjob.Status.StatusCode.DONE;
import static com.google.appengine.tools.mapreduce.impl.shardedjob.Status.StatusCode.RUNNING;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests that locking prevents concurrent execution.
 */
@PipelineSetupExtensions
public class LockingTest extends EndToEndTestCase {

  private final String queueName = "default";
  private ShardedJobSettings settings;

  @BeforeEach
  public void initSettings() throws Exception {
    settings = ShardedJobSettings.builder().build();
  }


  @Getter
  @Setter(onMethod_ = @BeforeEach)
  ShardedJobRunner shardedJobRunner;

  /**
   * This class relies on a static member to block and to count so that it works across
   * serialization. Therefore it is invalid to construct more than one of these at a time.
   */
  private static class StaticBlockingTask extends TestTask {

    static AtomicInteger timesRun = new AtomicInteger(0);
    static Semaphore runStarted = new Semaphore(0);
    static Semaphore finishRun = new Semaphore(0);

    public StaticBlockingTask(int result) {
      super(1, 1, result, 1);
    }

    @Override
    public void run() {
      super.run();
      timesRun.incrementAndGet();
      runStarted.release();
      finishRun.acquireUninterruptibly();
    }

    private static void resetStatus() {
      runStarted.release(Integer.MAX_VALUE);
      finishRun.release(Integer.MAX_VALUE);
      runStarted = new Semaphore(0);
      finishRun = new Semaphore(0);
      timesRun.set(0);
    }
  }

  @AfterEach
  public void cleanup() {
    StaticBlockingTask.resetStatus();
  }

  /**
   * Tests the case of a duplicate task from task queue after that task has finished.
   */
  @Test
  public void testLateTaskQueueDup() throws Exception {
    final ShardedJobRunId jobId = startNewTask(settings);

    final TaskStateInfo taskFromQueue = grabNextTaskFromQueue(queueName);

    //Run task
    SettableFuture<Void> result = runInNewThread(taskFromQueue);
    assertEquals(1, StaticBlockingTask.timesRun.get());
    StaticBlockingTask.finishRun.release();
    result.get();
    ShardedJobState state = getPipelineRunner().getJobState(jobId);
    assertEquals(new Status(RUNNING), state.getStatus());
    assertEquals(1, state.getActiveTaskCount());
    assertEquals(1, state.getTotalTaskCount());

    IncrementalTaskState<IncrementalTask> taskState = lookupTaskState(taskFromQueue);
    //Duplicate task again (after status change).
    executeTask(jobId.getJobId(), taskFromQueue); //Should do nothing.
    assertAreEqual(taskState, lookupTaskState(taskFromQueue));
    assertEquals(1, StaticBlockingTask.timesRun.get());

    //Finish execution of job.
    executeTasksUntilEmpty();
    state = getPipelineRunner().getJobState(jobId);
    assertEquals(new Status(DONE), state.getStatus());
    assertEquals(0, state.getActiveTaskCount());
    assertEquals(1, state.getTotalTaskCount());

    //Duplicate task again.
    StaticBlockingTask.resetStatus();
    executeTask(jobId.getJobId(), taskFromQueue); //Should do nothing.
    assertEquals(0, StaticBlockingTask.timesRun.get());
    assertDone(jobId);
  }


  private ShardedJobRunId startNewTask(ShardedJobSettings settings) {
    ShardedJobRunId jobId = shardedJobId("job1");
    assertNull(getPipelineRunner().getJobState(jobId));
    StaticBlockingTask task = new StaticBlockingTask(1);
    getPipelineOrchestrator().startJob(jobId, ImmutableList.<TestTask>of(task), new TestController(getDatastore().getOptions(), 1, getPipelineService(), false), settings);
    ShardedJobState state = getPipelineRunner().getJobState(jobId);
    assertEquals(new Status(RUNNING), state.getStatus());
    assertEquals(1, state.getActiveTaskCount());
    assertEquals(1, state.getTotalTaskCount());
    assertEquals(0, StaticBlockingTask.timesRun.get());
    return jobId;
  }

  private void assertDone(final ShardedJobRunId jobId) {
    ShardedJobState state = getPipelineRunner().getJobState(jobId);
    assertEquals(new Status(DONE), state.getStatus());
    assertEquals(0, state.getActiveTaskCount());
    assertEquals(1, state.getTotalTaskCount());
    assertTrue(getTasks(queueName).isEmpty());
  }

  /**
   * Tests a duplicate task from task queue while the execution of that task is in progress.
   */
  @Test
  public void testDupResultsInWaiting() throws Exception {
    final ShardedJobRunId jobId = startNewTask(settings);

    final TaskStateInfo taskFromQueue = grabNextTaskFromQueue(queueName);

    //Start task
    SettableFuture<Void> result = runInNewThread(taskFromQueue);
    assertEquals(1, StaticBlockingTask.timesRun.get());
    ShardedJobState state = getPipelineRunner().getJobState(jobId);
    assertEquals(new Status(RUNNING), state.getStatus());
    assertEquals(1, state.getActiveTaskCount());
    assertEquals(1, state.getTotalTaskCount());
    assertEquals( 0, getTasks(queueName).size(), "Something was left in the queue");

    //Duplicate task (first task is still running)
    executeTask(jobId.getJobId(), taskFromQueue); //Should not block because will not execute run.
    TaskStateInfo delayedRetry = grabNextTaskFromQueue(queueName);
    assertTrue(delayedRetry.getEtaDelta() > 0);

    //First task completes
    StaticBlockingTask.finishRun.release();
    result.get();

    //Finish execution of job.
    executeTasksUntilEmpty();
    state = getPipelineRunner().getJobState(jobId);
    assertEquals(new Status(DONE), state.getStatus());
    assertEquals(0, state.getActiveTaskCount());
    assertEquals(1, state.getTotalTaskCount());

    //Duplicate task again.
    StaticBlockingTask.resetStatus();
    executeTask(jobId.getJobId(), delayedRetry); //Should do nothing.
    assertEquals(0, StaticBlockingTask.timesRun.get());
    assertDone(jobId);
  }

  /**
   * Tests lock expiration
   */
  @Test
  public void testExpiryRestartsShard() throws Exception {
    //Setting the timeout to 0 insures that the shard will have timed out by the time the
    //duplicate arrives.
    ShardedJobSettings settings =
        ShardedJobSettings.builder().sliceTimeoutMillis(0).build();
    final ShardedJobRunId jobId = startNewTask(settings);

    //Run task
    final TaskStateInfo taskFromQueue = grabNextTaskFromQueue(queueName);
    PipelineBackendTransaction tx = PipelineBackendTransaction.newInstance(getDatastore());
    assertEquals(0, getShardRetryCount(tx, taskFromQueue));
    SettableFuture<Void> result = runInNewThread(taskFromQueue);
    assertEquals(1, StaticBlockingTask.timesRun.get());
    ShardedJobState state = getPipelineRunner().getJobState(jobId);
    assertEquals(new Status(RUNNING), state.getStatus());
    assertEquals(1, state.getActiveTaskCount());
    assertEquals(1, state.getTotalTaskCount());
    assertEquals(0, getTasks(queueName).size(), "Something was left in the queue");

    tx.commit();
    assertFalse(tx.isActive());
    tx = PipelineBackendTransaction.newInstance(getDatastore());
    assertEquals(0, getShardRetryCount(tx, taskFromQueue));

    //Duplicate task
    executeTask(jobId.getJobId(), taskFromQueue); //Should not block because will not execute run.

    tx.commit();
    assertFalse(tx.isActive());
    tx = PipelineBackendTransaction.newInstance(getDatastore());
    assertEquals(1, getShardRetryCount(tx, taskFromQueue));
    state = getPipelineRunner().getJobState(jobId);
    assertEquals(new Status(RUNNING), state.getStatus());
    assertEquals(1, state.getActiveTaskCount());
    assertEquals(1, state.getTotalTaskCount());
    assertEquals(1, StaticBlockingTask.timesRun.get());

    //First task completion should not update state
    IncrementalTaskState<IncrementalTask> taskState = lookupTaskState(taskFromQueue);
    StaticBlockingTask.finishRun.release();
    result.get();
    assertAreEqual(taskState, lookupTaskState(taskFromQueue));
    state = getPipelineRunner().getJobState(jobId);
    assertEquals(new Status(RUNNING), state.getStatus());

    //Run next task in queue (Which is a re-try of the shard)
    TaskStateInfo retry = grabNextTaskFromQueue(queueName);
    result = runInNewThread(retry);
    assertEquals(2, StaticBlockingTask.timesRun.get());
    state = getPipelineRunner().getJobState(jobId);
    assertEquals(new Status(RUNNING), state.getStatus());
    assertEquals(1, state.getActiveTaskCount());
    assertEquals(1, state.getTotalTaskCount());
    assertEquals(0, getTasks(queueName).size(), "Something was left in the queue");
    StaticBlockingTask.finishRun.release();
    result.get();

    //Finish by calling callback.
    executeTasksUntilEmpty();
    assertDone(jobId);
  }

  private void assertAreEqual(IncrementalTaskState<IncrementalTask> a,
      IncrementalTaskState<IncrementalTask> b) {
    assertEquals(a.getJobId(), b.getJobId());
    assertEquals(a.getSequenceNumber(), b.getSequenceNumber());
    assertEquals(a.getRetryCount(), b.getRetryCount());
    assertEquals(a.getTaskId(), b.getTaskId());
    assertEquals(a.getLockInfo().lockedSince(), b.getLockInfo().lockedSince());
    assertEquals(a.getMostRecentUpdateTime(), b.getMostRecentUpdateTime());
  }

  private SettableFuture<Void> runInNewThread(final TaskStateInfo taskFromQueue)
      throws InterruptedException {
    final Environment environment = ApiProxy.getCurrentEnvironment();
    final SettableFuture<Void> settableFuture = SettableFuture.create();
    Thread t = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          ApiProxy.setEnvironmentForCurrentThread(environment);
          executeTask(queueName, taskFromQueue);
          settableFuture.set(null);
        } catch (Exception e) {
          settableFuture.setException(e);
        }
      }
    }, "LockingTest tread");
    t.start();
    StaticBlockingTask.runStarted.acquire();
    return settableFuture;
  }


  @VisibleForTesting
  private int getShardRetryCount(PipelineBackendTransaction tx, final TaskStateInfo taskFromQueue)
      throws UnsupportedEncodingException {
    return getShardedJobRunner().lookupShardRetryState(tx, getTaskId(taskFromQueue)).getRetryCount();
  }

  @VisibleForTesting
  private IncrementalTaskState<IncrementalTask> lookupTaskState(final TaskStateInfo taskFromQueue)
      throws UnsupportedEncodingException {
    PipelineBackendTransaction pipelineBackendTransaction = PipelineBackendTransaction.newInstance(getDatastore());
    return getShardedJobRunner().lookupTaskState(pipelineBackendTransaction, getTaskId(taskFromQueue));
  }
}
