package com.google.appengine.tools.mapreduce.impl.shardedjob;

import static org.junit.Assert.assertTrue;

import com.google.appengine.tools.mapreduce.impl.IncrementalTaskContext;
import com.google.appengine.tools.mapreduce.impl.IncrementalTaskWithContext;

/**
 * A simple intermediate tasks object to be used in unit tests.
 *
 */
public class TestTask implements IncrementalTaskWithContext {
  private static final long serialVersionUID = 1L;
  private final IncrementalTaskContext context;
  private final int valueToYield;
  private byte[] initialPayload;
  private int total = 0;
  private int slicesRemaining;

  public TestTask(int shardId, int shardCount, int valueToYield, int numSlices, byte... payload) {
    ShardedJobRunId jobId = ShardedJobRunId.of("test-project", null, null,"TestMR");
    this.context =
        new IncrementalTaskContext(jobId, shardId, shardCount, "testCalls", "testCallsMillis");
    this.valueToYield = valueToYield;
    slicesRemaining = numSlices;
    this.initialPayload = payload;
  }

  byte[] getPayload() {
    return initialPayload;
  }

  @Override
  public void prepare() {
  }

  @Override
  public void run() {
    assertTrue(slicesRemaining-- > 0);
    total += valueToYield;
    context.getCounters().getCounter("TestTaskSum").increment(valueToYield);
  }

  @Override
  public void cleanup() {
  }

  @Override
  public boolean isDone() {
    return slicesRemaining <= 0;
  }

  public Integer getResult() {
    return total;
  }

  @Override
  public IncrementalTaskContext getContext() {
    return context;
  }

  @Override
  public boolean allowSliceRetry(boolean abandon) {
    return false;
  }

  @Override
  public void jobCompleted(Status status) {
    initialPayload = null;
  }
}
