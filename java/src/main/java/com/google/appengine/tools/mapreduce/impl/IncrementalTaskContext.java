package com.google.appengine.tools.mapreduce.impl;

import com.google.appengine.tools.mapreduce.Counters;
import com.google.appengine.tools.mapreduce.impl.shardedjob.ShardedJobRunId;
import lombok.Getter;
import lombok.NonNull;

import java.io.Serial;
import java.io.Serializable;

/**
 * Context used by incremental tasks.
 */
@Getter
public class IncrementalTaskContext implements Serializable {

  @Serial
  private static final long serialVersionUID = 1L;

  @NonNull
  private final String workerCallsCounterName;
  @NonNull
  private final String workerMillisCounterName;

  private final ShardedJobRunId jobId;
  private final int shardNumber;
  private final int shardCount;
  private final Counters counters;

  private String lastWorkItem;

  public IncrementalTaskContext(@NonNull ShardedJobRunId jobId,
                                int shardNumber,
                                int shardCount,
                                @NonNull String workerCallsCounterName,
                                @NonNull String workerMillisCounterName) {
    this.jobId = jobId;
    this.shardNumber = shardNumber;
    this.shardCount = shardCount;
    this.workerCallsCounterName = workerCallsCounterName;
    this.workerMillisCounterName = workerMillisCounterName;
    this.counters = new CountersImpl();
  }

  public long getWorkerCallCount() {
    return getCounters().getCounter(workerCallsCounterName).getValue();
  }

  long getWorkerTimeMillis() {
    return getCounters().getCounter(workerMillisCounterName).getValue();
  }

  public String getLastWorkItemString() {
    return lastWorkItem;
  }

  void setLastWorkItemString(String lastWorkItem) {
    this.lastWorkItem = lastWorkItem;
  }

  void incrementWorkerCalls(long workerCalls) {
    getCounters().getCounter(workerCallsCounterName).increment(workerCalls);
  }

  void incrementWorkerMillis(long millis) {
    getCounters().getCounter(workerMillisCounterName).increment(millis);
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "[jobId=" + jobId + ", shardNumber=" + shardNumber
        + ", shardCount=" + shardCount + ", lastWorkItem=" + lastWorkItem + ", workerCallCount="
        + getWorkerCallCount() + ", workerTimeMillis=" + getWorkerTimeMillis() + "]";
  }
}
