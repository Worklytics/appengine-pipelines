package com.google.appengine.tools.mapreduce.impl.shardedjob;

import lombok.AllArgsConstructor;
import lombok.NonNull;
import lombok.Value;

/**
 * identifies a task that will executed incrementally (slices)
 * represents a sharded job task (eg, one of N shards of a sharded task)
 *
 * arguably it's just a ShardId ... sliced/incremental executions is an implementation detail
 */
@Value
@AllArgsConstructor(staticName = "of")
public class IncrementalTaskId {

  /**
   * the sharded job this task belongs to
   */
  @NonNull
  ShardedJobId shardedJobId;

  /**
   * which shard this represents, eg, 0-39 for 40 shards
   */
  int number;

  @Override
  public String toString() {
    return asEncodedString();
  }

  /**
   * @return a string representation of this task id that can be used in URLs
   */
  public String asEncodedString() {
    return prefix(shardedJobId) + number;
  }

  private final static String NUMBER_SUFFIC_DELIMITER = "-task-";

  public static IncrementalTaskId parse(@NonNull String taskId) {
    String[] parts = taskId.split(NUMBER_SUFFIC_DELIMITER);
    if (parts.length != 2) {
      throw new IllegalArgumentException("Invalid taskId: " + taskId);
    }
    return IncrementalTaskId.of(ShardedJobId.fromEncodedString(parts[0]), Integer.parseInt(parts[1]));
  }

  private static String prefix(ShardedJobId shardedJobId) {
    return shardedJobId.asEncodedString() + NUMBER_SUFFIC_DELIMITER;
  }
}
