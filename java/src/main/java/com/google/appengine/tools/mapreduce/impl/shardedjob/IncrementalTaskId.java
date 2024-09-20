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

  @Deprecated
  static IncrementalTaskId parse(ShardedJobId shardedJobId, String taskId) {
    return IncrementalTaskId.of(shardedJobId, Integer.parseInt(taskId.substring(prefix(shardedJobId).length())));
  }

  public static IncrementalTaskId parse(String taskId) {
    String numberPart = taskId.substring(taskId.lastIndexOf("-"), taskId.length());
    String suffix = "-task-" + numberPart;
    String encodedJobId = taskId.replace(suffix, "").replace("-", "/");
    return IncrementalTaskId.of(ShardedJobId.fromEncodedString(encodedJobId), Integer.parseInt(numberPart));
  }

  private static String prefix(ShardedJobId shardedJobId) {
    return shardedJobId.asEncodedString().replace("/", "-") + "-task-";
  }
}
