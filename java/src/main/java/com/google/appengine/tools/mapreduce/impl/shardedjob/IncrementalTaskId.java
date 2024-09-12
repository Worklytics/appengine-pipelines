package com.google.appengine.tools.mapreduce.impl.shardedjob;

import lombok.AllArgsConstructor;
import lombok.NonNull;
import lombok.Value;

//represents a shared job task (eg, one of N shards of a sharded task)
@Value
@AllArgsConstructor(staticName = "of")
class IncrementalTaskId {

  @NonNull
  ShardedJobId shardedJobId;
  /**
   * which shard this represents, eg, 0-39 for 40 shards
   */
  int number;

  @Override
  public String toString() {
    return prefix(shardedJobId) + number;
  }

  static IncrementalTaskId parse(ShardedJobId shardedJobId, String taskId) {
    return IncrementalTaskId.of(shardedJobId, Integer.parseInt(taskId.substring(prefix(shardedJobId).length())));
  }

  private static String prefix(ShardedJobId shardedJobId) {
    return shardedJobId.asEncodedString().replace("/", "-") + "-task-";
  }
}
