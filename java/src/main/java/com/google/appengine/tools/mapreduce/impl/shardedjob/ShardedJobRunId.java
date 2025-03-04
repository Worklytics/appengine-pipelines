package com.google.appengine.tools.mapreduce.impl.shardedjob;

import com.google.appengine.tools.pipeline.JobRunId;
import com.google.cloud.datastore.Key;
import com.google.common.base.Preconditions;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.io.Serial;

/**
 * identifies a job that has been sharded (split into parallel tasks)
 *
 */
@SuperBuilder
@EqualsAndHashCode(callSuper = true)
public class ShardedJobRunId extends JobRunId {

  @Serial
  private static final long serialVersionUID = 1L;

  private ShardedJobRunId(String project, String databaseId, String namespace, String jobId) {
    super(project, databaseId, namespace, jobId);
  }

  protected ShardedJobRunId(String encoded) {
    super(encoded);
  }

  public static ShardedJobRunId of(String project, String databaseId, String namespace, String jobId) {
    Preconditions.checkArgument(!jobId.contains(":"), "Job id must not contain ':'");
    return new ShardedJobRunId(project, databaseId, namespace, jobId);
  }

  public static ShardedJobRunId fromEncodedString(@NonNull String encoded) {
    return new ShardedJobRunId(encoded);
  }

  public static ShardedJobRunId of(Key key) {
    return new ShardedJobRunId(key.getProjectId(), key.getDatabaseId(), key.getNamespace(), key.getName());
  }

  @Override
  public String toString() {
    return "ShardedJobRunId(" + this.asEncodedString() + ")";
  }

}
