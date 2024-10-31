package com.google.appengine.tools.mapreduce.impl.shardedjob;

import com.google.appengine.tools.pipeline.JobId;
import com.google.cloud.datastore.Key;
import lombok.*;

import java.io.Serial;

/**
 * identifies a job that has been sharded (split into parallel tasks)
 *
 */
@EqualsAndHashCode(callSuper = true)
@ToString
public class ShardedJobId extends JobId {

  @Serial
  private static final long serialVersionUID = 1L;

  private ShardedJobId(String project, String databaseId, String namespace, String jobId) {
    super(project, databaseId, namespace, jobId);
  }

  protected ShardedJobId(String encoded) {
    super(encoded);
  }

  public static ShardedJobId of(String project, String databaseId, String namespace, String jobId) {
    return new ShardedJobId(project, databaseId, namespace, jobId);
  }

  public static ShardedJobId fromEncodedString(@NonNull String encoded) {
    return new ShardedJobId(encoded);
  }

  public static ShardedJobId of(Key key) {
    return new ShardedJobId(key.getProjectId(), key.getDatabaseId(), key.getNamespace(), key.getName());
  }

}
