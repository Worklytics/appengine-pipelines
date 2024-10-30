package com.google.appengine.tools.mapreduce.impl.shardedjob;

import com.google.cloud.datastore.Key;
import com.google.common.base.Preconditions;
import lombok.AllArgsConstructor;
import lombok.NonNull;
import lombok.Value;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.Optional;

/**
 * identifies a job that has been sharded (split into parallel tasks)
 *
 */
@AllArgsConstructor(staticName = "of")
@Value
public class ShardedJobId implements Serializable {

  private static final long serialVersionUID = 1L;

  /**
   * project in which job is executing
   */
  @NonNull
  String project;

  /**
   * database within project
   */
  @NonNull
  String databaseId;

  /**
   * namespace within the database
   */
  @Nullable
  String namespace;

  /**
   * uniquely identifies a job within a project and namespace
   */
  @NonNull
  String jobId;


  //q: url encode this?
  public String asEncodedString() {
    // NOTE: presumes / never used in namespace or project or job id - correct/
    Preconditions.checkArgument(!project.contains("/"), "project must not contain /");
    Preconditions.checkArgument(databaseId == null || !databaseId.contains("/"), "databaseId must not contain /");
    Preconditions.checkArgument(namespace == null || !namespace.contains("/"), "namespace must not contain /");
    Preconditions.checkArgument(!jobId.contains("/"), "jobId must not contain /");

    return project + "/" + Optional.ofNullable(databaseId).orElse("") + "/" + Optional.ofNullable(namespace).orElse("") + "/" + jobId;
  }

  public static ShardedJobId fromEncodedString(@NonNull String encoded) {
    String[] parts = encoded.split("/");
    if (parts.length != 4) {
      throw new IllegalArgumentException("Invalid encoded string: " + encoded);
    }
    String databaseId = parts[1].isEmpty() ? null : parts[1];
    String namespace = parts[2].isEmpty() ? null : parts[2];
    return new ShardedJobId(parts[0], databaseId, namespace, parts[3]);
  }

  public static ShardedJobId of (Key key) {
    return new ShardedJobId(key.getProjectId(), key.getDatabaseId(), key.getNamespace(), key.getName());
  }

}
