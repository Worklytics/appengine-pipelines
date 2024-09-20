package com.google.appengine.tools.mapreduce.impl.shardedjob;

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

  // q: databaseId? not for now.

  /**
   * namespace within the project. (null for the default namespace)
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
    Preconditions.checkArgument(namespace == null || !namespace.contains("/"), "namespace must not contain /");
    Preconditions.checkArgument(!jobId.contains("/"), "jobId must not contain /");

    return project + "/" + Optional.ofNullable(namespace).orElse("") + "/" + jobId;
  }

  public static ShardedJobId fromEncodedString(@NonNull String encoded) {
    String[] parts = encoded.split("/");
    if (parts.length != 3) {
      throw new IllegalArgumentException("Invalid encoded string: " + encoded);
    }
    return new ShardedJobId(parts[0], parts[1], parts[2]);
  }

}
