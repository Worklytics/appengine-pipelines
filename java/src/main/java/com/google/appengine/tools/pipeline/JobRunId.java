package com.google.appengine.tools.pipeline;

import com.google.appengine.tools.pipeline.impl.model.JobRecord;
import com.google.cloud.datastore.Key;
import com.google.common.base.Preconditions;
import lombok.*;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.Objects;
import java.util.Optional;

/**
 * identifies a particular run of a job in the pipelines framework
 *  also, a root job, which identifies a run a *pipeline*
 *
 */
@EqualsAndHashCode(callSuper = false)
@AllArgsConstructor(access = AccessLevel.PROTECTED)
@Getter
public class JobRunId implements Serializable {

  private static final long serialVersionUID = 1L;

  /**
   * project in which job is executing
   */
  @NonNull
  private final  String project;

  /**
   * database within project
   */
  @Nullable
  private final  String databaseId;

  /**
   * namespace within the database
   */
  @Nullable
  private final  String namespace;

  /**
   * uniquely identifies a job within a project and namespace
   */
  @NonNull
  private final String jobId;


  protected JobRunId(String encoded) {
    String[] parts = encoded.split("/");
    if (parts.length != 4) {
      throw new IllegalArgumentException("Invalid encoded string: " + encoded);
    }
    this.project = parts[0];
    this.databaseId = parts[1].isEmpty() ? null : parts[1];
    this.namespace = parts[2].isEmpty() ? null : parts[2];
    this.jobId = parts[3];
  }

  //q: url encode this?
  public String asEncodedString() {
    // NOTE: presumes / never used in namespace or project or job id - correct/
    Preconditions.checkArgument(!project.contains("/"), "project must not contain /");
    Preconditions.checkArgument(databaseId == null || !databaseId.contains("/"), "databaseId must not contain /");
    Preconditions.checkArgument(namespace == null || !namespace.contains("/"), "namespace must not contain /");
    Preconditions.checkArgument(!jobId.contains("/"), "jobId must not contain /");

    return project + "/" + Optional.ofNullable(databaseId).orElse("") + "/" + Optional.ofNullable(namespace).orElse("") + "/" + jobId;
  }

  public static JobRunId fromEncodedString(@NonNull String encoded) {
    return new JobRunId(encoded);
  }

  public static JobRunId of (Key key) {
    Preconditions.checkArgument(Objects.equals(key.getKind(), JobRecord.DATA_STORE_KIND), "key must be a JobRecord key");
    return new JobRunId(key.getProjectId(), key.getDatabaseId(), key.getNamespace(), key.getName());
  }

  public static JobRunId of(String project, String databaseId, String namespace, String jobId) {
    return new JobRunId(project, databaseId, namespace, jobId);
  }


  @Override
  public String toString() {
    return asEncodedString();
  }
}
