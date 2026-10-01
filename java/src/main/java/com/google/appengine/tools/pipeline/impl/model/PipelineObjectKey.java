package com.google.appengine.tools.pipeline.impl.model;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;

import com.google.appengine.tools.pipeline.JobSetting;
import com.google.appengine.tools.pipeline.impl.util.GUIDGenerator;
import com.google.cloud.datastore.Key;
import com.google.cloud.datastore.KeyFactory;
import com.google.cloud.datastore.PathElement;

import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.Singular;

/**
 * Named description of a pipeline datastore key.
 *
 * Prefer {@link #builder()} over positional string factories so project, database, namespace,
 * and kind cannot be passed in the wrong order.
 */
@Builder
@Getter
public final class PipelineObjectKey {

  @NonNull
  private final String projectId;

  private final String databaseId;

  private final String namespace;

  @NonNull
  private final String kind;

  @NonNull
  private final String name;

  @Singular
  private final List<PathElement> ancestors;

  /**
   * Name for a new root pipeline object: a guid plus a UTC timestamp, so logs show when it was created.
   */
  public static String newName() {
    return GUIDGenerator.nextGUID().replace("-", "")
        + "_"
        + Instant.now().truncatedTo(ChronoUnit.SECONDS).toString()
            .replace(":", "")
            .replace("T", "_")
            .replace("Z", "")
            .replace("-", "");
  }

  /**
   * A new entity-group child of {@code parent}, in the same project, database, and namespace.
   */
  public static PipelineObjectKey childOf(@NonNull Key parent, @NonNull String kind) {
    PipelineObjectKeyBuilder builder = builder()
        .projectId(parent.getProjectId())
        .databaseId(parent.getDatabaseId())
        .namespace(parent.getNamespace())
        .kind(kind)
        .name(GUIDGenerator.nextGUID());
    for (PathElement ancestor : parent.getAncestors()) {
      builder.ancestor(ancestor);
    }
    builder.ancestor(PathElement.of(parent.getKind(), parent.getName()));
    return builder.build();
  }

  public Key toDatastoreKey() {
    KeyFactory keyFactory = new KeyFactory(projectId);
    String canonicalDatabaseId = JobSetting.canonicalDatabaseId(databaseId);
    if (canonicalDatabaseId != null) {
      keyFactory.setDatabaseId(canonicalDatabaseId);
    }
    String canonicalNamespace = JobSetting.canonicalNamespace(namespace);
    if (canonicalNamespace != null) {
      keyFactory.setNamespace(canonicalNamespace);
    }
    if (ancestors != null && !ancestors.isEmpty()) {
      keyFactory.addAncestors(ancestors);
    }
    keyFactory.setKind(kind);
    return keyFactory.newKey(name);
  }
}
