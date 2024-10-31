package com.google.appengine.tools.pipeline;

import com.google.appengine.tools.pipeline.impl.model.Slot;
import com.google.cloud.datastore.Key;
import com.google.common.base.Preconditions;
import lombok.*;

import javax.annotation.Nullable;
import java.io.Serial;
import java.io.Serializable;
import java.util.Optional;

/**
 * Identifies a {@link com.google.appengine.tools.pipeline.impl.model.Slot}, created by a pipeline job run, to be filled with a value.
 */
@EqualsAndHashCode(callSuper = false)
@AllArgsConstructor(access = AccessLevel.PROTECTED)
@Getter
public class SlotId implements Serializable {

  @Serial
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
   * uniquely identifies a slot, given project, database, and namespace
   */
  @NonNull
  private final String slotId;

  public String asEncodedString() {
    // NOTE: presumes / never used in namespace or project or job id - correct/
    Preconditions.checkArgument(!project.contains("/"), "project must not contain /");
    Preconditions.checkArgument(databaseId == null || !databaseId.contains("/"), "databaseId must not contain /");
    Preconditions.checkArgument(namespace == null || !namespace.contains("/"), "namespace must not contain /");
    Preconditions.checkArgument(!slotId.contains("/"), "jobId must not contain /");

    return project + "/" + Optional.ofNullable(databaseId).orElse("") + "/" + Optional.ofNullable(namespace).orElse("") + "/" + slotId;
  }

  public static SlotId fromEncodedString(@NonNull String encoded) {
    return new SlotId(encoded);
  }

  public static SlotId of (Key key) {
    Preconditions.checkArgument(key.getKind().equals(Slot.DATA_STORE_KIND), "Key must be of kind 'pipeline-slot'");
    return new SlotId(key.getProjectId(), key.getDatabaseId(), key.getNamespace(), key.getName());
  }

  protected SlotId(String encoded) {
    String[] parts = encoded.split("/");
    if (parts.length != 4) {
      throw new IllegalArgumentException("Invalid encoded string: " + encoded);
    }
    this.project = parts[0];
    this.databaseId = parts[1].isEmpty() ? null : parts[1];
    this.namespace = parts[2].isEmpty() ? null : parts[2];
    this.slotId = parts[3];
  }

  @Override
  public String toString() {
    return asEncodedString();
  }
}
