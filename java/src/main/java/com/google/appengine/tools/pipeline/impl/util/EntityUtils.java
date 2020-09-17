package com.google.appengine.tools.pipeline.impl.util;

import com.google.cloud.datastore.*;

import java.time.Instant;
import java.util.List;
import java.util.stream.Collectors;

/**
 * class to wrap Entity with accessors that are safe for missing properties
 */
public class EntityUtils {

  public static Key getKey(Entity entity, String propertyName) {
    return entity.contains(propertyName) ? entity.getKey(propertyName) : null;
  }

  public static Instant getInstant(Entity entity, String propertyName) {
    return entity.contains(propertyName) ? entity.getTimestamp(propertyName).toDate().toInstant() : null;
  }

  public static String getString(Entity entity, String propertyName) {
    return entity.contains(propertyName) ? entity.getString(propertyName) : null;
  }

  /**
   * set a "large" value onto entity builder
   *
   * @param builder
   * @param propertyName
   *
   * @param value either List<Key> or Blob
   */
  public static void setLargeValue(Entity.Builder builder, String propertyName, Object value) {
    if (value instanceof Blob) {
      //usual case
      builder.set(propertyName, BlobValue.newBuilder((Blob)value).setExcludeFromIndexes(false).build());
    } else if (value instanceof List) {
      builder.set(propertyName, ((List<Key>) value).stream().map(KeyValue::of).collect(Collectors.toList()));
    } else {
      throw new RuntimeException("value not of type that can be stored into Datastore");
    }
  }


}
