package com.google.appengine.tools.mapreduce.impl.util;

import com.google.cloud.datastore.*;

import java.time.Instant;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Copyright 2020 - Worklytics, Co.
 */

/**
 * class to wrap Entity with accessors that are safe for missing properties
 *
 * TODO: unify w com.google.appengine.tools.pipeline.impl.util.EntityUtils, when merge the two projects
 */
public class EntityUtils {
  final static String IS_SHARDED_PROPERTY = "isSharded";

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
    boolean isSharded;
    if (value instanceof Blob) {
      //usual case
      isSharded = false;
      builder.set(propertyName, BlobValue.newBuilder((Blob)value).setExcludeFromIndexes(true).build());
    } else if (value instanceof List) {
      isSharded = true;
      builder.set(propertyName, ((List<Key>) value).stream().map(k -> KeyValue.newBuilder(k).setExcludeFromIndexes(true).build()).collect(Collectors.toList()));

    } else {
      throw new RuntimeException("value not of type that can be stored into Datastore");
    }
    builder.set(IS_SHARDED_PROPERTY, BooleanValue.newBuilder(isSharded).setExcludeFromIndexes(true).build());
  }


  public static Object getLargeValue(Entity entity, String valueProperty) {
    if (entity.getBoolean(IS_SHARDED_PROPERTY)) {
      //sharded case
      // not great, this is basically knowing internals of serializationStrategy
      return entity.getList(valueProperty).stream().map(k -> ((KeyValue) k).get()).collect(Collectors.toList());
    } else {
      return entity.getBlob(valueProperty);
    }
  }
}
