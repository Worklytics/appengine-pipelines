// Copyright 2011 Google Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy of
// the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations under
// the License.

package com.google.appengine.tools.pipeline.impl.model;

import com.google.appengine.tools.pipeline.impl.backend.SerializationStrategy;
import com.google.appengine.tools.pipeline.impl.util.EntityUtils;
import com.google.cloud.Timestamp;
import com.google.cloud.datastore.*;
import com.google.common.annotations.VisibleForTesting;

import java.io.IOException;
import java.time.Instant;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * A slot to be filled in with a value.
 *
 * @author rudominer@google.com (Mitch Rudominer)
 */
public class Slot extends PipelineModelObject {

  public static final String DATA_STORE_KIND = "pipeline-slot";
  private static final String FILLED_PROPERTY = "filled";
  private static final String VALUE_PROPERTY = "value";
  private static final String WAITING_ON_ME_PROPERTY = "waitingOnMe";
  private static final String FILL_TIME_PROPERTY = "fillTime";
  private static final String SOURCE_JOB_KEY_PROPERTY = "sourceJob";

  // persistent
  private boolean filled;
  private Instant fillTime;
  private Object value;
  private Key sourceJobKey;
  private final List<Key> waitingOnMeKeys;

  // transient
  private List<Barrier> waitingOnMeInflated;
  //either a Blob or List<Key> ... given that we know implementation details of serialization
  private Object serializedVersion; //TODO: 'guaranteed' to be type serializable by datastore; what does this mean??
  private SerializationStrategy serializationStrategy;

  public Slot(Key rootJobKey, Key generatorJobKey, String graphGUID, SerializationStrategy serializationStrategy) {
    super(rootJobKey, generatorJobKey, graphGUID);
    this.waitingOnMeKeys = new LinkedList<>();
    this.serializationStrategy = serializationStrategy;
  }

  public Slot(Entity entity, SerializationStrategy serializationStrategy) {
    this(entity, serializationStrategy, false);
    this.serializationStrategy = serializationStrategy;
  }

  public Slot(Entity entity, SerializationStrategy serializationStrategy, boolean lazy) {
    super(entity);
    filled = entity.getBoolean(FILLED_PROPERTY);
    fillTime = EntityUtils.getInstant(entity, FILL_TIME_PROPERTY);
    sourceJobKey = EntityUtils.getKey(entity, SOURCE_JOB_KEY_PROPERTY);
    waitingOnMeKeys = getListProperty(WAITING_ON_ME_PROPERTY, entity);
    this.serializationStrategy = serializationStrategy;
    if (lazy) {
      serializedVersion = entity.getBlob(VALUE_PROPERTY);
    } else {
      value = deserializeValue(entity.getBlob(VALUE_PROPERTY));
    }
  }



  private Object deserializeValue(Object value) {
    try {
      return serializationStrategy.deserializeValue(this, value);
    } catch (IOException e) {
      throw new RuntimeException("Failed to deserialize slot value from " + this.getKey(), e);
    }
  }

  @Override
  public Entity toEntity() {
    Entity.Builder entity = toProtoBuilder();
    entity.set(FILLED_PROPERTY, BooleanValue.newBuilder(filled).setExcludeFromIndexes(true).build());
    if (null != fillTime) {
      entity.set(FILL_TIME_PROPERTY,
        TimestampValue.newBuilder(Timestamp.ofTimeMicroseconds(fillTime.toEpochMilli() * 1000L)).setExcludeFromIndexes(true).build());
    }
    if (null != sourceJobKey) {
      entity.set(SOURCE_JOB_KEY_PROPERTY, sourceJobKey);
    }
    entity.set(WAITING_ON_ME_PROPERTY, waitingOnMeKeys.stream().map(KeyValue::of).collect(Collectors.toList()));
    if (serializedVersion == null) {
      try {
        serializedVersion = serializationStrategy.serializeValue(this, value);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    EntityUtils.setLargeValue(entity, VALUE_PROPERTY, serializedVersion);

    return entity.build();
  }

  @Override
  protected String getDatastoreKind() {
    return DATA_STORE_KIND;
  }

  public void inflate(Map<Key, Barrier> pool) {
    waitingOnMeInflated = buildInflated(waitingOnMeKeys, pool);
  }

  public void addWaiter(Barrier waiter) {
    waitingOnMeKeys.add(waiter.getKey());
    if (null == waitingOnMeInflated) {
      waitingOnMeInflated = new LinkedList<>();
    }
    waitingOnMeInflated.add(waiter);
  }

  public boolean isFilled() {
    return filled;
  }

  public Object getValue() {
    if (serializedVersion != null) {
      value = deserializeValue(serializedVersion);
      serializedVersion = null;
    }
    return value;
  }

  /**
   * Will return {@code null} if this slot is not filled.
   */
  public Instant getFillTime() {
    return fillTime;
  }

  public Key getSourceJobKey() {
    return sourceJobKey;
  }

  public void setSourceJobKey(Key key) {
    sourceJobKey = key;
  }

  public void fill(Object value) {
    filled = true;
    this.value = value;
    serializedVersion = null;
    fillTime = Instant.now();
  }

  public List<Key> getWaitingOnMeKeys() {
    return waitingOnMeKeys;
  }

  /**
   * If this slot has not yet been inflated this method returns null;
   */
  public List<Barrier> getWaitingOnMeInflated() {
    return waitingOnMeInflated;
  }

  @Override
  public String toString() {
    return "Slot[" + getKeyName(getKey()) + ", value=" + (serializedVersion != null ? "..." : value)
        + ", filled=" + filled + ", waitingOnMe=" + waitingOnMeKeys + ", parent="
        + getKeyName(getGeneratorJobKey()) + ", guid=" + getGraphGuid() + "]";
  }

  public static Key key(String projectId, String namespace, String slotName) {
    KeyFactory keyFactory = new KeyFactory(projectId, namespace);
    keyFactory.setKind(DATA_STORE_KIND);
    return keyFactory.newKey(slotName);
  }

  @VisibleForTesting
  public static Key keyFromHandle(String handle) {
    return Key.fromUrlSafe(handle);
  }
}
