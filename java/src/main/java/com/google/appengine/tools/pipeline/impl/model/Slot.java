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

import com.google.appengine.tools.pipeline.SlotId;
import com.google.appengine.tools.pipeline.impl.backend.SerializationStrategy;
import com.google.appengine.tools.pipeline.impl.util.EntityUtils;
import com.google.cloud.Timestamp;
import com.google.cloud.datastore.*;
import com.google.common.annotations.VisibleForTesting;

import java.io.IOException;
import java.io.NotSerializableException;
import java.time.Instant;
import java.util.*;
import java.util.logging.Level;
import java.util.stream.Collectors;


import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.extern.java.Log;

import javax.annotation.Nullable;


/**
 * A slot to be filled in with a value.
 *
 * @author rudominer@google.com (Mitch Rudominer)
 */
@Log
public class Slot extends PipelineModelObject {

  public static final String DATA_STORE_KIND = "pipeline-slot";

  private static final String FILLED_PROPERTY = "filled";
  private static final String VALUE_PROPERTY = "value";
  private static final String WAITING_ON_ME_PROPERTY = "waitingOnMe";
  private static final String FILL_TIME_PROPERTY = "fillTime";
  private static final String SOURCE_JOB_KEY_PROPERTY = "sourceJob";

  // persistent
  @Getter
  private boolean filled;

  /**
   * time slot was filled; `null` otherwise
   */
  @Getter
  @Nullable
  private Instant fillTime;
  private Object value;
  @Setter
  @Getter
  private Key sourceJobKey;
  @Getter
  private final Set<Key> waitingOnMeKeys;

  // transient
  /**
   * If this slot has not yet been inflated this method returns null
   */
  @Getter
  private List<Barrier> waitingOnMeInflated;
  //either a Blob or List<Key> ... given that we know implementation details of serialization
  private Object serializedVersion; //TODO: 'guaranteed' to be type serializable by datastore; what does this mean??

  private SerializationStrategy serializationStrategy;

  public Slot(Key rootJobKey, Key generatorJobKey, String graphGUID, SerializationStrategy serializationStrategy) {
    super(rootJobKey, generatorJobKey, graphGUID);
    this.waitingOnMeKeys = new HashSet<>();
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
    waitingOnMeKeys = new HashSet<>(getListProperty(WAITING_ON_ME_PROPERTY, entity));
    this.serializationStrategy = serializationStrategy;

    Object valueBlob = EntityUtils.getLargeValue(entity, VALUE_PROPERTY);

    if (lazy) {
      serializedVersion = valueBlob;
    } else {
      value = deserializeValue(valueBlob);
    }
  }


  private Object deserializeValue(Object value) {
    try {
      return serializationStrategy.deserializeValue(this, value);
    } catch (IOException | ClassNotFoundException e) {
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
        if (e instanceof NotSerializableException) {
          log.log(Level.SEVERE, "NotSerializable value of " + value.getClass().getSimpleName() + " contained within: " + this, e);
        }
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

  public Object getValue() {
    if (serializedVersion != null) {
      value = deserializeValue(serializedVersion);
      serializedVersion = null;
    }
    return value;
  }


  public void fill(Object value) {
    filled = true;
    this.value = value;
    serializedVersion = null;
    fillTime = Instant.now();
  }

  public SlotId getFullId() {
    return SlotId.of(this.getKey());
  }

  @Override
  public String toString() {
    return "Slot[" + getKeyName(getKey()) + ", value=" + (serializedVersion != null ? "..." : value)
        + ", filled=" + filled + ", waitingOnMe=" + waitingOnMeKeys + ", parent="
        + getKeyName(getGeneratorJobKey()) + ", guid=" + getGraphGuid() + "]";
  }

  public static Key key(String projectId,
                        String databaseId,
                        String namespace,
                        @NonNull String slotName) {
    KeyFactory keyFactory = new KeyFactory(projectId);
    if (databaseId != null) {
      keyFactory.setDatabaseId(databaseId);
    }
    if (namespace != null) {
      keyFactory.setNamespace(namespace);
    }
    keyFactory.setKind(DATA_STORE_KIND);
    return keyFactory.newKey(slotName);
  }

  @VisibleForTesting
  public static Key keyFromHandle(SlotId handle) {
    return key(handle.getProject(), handle.getDatabaseId(), handle.getNamespace(), handle.getSlotId());
  }
}
