/*
 * Copyright 2010 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.appengine.tools.mapreduce.impl.util;

import com.google.appengine.tools.pipeline.impl.util.SerializationUtils;
import com.google.cloud.datastore.*;
import com.google.appengine.tools.mapreduce.CorruptDataException;
import com.google.common.base.Function;
import com.google.common.collect.Lists;
import lombok.NonNull;
import lombok.SneakyThrows;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import static com.google.appengine.tools.pipeline.impl.util.SerializationUtils.serializeToByteArray;

/**
 * A serialization utility class to aid with serializing and deserializing values to and from datastore entities (properties).
 *
 */
public class DatastoreSerializationUtil {

  private static final Logger log = Logger.getLogger(DatastoreSerializationUtil.class.getName());
  // 1MB - 200K slack for the rest of the properties and entity overhead
  private static final int MAX_BLOB_BYTE_SIZE = 1024 * 1024 - 200 * 1024;
  private static final String SHARDED_VALUE_KIND = "MR-ShardedValue";
  private static final Function<Entity, Key> ENTITY_TO_KEY = Entity::getKey;


  public static <T extends Serializable> T deserializeFromDatastoreProperty(
    Transaction tx, Entity entity, String property) {
    return deserializeFromDatastoreProperty(tx, entity, property, false);
  }

  @SneakyThrows
  @SuppressWarnings("unchecked")
  public static <T extends Serializable> T deserializeFromDatastoreProperty(
    Transaction tx, Entity entity, String property, boolean lenient) {

    try {
      byte[] bytes;
      try {
        Blob value = entity.getBlob(property);
        bytes = value.toByteArray();
      } catch (ClassCastException e) {
        List<KeyValue> keys = entity.getList(property);

        //NOTE: fetch() important here; unlike get(), fetch() will return null for missing entities AND return in order
        List<Key> asKeys = keys.stream().map(KeyValue::get).collect(Collectors.toList());
        List<Entity> shards = tx.fetch(asKeys.toArray(new Key[0]));
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        for (int i = 0; i < shards.size(); i++) {
          Entity shard = shards.get(i);
          if (shard == null) {
            throw new CorruptDataException("Missing data shard: " + i);
          }
          byte[] shardBytes = shard.getBlob("content").toByteArray();
          bout.write(shardBytes, 0, shardBytes.length);
        }
        for (Entity shard : shards) {
          if (shard == null) {
            throw new CorruptDataException("Missing data shard");
          }
          byte[] shardBytes = shard.getBlob("content").toByteArray();
          bout.write(shardBytes, 0, shardBytes.length);
        }
        bytes = bout.toByteArray();
      }
      return SerializationUtils.deserialize(bytes);
    } catch (RuntimeException | IOException ex) {
      log.warning("Deserialization of " + entity.getKey() + "#" + property + " failed: "
              + ex.getMessage() + ", returning null instead.");
      if (lenient) {
        return null;
      }
      throw ex;
    }
  }

  public static Iterable<Key> getShardedValueKeysFor(@NonNull Transaction tx, Key parent, String property) {
    KeyQuery.Builder queryBuilder = Query.newKeyQueryBuilder()
      .setKind(SHARDED_VALUE_KIND);

    StructuredQuery.Filter filter = StructuredQuery.PropertyFilter.hasAncestor(parent);
    if (property != null) {
      filter = StructuredQuery.CompositeFilter.and(filter, StructuredQuery.PropertyFilter.eq("property", property));
    }
    queryBuilder.setFilter(filter);

    KeyQuery query = queryBuilder.build();

    QueryResults<Key> results = tx.run(query);

    List<Key> keys = new ArrayList<>();
    while (results.hasNext()) {
      keys.add(results.next());
    }

    return keys;
  }

  /**
   * Serializes the given value to a byte array and stores it in the given entity under the property.
   * if too large to fit in a single BLOB property, may split into entities (shard), and store them
   *
   * @param tx                under which to store the value (only used in sharded case)
   * @param entity            on which to store the value
   * @param property          name to use
   * @param value             to store
   * @param priorVersionShards whether the property was previously sharded (if known)
   * @return number of shards used to store the value, if any
   */
  public static int serializeToDatastoreProperty(
    Transaction tx, Entity.Builder entity, String property, Serializable value, Optional<Integer> priorVersionShards) {
    byte[] bytes = serializeToByteArray(value);

    Key key = entity.build().getKey();

    if (bytes.length < MAX_BLOB_BYTE_SIZE) {
      if (priorVersionShards.isEmpty() || priorVersionShards.get() > 0) {
        //need to delete previous shards
        List<Key> toDelete = Lists.newArrayList(getShardedValueKeysFor(tx, key, property));
        tx.delete(toDelete.toArray(new Key[toDelete.size()]));
      }
      entity.set(property, BlobValue.newBuilder(Blob.copyFrom(bytes)).setExcludeFromIndexes(true).build());
      return 0;
    } else {
      int shardId = 0;
      int offset = 0;
      List<Entity> shards = new ArrayList<>(bytes.length / MAX_BLOB_BYTE_SIZE + 1);
      while (offset < bytes.length) {
        int limit = offset + MAX_BLOB_BYTE_SIZE;
        byte[] chunk = Arrays.copyOfRange(bytes, offset, Math.min(limit, bytes.length));
        offset = limit;
        String keyName = String.format("shard-%02d", ++shardId);
        KeyFactory keyFactory = tx.getDatastore().newKeyFactory().addAncestors(key.getAncestors());
        PathElement parentPathElement;
        if (key.hasId()) {
          parentPathElement = PathElement.of(key.getKind(), key.getId());
        } else {
          parentPathElement = PathElement.of(key.getKind(), key.getName());
        }
        keyFactory.addAncestor(parentPathElement);
        keyFactory.setKind(SHARDED_VALUE_KIND);

        Entity shard = Entity.newBuilder(keyFactory.newKey(keyName))
          .set("property", property)
          .set("content", BlobValue.newBuilder(Blob.copyFrom(chunk)).setExcludeFromIndexes(true).build())
          .build();
        shards.add(shard);
      }

      if (shards.size() < priorVersionShards.orElse(Integer.MAX_VALUE)) {
        List<Key> preExistingShards = Lists.newArrayList(getShardedValueKeysFor(tx, key, property));
        //NOTE: datastore lib is smart enough that any puts() overwriting entities deleted in this step() will take precedence
        // de-dup here would be premature optimization
        tx.delete(preExistingShards.toArray(Key[]::new));
      }

      tx.put(shards.toArray(Entity[]::new));
      List<KeyValue> values = shards.stream()
        .map(ENTITY_TO_KEY)
        .map(k -> KeyValue.newBuilder(k).setExcludeFromIndexes(true).build())
        .collect(Collectors.toList());
      entity.set(property, ListValue.newBuilder().set(values).build());
      return shards.size();
    }
  }

  /**
   * get count of shards used to store value for property on entity, if any
   *
   * @param entity
   * @param property
   * @return
   */
  public static int shardsUsedToStore(Entity entity, String property) {
    try {
      List<KeyValue> keys = entity.getList(property);
      return keys.size();
    } catch (ClassCastException e) {
      return 0;
    }
  }
}