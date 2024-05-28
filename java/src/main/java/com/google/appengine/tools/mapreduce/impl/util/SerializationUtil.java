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

import com.google.cloud.datastore.*;
import com.google.appengine.tools.mapreduce.CorruptDataException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.collect.Lists;
import lombok.SneakyThrows;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.zip.*;

/**
 * A serialization utility class.
 *
 */
public class SerializationUtil {

  private static final Logger log = Logger.getLogger(SerializationUtil.class.getName());
  // 1MB - 200K slack for the rest of the properties and entity overhead
  private static final int MAX_BLOB_BYTE_SIZE = 1024 * 1024 - 200 * 1024;
  private static final String SHARDED_VALUE_KIND = "MR-ShardedValue";
  private static final Function<Entity, Key> ENTITY_TO_KEY = Entity::getKey;

  public static int MAX_UNCOMPRESSED_BYTE_SIZE = 50_000;

  public static byte[] serialize(Serializable obj) throws IOException {
    // Serialize the object
    ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
    try (ObjectOutputStream objectOut = new ObjectOutputStream(byteOut)) {
      objectOut.writeObject(obj);
    }
    // Compress only if serialized data exceeds threshold
    if (byteOut.size() > MAX_UNCOMPRESSED_BYTE_SIZE) {
      ByteArrayOutputStream compressedByteOut = new ByteArrayOutputStream();
      try (GZIPOutputStream gzipOut = new GZIPOutputStream(compressedByteOut);
           ObjectOutputStream objectOut = new ObjectOutputStream(gzipOut)) {
        objectOut.writeObject(obj);
        objectOut.flush();
        gzipOut.finish();
        return compressedByteOut.toByteArray();
      }
    }
    return byteOut.toByteArray();
  }

  @SneakyThrows
  public static <T> T deserialize(byte[] data) throws IOException, ClassNotFoundException {
    // Attempt to decompress
    try (ByteArrayInputStream byteIn = new ByteArrayInputStream(data)) {
      if (isGZIPCompressed(data)) {
        try (GZIPInputStream gzipIn = new GZIPInputStream(byteIn);
             ObjectInputStream objectIn = new ObjectInputStream(gzipIn)) {
          return (T) objectIn.readObject();
        }
      } else {
        try (ObjectInputStream objectIn = new ObjectInputStream(byteIn)) {
          return (T) objectIn.readObject();
        }
      }
    }
  }

  @VisibleForTesting
  static boolean isGZIPCompressed(byte[] bytes) {
    return (bytes != null)
      && (bytes.length >= 2)
      && ((bytes[0] == (byte) (GZIPInputStream.GZIP_MAGIC))
      && (bytes[1] == (byte) (GZIPInputStream.GZIP_MAGIC >> 8)));
  }


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
      return (T) deserialize(bytes);
    } catch (RuntimeException | IOException ex) {
      log.warning("Deserialization of " + entity.getKey() + "#" + property + " failed: "
              + ex.getMessage() + ", returning null instead.");
      if (lenient) {
        return null;
      }
      throw ex;
    }
  }

  public static Iterable<Key> getShardedValueKeysFor(Transaction tx, Key parent, String property) {

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

  public static void serializeToDatastoreProperty(
      Transaction tx, Entity.Builder entity, String property, Serializable o) {
    byte[] bytes = serializeToByteArray(o);

    Key key = entity.build().getKey();

    // deleting previous shards
    List<Key> toDelete = Lists.newArrayList(getShardedValueKeysFor(tx, key, property));

    if (bytes.length < MAX_BLOB_BYTE_SIZE) {
      tx.delete(toDelete.toArray(new Key[toDelete.size()]));
      entity.set(property, BlobValue.newBuilder(Blob.copyFrom(bytes)).setExcludeFromIndexes(true).build());
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
      if (shards.size() < toDelete.size()) {
        tx.delete(toDelete.toArray(new Key[toDelete.size()]));
      }
      tx.put(shards.toArray(new Entity[shards.size()]));
      List<KeyValue> value = shards.stream()
        .map(ENTITY_TO_KEY)
        .map(k -> KeyValue.newBuilder(k).setExcludeFromIndexes(true).build())
        .collect(Collectors.toList());
      entity.set(property, ListValue.newBuilder().set(value).build());
    }
  }

  @SneakyThrows
  public static byte[] serializeToByteArray(Serializable o) {
    return serialize(o);
  }

  public static byte[] getBytes(ByteBuffer in) {
    if (in.hasArray() && in.position() == 0
        && in.arrayOffset() == 0 && in.array().length == in.limit()) {
      return in.array();
    } else {
      byte[] buf = new byte[in.remaining()];
      int position = in.position();
      in.get(buf);
      in.position(position);
      return buf;
    }
  }

  @SneakyThrows
  @SuppressWarnings("unchecked")
  public static <T extends Serializable> T clone(T toClone) {
    byte[] bytes = SerializationUtil.serializeToByteArray(toClone);
    return (T) SerializationUtil.deserialize(bytes);
  }
}