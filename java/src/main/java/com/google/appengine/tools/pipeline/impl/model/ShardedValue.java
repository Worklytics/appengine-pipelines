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


import com.google.cloud.datastore.Blob;
import com.google.cloud.datastore.BlobValue;
import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.LongValue;

/**
 * A data model to represent shard of a large value.
 *
 * @author ozarov@google.com (Arie Ozarov)
 *
 */
public class ShardedValue extends PipelineModelObject {

  public static final String DATA_STORE_KIND = "pipeline-sharded-value";
  private static final String SHARD_ID_PROPERTY = "shard-id";
  private static final String VALUE_PROPERTY = "value";

  private final long shardId;
  private final byte[] value;

  public ShardedValue(PipelineModelObject parent, long shardId, byte[] value) {
    super(parent.getRootJobKey(), parent.getKey(), null, parent.getGeneratorJobKey(),
        parent.getGraphGuid());
    this.shardId = shardId;
    this.value = value;
  }

  public ShardedValue(Entity entity) {
    super(entity);
    this.shardId = entity.getLong(SHARD_ID_PROPERTY);
    this.value = entity.getBlob(VALUE_PROPERTY).toByteArray();
  }

  @Override
  public Entity toEntity() {
    Entity.Builder entity = toProtoBuilder();
    entity.set(SHARD_ID_PROPERTY, LongValue.newBuilder(shardId).setExcludeFromIndexes(true).build());
    entity.set(VALUE_PROPERTY, BlobValue.newBuilder(Blob.copyFrom(value)).setExcludeFromIndexes(true).build());
    return entity.build();
  }

  @Override
  public String getDatastoreKind() {
    return DATA_STORE_KIND;
  }

  public long getShardId() {
    return shardId;
  }

  public byte[] getValue() {
    return value;
  }

  @Override
  public String toString() {
    return "ShardedValue[" + getKey().getName() + ", shardId=" + shardId + ", value="
        + getValueForDisplay() + "]";
  }

  private String getValueForDisplay() {
    int count = 0;
    StringBuilder stBuilder = new StringBuilder(103);
    for (byte b : value) {
      if (++count == 100) {
        stBuilder.append("...");
        break;
      }
      stBuilder.append(String.format("%02x", b & 0xFF));
    }
    return stBuilder.toString();
  }
}
