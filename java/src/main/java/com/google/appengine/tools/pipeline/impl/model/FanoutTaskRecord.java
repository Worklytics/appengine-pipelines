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
import com.google.cloud.datastore.Key;

/**
 * A datastore entity for storing data necessary for a fan-out task
 *
 * @author rudominer@google.com (Mitch Rudominer)
 */
public class FanoutTaskRecord extends PipelineModelObject {

  public static final String DATA_STORE_KIND = "pipeline-fanoutTask";
  private static final String PAYLOAD_PROPERTY = "payload";

  private final byte[] payload;

  public FanoutTaskRecord(Key rootJobKey, byte[] payload) {
    super(rootJobKey, null, null);
    if (payload == null) {
      throw new RuntimeException("Payload must not be null");
    }
    this.payload = payload;
  }

  public FanoutTaskRecord(Entity entity) {
    super(entity);
    Blob payloadBlob = entity.getBlob(PAYLOAD_PROPERTY);
    payload = payloadBlob.toByteArray();
  }

  public byte[] getPayload() {
    return payload;
  }

  @Override
  protected String getDatastoreKind() {
    return DATA_STORE_KIND;
  }

  @Override
  public Entity toEntity() {
    Entity.Builder entity = toProtoBuilder();
    entity.set(PAYLOAD_PROPERTY, BlobValue.newBuilder(Blob.copyFrom(payload)).setExcludeFromIndexes(true).build());
    return entity.build();
  }
}
