// Copyright 2013 Google Inc.
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
import com.google.appengine.tools.pipeline.impl.util.SerializationUtils;

import java.io.IOException;

/**
 * A datastore entity for storing information about job failure.
 *
 * @author maximf@google.com (Maxim Fateev)
 */
public class ExceptionRecord extends PipelineModelObject {

  public static final String DATA_STORE_KIND = "pipeline-exception";
  private static final String EXCEPTION_PROPERTY = "exception";

  private final Throwable exception;

  public ExceptionRecord(
      Key rootJobKey, Key generatorJobKey, String graphGUID, Throwable exception) {
    super(rootJobKey, generatorJobKey, graphGUID);
    this.exception = exception;
  }

  public ExceptionRecord(Entity entity) {
    super(entity);
    Blob serializedExceptionBlob = entity.getBlob(EXCEPTION_PROPERTY);
    byte[] serializedException = serializedExceptionBlob.toByteArray();
    try {
      exception = (Throwable) SerializationUtils.deserialize(serializedException);
    } catch (IOException e) {
      throw new RuntimeException("Failed to deserialize exception for " + getKey(), e);
    }
  }

  public Throwable getException() {
    return exception;
  }

  @Override
  protected String getDatastoreKind() {
    return DATA_STORE_KIND;
  }

  @Override
  public Entity toEntity() {
    try {
      Entity.Builder entity = toProtoBuilder();
      byte[] serializedException = SerializationUtils.serialize(exception);
      entity.set(EXCEPTION_PROPERTY, BlobValue.newBuilder(Blob.copyFrom(serializedException)).setExcludeFromIndexes(true).build());
      return entity.build();
    } catch (IOException e) {
      throw new RuntimeException("Failed to serialize exception for " + getKey(), e);
    }
  }
}
