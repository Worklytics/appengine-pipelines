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
import lombok.extern.java.Log;

import java.io.IOException;
import java.util.logging.Level;

/**
 * A datastore entity for storing information about job failure.
 *
 * @author maximf@google.com (Maxim Fateev)
 */
@Log
public class ExceptionRecord extends PipelineModelObject implements ExpiringDatastoreEntity {

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
    } catch (IOException | ClassNotFoundException e) {
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
    Entity.Builder entity = toProtoBuilder();
    try {
      byte[] serializedException = SerializationUtils.serialize(exception);
      entity.set(EXCEPTION_PROPERTY, BlobValue.newBuilder(Blob.copyFrom(serializedException)).setExcludeFromIndexes(true).build());
    } catch (java.io.NotSerializableException e) {
      // let's not break the whole pipeline if the exception is not serializable
      // log and create a new one that it is with enough information for us
      log.log(Level.SEVERE, String.format("Key %s: failed to serialize exception: %s", getKey(), exception), e);
      try {
        Throwable t = new Throwable(String.format("%s: %s", exception.getClass().getName(), exception.getMessage()));
        t.setStackTrace(exception.getStackTrace());
        byte[] serializedException = SerializationUtils.serialize(t);
        entity.set(EXCEPTION_PROPERTY, BlobValue.newBuilder(Blob.copyFrom(serializedException)).setExcludeFromIndexes(true).build());
      } catch (IOException ex) {
        // should never happen now
        throw new RuntimeException("Failed to serialize exception for " + getKey(), ex);
      }
    } catch (IOException e) {
      throw new RuntimeException("Failed to serialize exception for " + getKey(), e);
    }
    return entity.build();
  }
}
