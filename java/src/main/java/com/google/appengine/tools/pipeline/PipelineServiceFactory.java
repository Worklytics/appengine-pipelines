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

package com.google.appengine.tools.pipeline;

import com.google.appengine.api.utils.SystemProperty;
import com.google.appengine.tools.pipeline.impl.PipelineServiceImpl;
import com.google.auth.Credentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.datastore.Datastore;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Base64;

/**
 * A factory for obtaining instances of {@link PipelineService}
 *
 * @author rudominer@google.com (Mitch Rudominer)
 */
public final class PipelineServiceFactory {
  private PipelineServiceFactory() {
  }

  /**
   * @return PipelineService using application defaults for GCP infra needed underneath
   */
  @Deprecated //coupled to AppEngine
  public static PipelineService newPipelineService() {
    return newPipelineService(SystemProperty.applicationId.get());
  }

  /**
   * @param projectId GCP project under which pipelines will execute
   * @return PipelineService that will execute pipelines in specific project, auth'd by application default credentials
   */
  public static PipelineService newPipelineService(String projectId) {
    try {
      GoogleCredentials credentials = GoogleCredentials.getApplicationDefault();
      return new PipelineServiceImpl(projectId, credentials);
    } catch (IOException e) {
      throw new RuntimeException("Failed to get default credentials", e);
    }
  }

  public static PipelineService newPipelineService(String projectId, Datastore datastore) {
    return new PipelineServiceImpl(projectId, datastore);
  }

  /**
   *
   * @param projectId GCP project under which pipelines will execute
   * @param base64EncodedServiceAccountKey service account key used to authenticate for access to that GCP project
   *                                       (need not be from same project, but must have datastore/task queue perms)
   * @return PipelineService that will execute pipelines in specific project, auth'd by serviceAccountKey
   */
  public static PipelineService newPipelineService(String projectId, String base64EncodedServiceAccountKey) {

    String jsonKey = new String(Base64.getDecoder().decode(base64EncodedServiceAccountKey.trim().getBytes()));
    try {
      return new PipelineServiceImpl(projectId, ServiceAccountCredentials.fromStream(new ByteArrayInputStream(jsonKey.getBytes())));
    } catch (IOException e) {
      throw new RuntimeException("Failed to build ServiceAccountCredentials from key", e);
    }
  }
}
