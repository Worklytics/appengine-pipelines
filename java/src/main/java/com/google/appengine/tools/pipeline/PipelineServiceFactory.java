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
import com.google.appengine.tools.pipeline.impl.backend.AppEngineBackEnd;
import com.google.appengine.tools.pipeline.impl.backend.PipelineBackEnd;
import com.google.auth.Credentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.datastore.Datastore;

import java.io.IOException;

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
    try {
      return newPipelineService(SystemProperty.applicationId.get(), GoogleCredentials.getApplicationDefault());
    } catch (IOException e) {
      throw new RuntimeException("Failed to get default credentials", e);
    }
  }

  public static PipelineService newPipelineService(PipelineBackEnd backEnd) {
    return new PipelineServiceImpl(backEnd);
  }

  /**
   *
   * @param projectId GCP project under which pipelines will execute
   * @param gcpCredentials credentials to use when authenticating pipeline service with GCP
   * @return PipelineService that will execute pipelines in specific project, auth'd by serviceAccountKey
   */
  public static PipelineService newPipelineService(String projectId, Credentials gcpCredentials) {
    return new PipelineServiceImpl(projectId, gcpCredentials);
  }

  public static PipelineService newPipelineService(PipelineBackEnd.Options options) {
    if (options instanceof AppEngineBackEnd.Options) {
      return new PipelineServiceImpl(new AppEngineBackEnd(options.as(AppEngineBackEnd.Options.class)));
    } else {
      throw new IllegalArgumentException("Options of type that is not supported by PipelineServiceFactory");
    }
  }
}
