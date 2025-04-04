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

import static com.google.appengine.tools.pipeline.impl.util.GUIDGenerator.USE_SIMPLE_GUIDS_FOR_DEBUGGING;
import static org.mockito.Mockito.mock;

import com.google.appengine.api.taskqueue.dev.LocalTaskQueue;


import com.google.appengine.tools.development.testing.LocalServiceTestHelper;
import com.google.appengine.tools.development.testing.LocalTaskQueueTestConfig;
import com.google.appengine.tools.mapreduce.PipelineSetupExtensions;
import com.google.appengine.tools.pipeline.di.JobRunServiceComponent;
import com.google.appengine.tools.pipeline.impl.PipelineManager;

import com.google.appengine.tools.pipeline.impl.backend.AppEngineBackEnd;
import com.google.appengine.tools.pipeline.impl.backend.SerializationStrategy;
import com.google.apphosting.api.ApiProxy;

import com.google.auth.Credentials;
import com.google.cloud.datastore.DatastoreOptions;
import lombok.Getter;

import lombok.Setter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

/**
 * @author rudominer@google.com (Mitch Rudominer)
 *
 */
@PipelineSetupExtensions
public abstract class PipelineTest {

  protected LocalServiceTestHelper helper;
  protected ApiProxy.Environment apiProxyEnvironment;

  private static StringBuffer traceBuffer;


  @Getter
  private LocalTaskQueue taskQueue;

  protected PipelineService pipelineService;
  protected PipelineManager pipelineManager;

  protected AppEngineBackEnd appEngineBackend;

  public PipelineTest() {
    LocalTaskQueueTestConfig taskQueueConfig = new LocalTaskQueueTestConfig();
    taskQueueConfig.setCallbackClass(TestingTaskQueueCallback.class);
    taskQueueConfig.setDisableAutoTaskExecution(false);
    taskQueueConfig.setShouldCopyApiProxyEnvironment(true);
    helper = new LocalServiceTestHelper(taskQueueConfig);
  }

  protected static void trace(String what) {
    if (traceBuffer.length() > 0) {
      traceBuffer.append(' ');
    }
    traceBuffer.append(what);
  }

  protected static String trace() {
    return traceBuffer.toString();
  }

  @BeforeEach
  public void setUp(PipelineService pipelineService, PipelineManager pipelineManager, AppEngineBackEnd appEngineBackend) throws Exception {
    traceBuffer = new StringBuffer();
    helper.setUp();
    apiProxyEnvironment = ApiProxy.getCurrentEnvironment();
    System.setProperty(USE_SIMPLE_GUIDS_FOR_DEBUGGING, "true");
    taskQueue = LocalTaskQueueTestConfig.getLocalTaskQueue();

    this.appEngineBackend = appEngineBackend;
    this.pipelineManager = pipelineManager;
    this.pipelineService = pipelineService;

    //hack to put pipelineManager into taskQueuecallback; we need to replace tasks client any way, so this will go away
    TestingTaskQueueCallback.pipelineManager = pipelineManager;
  }

  @Getter
  @Setter(onMethod_ = @BeforeEach)
  JobRunServiceComponent component;


  public static SerializationStrategy getSerializationStrategy() {
    //just fake this, project/credentials shouldn't be used
    return new AppEngineBackEnd(AppEngineBackEnd.Options.builder()
      .datastoreOptions(DatastoreOptions.newBuilder()
        .setProjectId("test-project")
        .setCredentials(mock(Credentials.class))
        .build())
      .build(),
      null,
      null);
  }

  @AfterEach
  public void tearDown() throws Exception {
    helper.tearDown();
  }
}
