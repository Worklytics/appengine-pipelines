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

import com.google.appengine.api.taskqueue.dev.LocalTaskQueue;
import com.google.appengine.api.taskqueue.dev.QueueStateInfo;
import com.google.appengine.tools.development.testing.LocalDatastoreServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalModulesServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalServiceTestHelper;
import com.google.appengine.tools.development.testing.LocalTaskQueueTestConfig;
import com.google.appengine.tools.pipeline.impl.PipelineManager;
import com.google.apphosting.api.ApiProxy;

import com.google.auth.oauth2.GoogleCredentials;
import lombok.Getter;
import lombok.SneakyThrows;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.io.IOException;

/**
 * @author rudominer@google.com (Mitch Rudominer)
 *
 */
public abstract class PipelineTest {

  protected LocalServiceTestHelper helper;
  protected ApiProxy.Environment apiProxyEnvironment;

  private static StringBuffer traceBuffer;

  public static final String PROJECT = "project";

  @Getter
  private LocalTaskQueue taskQueue;

  protected PipelineService pipelineService;
  protected PipelineManager pipelineManager;

  public PipelineTest() {
    LocalTaskQueueTestConfig taskQueueConfig = new LocalTaskQueueTestConfig();
    taskQueueConfig.setCallbackClass(TestingTaskQueueCallback.class);
    taskQueueConfig.setDisableAutoTaskExecution(false);
    taskQueueConfig.setShouldCopyApiProxyEnvironment(true);
    helper = new LocalServiceTestHelper(
        new LocalDatastoreServiceTestConfig()
            .setDefaultHighRepJobPolicyUnappliedJobPercentage(
                isHrdSafe() ? 100 : 0),
        taskQueueConfig, new LocalModulesServiceTestConfig());
  }

  /**
   * Whether this test will succeed even if jobs remain unapplied indefinitely.
   *
   * NOTE: This may be called from the constructor, i.e., before the object is
   * fully initialized.
   */
  protected boolean isHrdSafe() {
    return true;
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
  public void setUp() throws Exception {
    traceBuffer = new StringBuffer();
    helper.setUp();
    apiProxyEnvironment = ApiProxy.getCurrentEnvironment();
    System.setProperty(USE_SIMPLE_GUIDS_FOR_DEBUGGING, "true");
    taskQueue = LocalTaskQueueTestConfig.getLocalTaskQueue();

    pipelineService = pipelineService();
    pipelineManager = pipelineManager();
  }

  @SneakyThrows
  public static PipelineManager pipelineManager() {
    return new PipelineManager(PROJECT, GoogleCredentials.getApplicationDefault());
  }

  @SneakyThrows
  public static PipelineService pipelineService() {
    return PipelineServiceFactory.newPipelineService(PROJECT);
  }

  @AfterEach
  public void tearDown() throws Exception {
    helper.tearDown();
  }


}
