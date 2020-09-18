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
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.appengine.tools.development.testing.LocalDatastoreServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalModulesServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalServiceTestHelper;
import com.google.appengine.tools.development.testing.LocalTaskQueueTestConfig;
import com.google.appengine.tools.pipeline.demo.UserGuideExamples.ComplexJob;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

/**
 * Tests for the sample code in the User Guide
 *
 * @author rudominer@google.com (Mitch Rudominer)
 */
public class UserGuideTest {

  private transient LocalServiceTestHelper helper;

  public UserGuideTest() {
    LocalTaskQueueTestConfig taskQueueConfig = new LocalTaskQueueTestConfig();
    taskQueueConfig.setCallbackClass(TestingTaskQueueCallback.class);
    taskQueueConfig.setDisableAutoTaskExecution(false);
    taskQueueConfig.setShouldCopyApiProxyEnvironment(true);
    helper = new LocalServiceTestHelper(new LocalDatastoreServiceTestConfig(), taskQueueConfig,
        new LocalModulesServiceTestConfig());
  }

  @BeforeEach
  public void setUp() throws Exception {
    helper.setUp();
    System.setProperty(USE_SIMPLE_GUIDS_FOR_DEBUGGING, "true");
  }

  @AfterEach
  public void tearDown() throws Exception {
    helper.tearDown();
  }

  public void testComplexJob() throws Exception {
    doComplexJobTest(3, 7, 11);
    doComplexJobTest(-5, 71, 6);
  }

  private void doComplexJobTest(int x, int y, int z) throws Exception {
    String pipelineId = PipelineTest.pipelineService().startNewPipeline(new ComplexJob(), x, y, z);
    JobInfo jobInfo = PipelineTest.pipelineService().getJobInfo(pipelineId);
    JobInfo.State state = jobInfo.getJobState();
    if (JobInfo.State.COMPLETED_SUCCESSFULLY == state) {
      System.out.println("The output is " + jobInfo.getOutput());
    }
    int output = (Integer) waitForJobToComplete(pipelineId);
    assertEquals(((x - y) * (x - z)) - 2, output);
  }

  @SuppressWarnings("unchecked")
  private <E> E waitForJobToComplete(String pipelineId) throws Exception {
    while (true) {
      Thread.sleep(2000);
      JobInfo jobInfo = PipelineTest.pipelineService().getJobInfo(pipelineId);
      switch (jobInfo.getJobState()) {
        case COMPLETED_SUCCESSFULLY:
          return (E) jobInfo.getOutput();
        case RUNNING:
          break;
        case WAITING_TO_RETRY:
          break;
        case STOPPED_BY_ERROR:
          throw new RuntimeException("Job stopped " + jobInfo.getError());
        case STOPPED_BY_REQUEST:
          throw new RuntimeException("Job stopped by request.");
        case CANCELED_BY_REQUEST:
          throw new RuntimeException("Job cancelled by request.");
        default:
          throw new RuntimeException("Unknown Job state: " + jobInfo.getJobState());
      }
    }
  }
}
