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
import static org.junit.jupiter.api.Assertions.*;

import com.google.appengine.tools.development.testing.LocalModulesServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalServiceTestHelper;
import com.google.appengine.tools.development.testing.LocalTaskQueueTestConfig;
import com.google.appengine.tools.pipeline.JobSetting.BackoffFactor;
import com.google.appengine.tools.pipeline.JobSetting.BackoffSeconds;
import com.google.appengine.tools.pipeline.JobSetting.MaxAttempts;


import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.opentest4j.AssertionFailedError;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * @author rudominer@google.com (Mitch Rudominer)
 *
 */
@PipelineSetupExtensions
public class RetryTest {

  private LocalServiceTestHelper helper;

  public RetryTest() {
    LocalTaskQueueTestConfig taskQueueConfig = new LocalTaskQueueTestConfig();
    taskQueueConfig.setCallbackClass(TestingTaskQueueCallback.class);
    taskQueueConfig.setDisableAutoTaskExecution(false);
    taskQueueConfig.setShouldCopyApiProxyEnvironment(true);
    helper = new LocalServiceTestHelper(taskQueueConfig, new LocalModulesServiceTestConfig());
  }

  @BeforeEach
  public void setUp(PipelineService pipelineService) throws Exception {
    helper.setUp();
    System.setProperty(USE_SIMPLE_GUIDS_FOR_DEBUGGING, "true");
    this.pipelineService = pipelineService;
  }

  @AfterEach
  public void tearDown() throws Exception {
    helper.tearDown();
  }

  private static volatile CountDownLatch countdownLatch;

  private PipelineService pipelineService;

  @Test
  public void testMaxAttempts() throws Exception {
    doMaxAttemptsTest(true);
    doMaxAttemptsTest(false);
  }

  @Test
  public void testLongBackoffTime() throws Exception {
    // Fail twice with a 3 second backoff factor. Wait 5 seconds. Should
    // succeed.
    runJob(3, 2, 5, false);

    // Fail 3 times with a 3 second backoff factor. Wait 10 seconds. Should fail
    // because 3 + 9 = 12 > 10
    try {
      runJob(3, 3, 10, false);
      fail("Excepted exception");
    } catch (AssertionFailedError e) {
      // expected;
    }

    // Fail 3 times with a 3 second backoff factor. Wait 15 seconds. Should
    // succeed
    // because 3 + 9 = 12 < 15
    /* Commented, as it will fail with following trace:

    Exception in thread "DefaultQuartzScheduler_Worker-4" java.lang.NoClassDefFoundError: com/google/apphosting/executor/Task
	at com.google.appengine.repackaged.com.google.storage.onestore.v3.proto2api.OnestoreAction.<clinit>(OnestoreAction.java:3245)
	at com.google.apphosting.api.proto2api.DatastorePb.<clinit>(DatastorePb.java:60422)
	at com.google.appengine.api.taskqueue.TaskQueuePb.<clinit>(TaskQueuePb.java:47981)
	at com.google.appengine.api.taskqueue.TaskQueuePb$TaskQueueAddRequest$Builder.getDescriptorForType(TaskQueuePb.java:8187)
	at com.google.appengine.repackaged.com.google.protobuf.TextFormat$Printer.print(TextFormat.java:369)
	at com.google.appengine.repackaged.com.google.protobuf.TextFormat$Printer.print(TextFormat.java:359)
	at com.google.appengine.repackaged.com.google.protobuf.TextFormat$Printer.printToString(TextFormat.java:647)
	at com.google.appengine.repackaged.com.google.protobuf.AbstractMessage$Builder.toString(AbstractMessage.java:447)
	at java.util.Formatter$FormatSpecifier.printString(Formatter.java:2886)
	at java.util.Formatter$FormatSpecifier.print(Formatter.java:2763)
	at java.util.Formatter.format(Formatter.java:2520)
	at java.util.Formatter.format(Formatter.java:2455)
	at java.lang.String.format(String.java:2940)
	at com.google.appengine.api.taskqueue.dev.UrlFetchJob.reschedule(UrlFetchJob.java:162)


    The reason is that it will try to instantiate a class (TaskQueueAddRequest.Builder) with it is not present
    as part of "dev/stub" libraries, provoking that all threads will be blocked and the test will never finish due that error
    This happens from > 1.9.82 version
     */
    //runJob(3, 3, 15, false);
  }

  private void doMaxAttemptsTest(boolean succeedTheLastTime) throws Exception {
    String pipelineId = runJob(1, 4, 10, succeedTheLastTime);
    // Wait for framework to save Job information
    Thread.sleep(1000L);
    JobInfo jobInfo = pipelineService.getJobInfo(pipelineId);
    JobInfo.State expectedState =
        (succeedTheLastTime ? JobInfo.State.COMPLETED_SUCCESSFULLY
            : JobInfo.State.STOPPED_BY_ERROR);
    assertEquals(expectedState, jobInfo.getJobState());
  }

  private String runJob(int backoffFactor, int maxAttempts, int awaitSeconds,
      boolean succeedTheLastTime) throws Exception {
    countdownLatch = new CountDownLatch(maxAttempts);

    String pipelineId = pipelineService.startNewPipeline(
        new InvokesFailureJob(succeedTheLastTime, maxAttempts, backoffFactor));
    assertTrue(countdownLatch.await(awaitSeconds, TimeUnit.SECONDS));
    return pipelineId;
  }

  /**
   * A job that invokes {@link FailureJob}.
   */
  @SuppressWarnings("serial")
  public static class InvokesFailureJob extends Job0<Void> {
    private boolean succeedTheLastTime;
    int maxAttempts;
    int backoffFactor;

    public InvokesFailureJob(boolean succeedTheLastTime, int maxAttempts, int backoffFactor) {
      this.succeedTheLastTime = succeedTheLastTime;
      this.maxAttempts = maxAttempts;
      this.backoffFactor = backoffFactor;
    }

    @Override
    public Value<Void> run() {
      JobSetting[] jobSettings =
          new JobSetting[] {new MaxAttempts(maxAttempts), new BackoffSeconds(1),
              new BackoffFactor(backoffFactor)};
      return futureCall(new FailureJob(succeedTheLastTime), jobSettings);
    }
  }

  /**
   * A job that fails every time except possibly the last time.
   */
  @SuppressWarnings("serial")
  public static class FailureJob extends Job0<Void> {
    private boolean succeedTheLastTime;

    public FailureJob(boolean succeedTheLastTime) {
      this.succeedTheLastTime = succeedTheLastTime;
    }

    @Override
    public Value<Void> run() {
      countdownLatch.countDown();
      if (countdownLatch.getCount() == 0 && succeedTheLastTime) {
        return null;
      }
      throw new RuntimeException("Hello");
    }
  }
}