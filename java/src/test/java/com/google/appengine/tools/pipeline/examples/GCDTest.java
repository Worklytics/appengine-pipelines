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

package com.google.appengine.tools.pipeline.examples;

import com.google.appengine.tools.pipeline.JobInfo;
import com.google.appengine.tools.pipeline.JobRunId;
import com.google.appengine.tools.pipeline.PipelineTest;
import com.google.appengine.tools.pipeline.examples.AsyncGCDExample.PrintGCDJob;
import com.google.appengine.tools.pipeline.examples.GCDExample.GCDJob;
import com.google.apphosting.api.ApiProxy;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import static com.google.appengine.tools.pipeline.TestUtils.waitForJobToComplete;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 *
 * @author rudominer@google.com (Mitch Rudominer)
 *
 */
public class GCDTest extends PipelineTest {

  private static final transient Logger logger = Logger.getLogger(GCDTest.class.getName());

  @Test
  public void testGCDCalculation() throws Exception {
    doGcdTest(1, 1, 1);
    doGcdTest(12, 20, 4);
    doGcdTest(3600, 105, 15);
  }

  @Test
  public void testAsyncGCD() throws Exception {
    doAsyncGcdTest("Sparkles", 2, 3, "Hello, Sparkles. The GCD of 2 and 3 is 1.");
    doAsyncGcdTest("Biff", 2, 2, "Hello, Biff. The GCD of 2 and 2 is 2.");
  }

  private void doGcdTest(int x, int y, int expectedGcd) throws Exception {
    JobRunId pipelineId= pipelineService.startNewPipeline(new GCDJob(), x, y);
    int calculatedGcd = waitForJobToComplete(pipelineService, pipelineId);
    logger.info("The GCD of " + x + " and " + y + " is " + calculatedGcd);
    assertEquals(expectedGcd, calculatedGcd);
    // PipelineObjects pipelineObjects =
    // PipelineManager.queryFullPipeline(pipelineId);
    // System.out.println(pipelineObjects.toJson());
  }

  private void doAsyncGcdTest(final String userName, final int x, final int y,
      String expectedMessage) throws Exception {
    final CountDownLatch latch = new CountDownLatch(1);
    final StringBuilder builder = new StringBuilder();
    AsyncGCDExample.callback = new AsyncGCDExample.Callback() {
      @Override
      public String getUserName() {
        ApiProxy.setEnvironmentForCurrentThread(apiProxyEnvironment);
        return userName;
      }

      @Override
      public int getSecondInt() {
        ApiProxy.setEnvironmentForCurrentThread(apiProxyEnvironment);
        return y;
      }

      @Override
      public int getFirstInt() {
        ApiProxy.setEnvironmentForCurrentThread(apiProxyEnvironment);
        return x;
      }

      @Override
      public void acceptOutput(String output) {
        builder.append(output);
        latch.countDown();
      }
    };
    JobRunId pipelineId= pipelineService.startNewPipeline(new PrintGCDJob());
    assertTrue(latch.await(3, TimeUnit.MINUTES));
    assertEquals(expectedMessage, builder.toString());
    // Wait for job task thread to complete
    Thread.sleep(2000);
    JobInfo jobInfo = pipelineService.getJobInfo(pipelineId);
    assertEquals(JobInfo.State.COMPLETED_SUCCESSFULLY, jobInfo.getJobState());
  }
}
