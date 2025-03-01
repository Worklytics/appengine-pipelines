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

package com.google.appengine.tools.pipeline;

import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicLong;

import static com.google.appengine.tools.pipeline.TestUtils.waitForJobToComplete;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test error handling through handleException.
 *
 * @author maximf@google.com (Maxim Fateev)
 */
public class DelayedValueTest extends PipelineTest {

  private static final int EXPECTED_RESULT = 5;
  private static final long DELAY_SECONDS = 10;

  private static AtomicLong duration1 = new AtomicLong();
  private static AtomicLong duration2 = new AtomicLong();

  @SuppressWarnings("serial")
  static class DelayedJob extends Job0<Integer> {

    @Override
    public Value<Integer> run() throws Exception {
      trace("DelayedJob.run");
      duration2.set(System.currentTimeMillis());
      return immediate(EXPECTED_RESULT);
    }
  }

  @SuppressWarnings("serial")
  static class TestDelayedValueJob extends Job0<Integer> {

    @Override
    public Value<Integer> run() {
      trace("TestDelayedValueJob.run");
      duration1.set(System.currentTimeMillis());
      Value<Void> delayedValue = newDelayedValue(DELAY_SECONDS);
      return futureCall(new DelayedJob(), waitFor(delayedValue));
    }
  }

  @Test
  public void testDelayedValue() throws Exception {
    JobRunId pipelineId= pipelineService.startNewPipeline(new TestDelayedValueJob());
    Integer five = waitForJobToComplete(pipelineService, pipelineId);
    assertEquals(EXPECTED_RESULT, five.intValue());
    assertEquals("TestDelayedValueJob.run DelayedJob.run", trace());
    assertTrue(duration2.get() - duration1.get() >= DELAY_SECONDS * 1000);
  }
}
