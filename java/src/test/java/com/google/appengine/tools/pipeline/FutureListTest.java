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

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static com.google.appengine.tools.pipeline.TestUtils.waitForJobToComplete;
import static org.junit.jupiter.api.Assertions.assertEquals;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Note the difference between testFutureList() and testReturnFutureList(). In
 * testFutureList() the call to futureList() happens in the parent job,
 * SumsListJob. In testReturnFutureList() the call to futureList() happens in
 * the child job, ReturnsListJob.
 */
public class FutureListTest extends PipelineTest {

  @Test
  public void testFutureList() throws Exception {
    JobRunId pipelineId= pipelineService.startNewPipeline(new SumsListJob1());
    Integer sum = waitForJobToComplete(pipelineService, pipelineId);
    assertEquals(21, sum.intValue());
  }

  @Test
  public void testReturnFutureList() throws Exception {
    JobRunId pipelineId= pipelineService.startNewPipeline(new SumsListJob2());
    Integer sum = waitForJobToComplete(pipelineService, pipelineId);
    assertEquals(21, sum.intValue());
  }

  // Thanks to Ronoaldo José de Lana Pereira for
  // suggesting this.
  @Test
  public void testEmptyFutureList() throws Exception {
    JobRunId pipelineId= pipelineService.startNewPipeline(new SumsEmptyListJob());
    Integer sum = waitForJobToComplete(pipelineService, pipelineId);
    assertEquals(0, sum.intValue());
  }

  /**
   * In this job, the call to futureList() happens not in a child job but in
   * this job itself. This means that the FutureList is not the return value of
   * any job.
   */
  @SuppressWarnings("serial")
  private static class SumsListJob1 extends Job0<Integer> {
    @Override
    public Value<Integer> run() {
      Returns5Job returns5Job = new Returns5Job();
      SumJob sumJob = new SumJob();
      List<Value<Integer>> valueList = new ArrayList<>(4);
      valueList.add(futureCall(returns5Job));
      valueList.add(immediate(7));
      valueList.add(futureCall(returns5Job));
      valueList.add(immediate(4));
      return futureCall(sumJob, futureList(valueList));
    }
  }

  /**
   * In this job, the call to futureList() happens in a child job, ReturnsList
   * job. This means that the FutureList is the return value of the child job
   */
  @SuppressWarnings("serial")
  private static class SumsListJob2 extends Job0<Integer> {
    @Override
    public Value<Integer> run() {
      return futureCall(new SumJob(), futureCall(new ReturnsListJob()));
    }
  }

  @SuppressWarnings("serial")
  private static class SumsEmptyListJob extends Job0<Integer> {
    @Override
    public Value<Integer> run() {
      List<Value<Integer>> emptyValueList = new ArrayList<>(0);
      return futureCall(new SumJob(), futureList(emptyValueList));
    }
  }

  @SuppressWarnings("serial")
  private static class ReturnsListJob extends Job0<List<Integer>> {
    @Override
    public Value<List<Integer>> run() {
      Returns5Job returns5Job = new Returns5Job();
      return new FutureList<>(
          futureCall(returns5Job), immediate(7), futureCall(returns5Job), immediate(4));
    }
  }

  @SuppressWarnings("serial")
  private static class Returns5Job extends Job0<Integer> {
    @Override
    public Value<Integer> run() {
      return immediate(5);
    }
  }

  @SuppressWarnings("serial")
  private static class SumJob extends Job1<Integer, List<Integer>> {
    @Override
    public Value<Integer> run(List<Integer> list) {
      int sum = 0;
      for (int x : list) {
        sum += x;
      }
      return immediate(sum);
    }
  }
}
