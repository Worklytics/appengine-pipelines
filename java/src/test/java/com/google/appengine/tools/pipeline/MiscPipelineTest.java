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

import com.google.appengine.tools.pipeline.JobInfo.State;
import com.google.appengine.tools.pipeline.JobSetting.StatusConsoleUrl;
import com.google.appengine.tools.pipeline.impl.model.JobRecord;
import com.google.appengine.tools.pipeline.impl.model.PipelineObjects;
import com.google.cloud.datastore.DatastoreOptions;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.java.Log;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static com.google.appengine.tools.pipeline.TestUtils.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Misc tests including:
 *  Passing large values.
 *  Delay used in a slow job
 *  JobSetting inheritance
 *  PromisedValue (and filling it more than once)
 *  Cancel pipeline
 *  WaitFor passed to a new pipeline
 *  FutureValue passed to a new pipeline
 *
 * @author rudominer@google.com (Mitch Rudominer)
 */
public class MiscPipelineTest extends PipelineTest {

  private static long[] largeValue;

  @BeforeEach
  public void setUp() throws Exception {
    // prev version of library worked w 2e6 longs (16MB)- this doesn't, possibly emulator limitation
    //int valueSize = 2_000_000;
    int valueSize = 500_000;

    largeValue = new long[valueSize];

    Random random = new Random();
    for (int i = 0; i < largeValue.length; i++) {
      largeValue[i] = random.nextLong();
    }
  }

  @SuppressWarnings("serial")
  private static class ReturnFutureListJob extends Job0<List<String>> {

    @Override
    public Value<List<String>> run() throws Exception {
      FutureValue<String> child1 = futureCall(new StrJob<>(), immediate(Long.valueOf(123)));
      FutureValue<String> child2 = futureCall(new StrJob<>(), immediate(Long.valueOf(456)));
      return new FutureList<>(ImmutableList.of(child1, child2));
    }
  }

  @Test
  public void testReturnFutureList() throws Exception {
    JobRunId pipelineId = pipelineService.startNewPipeline(new ReturnFutureListJob());
    List<String> value = waitForJobToComplete(pipelineService, pipelineId);
    assertEquals(ImmutableList.of("123", "456"), value);
  }

  private static class StringToLong implements Function<String, Long>, Serializable {

    private static final long serialVersionUID = -913828405842203610L;

    @Override
    public Long apply(String input) {
      return Long.valueOf(input);
    }
  }

  @SuppressWarnings("serial")
  private static class TestTransformJob extends Job0<Long> {

    @Override
    public Value<Long> run() {
      FutureValue<String> child1 = futureCall(new StrJob<>(), immediate(Long.valueOf(123)));
      return futureCall(new Jobs.Transform<>(new StringToLong()), child1);
    }
  }

  @Test
  public void testTransform() throws Exception {
    JobRunId pipelineId= pipelineService.startNewPipeline(new TestTransformJob());
    Long value = waitForJobToComplete(pipelineService, pipelineId);
    assertEquals(Long.valueOf(123), value);
  }

  @Log

  @SuppressWarnings("serial")
  private static class RootJob extends Job0<String> {

    private final boolean delete;

    RootJob(boolean delete) {
      this.delete = delete;
    }

    @Override
    public Value<String> run() throws Exception {
      FutureValue<String> child1 = futureCall(new StrJob<>(), immediate(Long.valueOf(123)));
      FutureValue<String> child2 = futureCall(new StrJob<>(), immediate(Long.valueOf(456)));
      if (delete) {
        // this corrupts itself, bc it deletes job records for itself; so then when job executor tries to update state
        // AFTER execution of run() method, there is no state in datastore to update.

        // previously, this worked in tests bc DeletePipelineJob was async, via queues, with delay. so the job record
        // existed long enough to be marked as complete by executor before deletion.

        // NOTE 500, 1000 ms don't seem sufficient when testing locally.
        // but default is 10s, which slows down tests
        return Jobs.waitForAllAndDeleteWithDelay(this, 2000L, child1, child2);
      } else {
        return Jobs.waitForAll(this, child1, child2);
      }
    }
  }

  @Test
  public void testWaitForAll() throws Exception {
    JobRunId pipelineId= pipelineService.startNewPipeline(new RootJob(false));
    JobInfo jobInfo = waitUntilJobComplete(pipelineService, pipelineId);
    assertEquals(JobInfo.State.COMPLETED_SUCCESSFULLY, jobInfo.getJobState());
    assertEquals("123", jobInfo.getOutput());
    assertNotNull(pipelineService.getJobInfo(pipelineId));
  }

  @Test
  public void testWaitForAllAndDelete() throws Exception {
    JobRunId pipelineId= pipelineService.startNewPipeline(new RootJob(true));

    JobInfo jobInfo = waitUntilJobComplete(pipelineService, pipelineId);
    assertEquals(JobInfo.State.COMPLETED_SUCCESSFULLY, jobInfo.getJobState());
    assertEquals("123", jobInfo.getOutput());

    // expect DeletePipelineJob to delete async via queue, rather than inline ...
    waitUntilTaskQueueIsEmpty(getTaskQueue());
    try {
      pipelineService.getJobInfo(pipelineId);
      fail("Was expecting a NoSuchObjectException exception");
    } catch (NoSuchObjectException expected) {
      // expected;
    }
  }

  @SuppressWarnings("serial")
  @RequiredArgsConstructor
  private static class CallerJob extends Job0<String> {

    static AtomicBoolean flag = new AtomicBoolean();

    @Getter
    final DatastoreOptions options;


    @Override
    public Value<String> run() throws Exception {

      FutureValue<Void> child1 = futureCall(new ChildJob());
      FutureValue<String> child2 = futureCall(new StrJob<>(), immediate(Long.valueOf(123)));
      String str1 = getPipelineService().startNewPipeline(new EchoJob<>(), child2).asEncodedString();
      String str2 = getPipelineService().startNewPipeline(new CalledJob(), waitFor(child1)).asEncodedString();
      return immediate(str1 + "," + str2);
    }
  }

  @SuppressWarnings("serial")
  private static class EchoJob<T> extends Job1<T, T> {

    @Override
    public Value<T> run(T t) throws Exception {
      return immediate(t);
    }
  }

  @SuppressWarnings("serial")
  private static class ChildJob extends Job0<Void> {

    @Override
    public Value<Void> run() throws Exception {
      Thread.sleep(5000);
      CallerJob.flag.set(true);
      return null;
    }
  }

  @SuppressWarnings("serial")
  private static class CalledJob extends Job0<Boolean> {

    @Override
    public Value<Boolean> run() throws Exception {
      return immediate(CallerJob.flag.get());
    }
  }

  @Test
  public void testWaitForUsedByNewPipeline(DatastoreOptions datastoreOptions) throws Exception {
    JobRunId pipelineId= pipelineService.startNewPipeline(new CallerJob(datastoreOptions));
    JobInfo jobInfo = waitUntilJobComplete(pipelineService, pipelineId);
    assertEquals(JobInfo.State.COMPLETED_SUCCESSFULLY, jobInfo.getJobState());
    JobRunId[] calledPipelines = Arrays.stream(((String) jobInfo.getOutput()).split(","))
      .map(s -> JobRunId.fromEncodedString(s)).collect(Collectors.toCollection(ArrayList::new)).toArray(new JobRunId[0]);
    jobInfo = waitUntilJobComplete(pipelineService, calledPipelines[0]);
    assertEquals(JobInfo.State.COMPLETED_SUCCESSFULLY, jobInfo.getJobState());
    assertEquals("123", jobInfo.getOutput());
    jobInfo = waitUntilJobComplete(pipelineService, calledPipelines[1]);
    assertEquals(JobInfo.State.COMPLETED_SUCCESSFULLY, jobInfo.getJobState());
    assertTrue((Boolean) jobInfo.getOutput());
  }


  @SuppressWarnings("serial")
  private abstract static class AbstractJob extends Job0<String> {

    @Override
    public Value<String> run() throws Exception {
      return immediate(getValue());
    }

    protected abstract String getValue();
  }

  @SuppressWarnings("serial")
  private static class ConcreteJob extends AbstractJob {

    @Override
    protected String getValue() {
      return "Shalom";
    }

    @Override
    public String getJobDisplayName() {
      return "ConcreteJob: " + getValue();
    }

    @SuppressWarnings("unused")
    public Value<String> handleException(Throwable t) {
      return immediate("Got exception!");
    }
  }

  @Test
  public void testGetJobDisplayName() throws Exception {
    ConcreteJob job = new ConcreteJob();
    JobRunId pipelineId= pipelineService.startNewPipeline(job);
    JobRecord jobRecord = pipelineManager.getJob(pipelineId);
    assertEquals(job.getJobDisplayName(), jobRecord.getRootJobDisplayName());
    JobInfo jobInfo = waitUntilJobComplete(pipelineService, pipelineId);
    assertEquals("Shalom", jobInfo.getOutput());
    jobRecord = pipelineManager.getJob(pipelineId);
    assertEquals(job.getJobDisplayName(), jobRecord.getRootJobDisplayName());
    PipelineObjects pobjects = pipelineManager.queryFullPipeline(pipelineId);
    assertEquals(job.getJobDisplayName(), pobjects.getRootJob().getRootJobDisplayName());
  }

  @Test
  public void testJobInheritence() throws Exception {
    JobRunId pipelineId= pipelineService.startNewPipeline(new ConcreteJob());
    JobInfo jobInfo = waitUntilJobComplete(pipelineService, pipelineId);
    assertEquals("Shalom", jobInfo.getOutput());
  }

  @SuppressWarnings("serial")
  private static class FailedJob extends Job0<String> {

    @Override
    public Value<String> run() throws Exception {
      throw new RuntimeException("koko");
    }
  }

  @Test
 public void testJobFailure() throws Exception {
    //fail after 1 attempt, to speed up this test (although default is 3)
    JobRunId pipelineId= pipelineService.startNewPipeline(new FailedJob(), new JobSetting.MaxAttempts(1));
    JobInfo jobInfo = waitUntilJobComplete(pipelineService, pipelineId);
    assertEquals(JobInfo.State.STOPPED_BY_ERROR, jobInfo.getJobState());
    assertEquals("koko", jobInfo.getException().getMessage());
    assertNull(jobInfo.getOutput());
  }

  @Test
  public void testReturnValue() throws Exception {
    // Testing that return value from parent is always after all children complete
    // which is not the case right now. This this *SHOULD* change after we fix
    // it, as fixing it should cause a dead-lock.
    // see b/12249138
    JobRunId pipelineId= pipelineService.startNewPipeline(new ReturnValueParentJob());
    String value = waitForJobToComplete(pipelineService, pipelineId);
    assertEquals("bla", value);
    ReturnValueParentJob.latch1.countDown();
    waitUntilTaskQueueIsEmpty(getTaskQueue());
    ReturnValueParentJob.latch2.await();
  }

  @SuppressWarnings("serial")
  private static class ReturnValueParentJob extends Job0<String> {

    static CountDownLatch latch1 = new CountDownLatch(1);
    static CountDownLatch latch2 = new CountDownLatch(1);

    @Override
    public Value<String> run() throws Exception {
      futureCall(new ReturnedValueChildJob());
      return immediate("bla");
    }
  }

  @SuppressWarnings("serial")
  private static class ReturnedValueChildJob extends Job0<Void> {
    @Override
    public Value<Void> run() throws Exception {
      ReturnValueParentJob.latch1.await();
      ReturnValueParentJob.latch2.countDown();
      return null;
    }
  }

  @Test
  public void testSubmittingPromisedValueMoreThanOnce(DatastoreOptions datastoreOptions) throws Exception {
    JobRunId pipelineId= pipelineService.startNewPipeline(new SubmitPromisedParentJob(datastoreOptions));
    String value = waitForJobToComplete(pipelineService, pipelineId);
    assertEquals("2", value);
  }

  @SuppressWarnings("serial")

  @AllArgsConstructor
  private static class SubmitPromisedParentJob extends Job0<String> {

    final DatastoreOptions datastoreOptions;
    @Override
    public Value<String> run() throws Exception {
      PromisedValue<String> promise = newPromise();
      FutureValue<Void> child1 = futureCall(
          new FillPromiseJob(datastoreOptions), immediate("1"), immediate(promise.getHandle()));
      FutureValue<Void> child2 = futureCall(
          new FillPromiseJob(datastoreOptions), immediate("2"), immediate(promise.getHandle()), waitFor(child1));
      FutureValue<String> child3 = futureCall(new ReadPromiseJob(), promise, waitFor(child2));
      // If we return promise directly then the value would be "1" rather than "2", see b/12216307
      //return promise;
      return child3;
    }
  }

  @SuppressWarnings("serial")
  private static class ReadPromiseJob extends Job1<String, String> {
    @Override
    public Value<String> run(String value) {
      return immediate(value);
    }
  }

  @SuppressWarnings("serial")
  @AllArgsConstructor
  private static class FillPromiseJob extends Job2<Void, String, String> {

    final DatastoreOptions datastoreOptions;

    @Override
    public Value<Void> run(String value, String handle) throws Exception {
      getPipelineService().submitPromisedValue(handle, value);
      return null;
    }
  }

  @Test
  public void testCancelPipeline() throws Exception {
    JobRunId pipelineId= pipelineService.startNewPipeline(new HandleExceptionParentJob(),
        new JobSetting.BackoffSeconds(1), new JobSetting.BackoffFactor(1),
        new JobSetting.MaxAttempts(2));
    JobInfo jobInfo = pipelineService.getJobInfo(pipelineId);
    assertEquals(State.RUNNING, jobInfo.getJobState());
    HandleExceptionChild2Job.childLatch1.await();
    pipelineService.cancelPipeline(pipelineId);
    HandleExceptionChild2Job.childLatch2.countDown();
    waitUntilTaskQueueIsEmpty(getTaskQueue());
    jobInfo = pipelineService.getJobInfo(pipelineId);
    assertEquals(State.CANCELED_BY_REQUEST, jobInfo.getJobState());
    assertNull(jobInfo.getOutput());
    assertTrue(HandleExceptionParentJob.child0.get());
    assertTrue(HandleExceptionParentJob.child1.get());
    assertTrue(HandleExceptionParentJob.child2.get());
    assertFalse(HandleExceptionParentJob.child3.get());
    assertFalse(HandleExceptionParentJob.child4.get());
    // Unexpected callbacks (should be fixed after b/12250957)

    assertTrue(HandleExceptionParentJob.child3Cancel.get()); // job not started
    assertTrue(HandleExceptionParentJob.child4Cancel.get()); // job not started

    // expected callbacks
    assertFalse(HandleExceptionParentJob.child0Cancel.get()); // job already finalized
    assertTrue(HandleExceptionParentJob.parentCancel.get());
    assertTrue(HandleExceptionParentJob.child1Cancel.get());
    assertTrue(HandleExceptionParentJob.child2Cancel.get()); // after job run, but not finalized
  }

  @SuppressWarnings("serial")
  private static class HandleExceptionParentJob extends Job0<String> {

    private static AtomicBoolean parentCancel = new AtomicBoolean();
    private static AtomicBoolean child0 = new AtomicBoolean();
    private static AtomicBoolean child0Cancel = new AtomicBoolean();
    private static AtomicBoolean child1 = new AtomicBoolean();
    private static AtomicBoolean child1Cancel = new AtomicBoolean();
    private static AtomicBoolean child2 = new AtomicBoolean();
    private static AtomicBoolean child2Cancel = new AtomicBoolean();
    private static AtomicBoolean child3 = new AtomicBoolean();
    private static AtomicBoolean child3Cancel = new AtomicBoolean();
    private static AtomicBoolean child4 = new AtomicBoolean();
    private static AtomicBoolean child4Cancel = new AtomicBoolean();

    @Override
    public Value<String> run() {
      FutureValue<String> child0 = futureCall(new HandleExceptionChild0Job());
      FutureValue<String> child1 = futureCall(new HandleExceptionChild1Job(), waitFor(child0));
      FutureValue<String> child2 = futureCall(new HandleExceptionChild3Job(), waitFor(child1));
      return futureCall(new HandleExceptionChild4Job(), child1, child2);
    }

    @SuppressWarnings("unused")
    public Value<String> handleException(CancellationException ex) throws Exception {
      HandleExceptionParentJob.parentCancel.set(true);
      return immediate("should not be used");
    }
  }

  @SuppressWarnings("serial")
  private static class HandleExceptionChild0Job extends Job0<String> {
    @Override
    public Value<String> run() {
      HandleExceptionParentJob.child0.set(true);
      return immediate("1");
    }

    @SuppressWarnings("unused")
    public Value<String> handleException(CancellationException ex) {
      HandleExceptionParentJob.child0Cancel.set(true);
      return null;
    }
  }

  @SuppressWarnings("serial")
  private static class HandleExceptionChild1Job extends Job0<String> {

    @Override
    public Value<String> run() {
      HandleExceptionParentJob.child1.set(true);
      return futureCall(new HandleExceptionChild2Job());
    }

    @SuppressWarnings("unused")
    public Value<String> handleException(CancellationException ex) {
      HandleExceptionParentJob.child1Cancel.set(true);
      return null;
    }
  }

  @SuppressWarnings("serial")
  private static class HandleExceptionChild2Job extends Job0<String> {

    private static CountDownLatch childLatch1 = new CountDownLatch(1);
    private static CountDownLatch childLatch2 = new CountDownLatch(1);

    @Override
    public Value<String> run() throws InterruptedException {
      HandleExceptionParentJob.child2.set(true);
      childLatch1.countDown();
      childLatch2.await();
      Thread.sleep(1000);
      return immediate("1");
    }

    @SuppressWarnings("unused")
    public Value<String> handleException(CancellationException ex) {
      HandleExceptionParentJob.child2Cancel.set(true);
      return null;
    }
  }

  @SuppressWarnings("serial")
  private static class HandleExceptionChild3Job extends Job0<String> {

    @Override
    public Value<String> run() {
      HandleExceptionParentJob.child3.set(true);
      return immediate("2");
    }

    @SuppressWarnings("unused")
    public Value<String> handleException(CancellationException ex) {
      HandleExceptionParentJob.child3Cancel.set(true);
      return null;
    }
  }

  @SuppressWarnings("serial")
  private static class HandleExceptionChild4Job extends Job2<String, String, String> {

    @Override
    public Value<String> run(String str1, String str2) {
      HandleExceptionParentJob.child4.set(true);
      return immediate(str1 + str2);
    }

    @SuppressWarnings("unused")
    public Value<String> handleException(CancellationException ex) {
      HandleExceptionParentJob.child4Cancel.set(true);
      return immediate("not going to be used");
    }
  }

  @Test
  public void testImmediateChild() throws Exception {
    // This is also testing inheritance of statusConsoleUrl.
    JobRunId pipelineId= pipelineService.startNewPipeline(new Returns5FromChildJob(), largeValue);
    Integer five = waitForJobToComplete(pipelineService, pipelineId);
    assertEquals(5, five.intValue());
  }

  @Test
  public void testPromisedValue(DatastoreOptions datastoreOptions) throws Exception {
    JobRunId pipelineId= pipelineService.startNewPipeline(new FillPromisedValueJob(datastoreOptions));
    String helloWorld = waitForJobToComplete(pipelineService, pipelineId);
    assertEquals("hello world", helloWorld);
  }

  @Test
  public void testDelayedValueInSlowJob() throws Exception {
    JobRunId pipelineId= pipelineService.startNewPipeline(new UsingDelayedValueInSlowJob());
    String hello = waitForJobToComplete(pipelineService, pipelineId);
    assertEquals("I am delayed", hello);
  }

  @SuppressWarnings("serial")
  static class Temp<T extends Serializable> implements Serializable {

    private final T value;

    Temp(T value) {
      this.value = value;
    }

    T getValue() {
      return value;
    }
  }

  @SuppressWarnings("serial")
  @RequiredArgsConstructor
  private static class FillPromisedValueJob extends Job0<String> {

    final DatastoreOptions datastoreOptions;

    @Override
    public Value<String> run() {
      PromisedValue<List<Temp<String>>> ps = newPromise();
      futureCall(new PopulatePromisedValueJob(datastoreOptions), immediate(ps.getHandle()));
      return futureCall(new ConsumePromisedValueJob(), ps);
    }
  }

  @AllArgsConstructor
  @SuppressWarnings("serial")
  private static class PopulatePromisedValueJob extends Job1<Void, String> {

    final DatastoreOptions datastoreOptions;
    @Override
    public Value<Void> run(String handle) throws NoSuchObjectException, OrphanedObjectException {

      List<Temp<String>> list = new ArrayList<>();
      list.add(new Temp<>("hello"));
      list.add(new Temp<>(" "));
      list.add(new Temp<>("world"));
      getPipelineService().submitPromisedValue(handle, list);
      return null;
    }
  }

  @SuppressWarnings("serial")
  private static class ConsumePromisedValueJob extends Job1<String, List<Temp<String>>> {

    @Override
    public Value<String> run(List<Temp<String>> values) {
      String value = "";
      for (Temp<String> temp : values) {
        value += temp.getValue();
      }
      return immediate(value);
    }
  }

  @SuppressWarnings("serial")
  private static class StrJob<T extends Serializable> extends Job1<String, T> {

    @Override
    public Value<String> run(T obj) {
      return immediate(obj == null ? "null" : obj.toString());
    }
  }

  @SuppressWarnings("serial")
  private static class UsingDelayedValueInSlowJob extends Job0<String> {

    @Override
    public Value<String> run() throws InterruptedException {
      Value<Void> delayedValue = newDelayedValue(1);
      Thread.sleep(3000);
      // We would like to validate that the delay will work even when used after
      // its delayed value. It used to fail before, b/12081152, but now it should
      // pass (and semantic of the delay was changed, so delay value starts only
      // after this run method completes.
      return futureCall(new StrJob<>(), immediate("I am delayed"), waitFor(delayedValue));
    }
  }

  @SuppressWarnings("serial")
  private static class Returns5FromChildJob extends Job1<Integer, long[]> {
    @Override
    public Value<Integer> run(long[] bytes) {
      setStatusConsoleUrl("my-console-url");
      assertTrue(Arrays.equals(largeValue, bytes));
      FutureValue<Integer> lengthJob = futureCall(new LengthJob(), immediate(bytes));
      return futureCall(
          new IdentityJob(bytes), immediate(5), lengthJob, new StatusConsoleUrl(null));
    }
  }

  @SuppressWarnings("serial")
  private static class IdentityJob extends Job2<Integer, Integer, Integer> {

    private final long[] bytes;

    public IdentityJob(long[] bytes) {
      this.bytes = bytes;
    }

    @Override
    public Value<Integer> run(Integer param1, Integer length) {
      assertNull(getStatusConsoleUrl());
      assertEquals(largeValue.length, length.intValue());
      assertTrue(Arrays.equals(largeValue, bytes));
      return immediate(param1);
    }
  }

  @SuppressWarnings("serial")
  private static class LengthJob extends Job1<Integer, long[]> {
    @Override
    public Value<Integer> run(long[] bytes) {
      assertEquals("my-console-url", getStatusConsoleUrl());
      assertTrue(Arrays.equals(largeValue, bytes));
      return immediate(bytes.length);
    }
  }
}
