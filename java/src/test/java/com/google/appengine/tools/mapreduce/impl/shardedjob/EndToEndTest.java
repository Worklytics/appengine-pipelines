// Copyright 2012 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce.impl.shardedjob;

import static com.google.appengine.tools.mapreduce.impl.shardedjob.Status.StatusCode.DONE;
import static com.google.appengine.tools.mapreduce.impl.shardedjob.Status.StatusCode.RUNNING;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;


import com.google.appengine.tools.mapreduce.EndToEndTestCase;
import com.google.appengine.tools.pipeline.PipelineService;
import com.google.appengine.tools.pipeline.TestUtils;
import com.google.cloud.datastore.*;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Iterator;
import java.util.List;
import java.util.Random;

/**
 * @author ohler@google.com (Christian Ohler)
 */
public class EndToEndTest extends EndToEndTestCase {

  private ShardedJobSettings settings;

  @BeforeEach
  public void initSettings() throws Exception {
    settings = new ShardedJobSettings.Builder().build();
  }

  @Test
  public void testSimpleJob() throws Exception {
    List<TestTask> tasks = ImmutableList.of(
        new TestTask(0, 5, 1, 3),
        new TestTask(1, 5, 10, 1),
        new TestTask(2, 5, 100, 1),
        new TestTask(3, 5, 1000, 2),
        new TestTask(4, 5, 10000, 4));
    int expectedResult = 42113;

    String jobId = "job1";
    assertNull(getPipelineRunner().getJobState(jobId));

    assertEquals(0, getTasks().size());
    TestController controller = new TestController(getDatastore().getOptions(), expectedResult, getPipelineService(), false);
    getPipelineOrchestrator().startJob(jobId, tasks, controller, settings);
    assertEquals(new Status(RUNNING), getPipelineRunner().getJobState(jobId).getStatus());
    // 5 initial tasks
    assertEquals(5, getTasks().size());
    assertEquals(5, getPipelineRunner().getJobState(jobId).getActiveTaskCount());
    assertEquals(5, getPipelineRunner().getJobState(jobId).getTotalTaskCount());

    // Starting again should not add any tasks.
    controller = new TestController(getDatastore().getOptions(), expectedResult, getPipelineService(), false);
    getPipelineOrchestrator().startJob(jobId, tasks, controller, settings);
    assertEquals(new Status(RUNNING), getPipelineRunner().getJobState(jobId).getStatus());
    assertEquals(5, getTasks().size());
    assertEquals(5, getPipelineRunner().getJobState(jobId).getActiveTaskCount());
    assertEquals(5, getPipelineRunner().getJobState(jobId).getTotalTaskCount());

    executeTasksUntilEmpty();

    ShardedJobState state = getPipelineRunner().getJobState(jobId);
    assertEquals(new Status(DONE), state.getStatus());
    assertEquals(0, state.getActiveTaskCount());
    assertEquals(5, state.getTotalTaskCount());
  }


  private static class TestController1 extends TestController {
    private static final long serialVersionUID = 8297824686146604329L;

    public TestController1(DatastoreOptions datastoreOptions, int expectedResult, PipelineService pipelineService) {
      super(datastoreOptions, expectedResult, pipelineService, false);
    }

    @Override
    public void completed(Iterator<TestTask> results) {
      Datastore datastore = getDatastoreOptions().getService();
      //q: why is this 3? original test was 7 ... something about how the async processing working?  or something else?
      // at this point, there is ONLY MR-stuff has been created (MR-ShardedJob, MR-IncrementalTask, MR-ShardRetryState);
      // and NOT pipeline- entities yet.
      assertEquals(3, TestUtils.countDatastoreEntities(datastore));
      //assertEquals(7, TestUtils.countDatastoreEntities(datastore));
      super.completed(results);
    }
  }

  @Test
  public void testJobFinalization() throws Exception {
    byte[] bytes = new byte[1024 * 1024];
    new Random().nextBytes(bytes);
    TestTask task = new TestTask(0, 1, 1, 1, bytes);
    String jobId = "job1";
    TestController controller = new TestController1( getDatastore().getOptions(), 1, getPipelineService());
    getPipelineOrchestrator().startJob(jobId, ImmutableList.of(task), controller, settings);
    assertEquals(new Status(RUNNING), getPipelineRunner().getJobState(jobId).getStatus());
    executeTasksUntilEmpty();
    ShardedJobState state = getPipelineRunner().getJobState(jobId);
    assertEquals(new Status(DONE), state.getStatus());
    IncrementalTaskState<IncrementalTask> it = Iterators.getOnlyElement(getPipelineRunner().lookupTasks(state));
    assertNull(((TestTask) it.getTask()).getPayload());

    assertEquals(2, TestUtils.countDatastoreEntities(getDatastore()));
  }

  // TODO(ohler): Test idempotence of startJob() in more depth, especially in
  // the case of errors (incomplete initializations).
  @Test
  public void testNoTasks() throws Exception {
    String jobId = "job1";
    assertNull(getPipelineRunner().getJobState(jobId));
    TestController controller = new TestController(getDatastore().getOptions(), 0, getPipelineService(), false);
    getPipelineOrchestrator().startJob(jobId, ImmutableList.of(), controller, settings);
    ShardedJobState state = getPipelineRunner().getJobState(jobId);
    assertEquals(new Status(DONE), state.getStatus());
    assertEquals(0, state.getActiveTaskCount());
    assertEquals(0, state.getTotalTaskCount());
  }
}
