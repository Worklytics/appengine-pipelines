// Copyright 2012 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce.impl.shardedjob;

import static com.google.appengine.tools.mapreduce.impl.shardedjob.Status.StatusCode.DONE;
import static com.google.appengine.tools.mapreduce.impl.shardedjob.Status.StatusCode.RUNNING;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;


import com.google.appengine.tools.mapreduce.EndToEndTestCase;
import com.google.appengine.tools.mapreduce.di.DaggerDefaultMapReduceContainer;
import com.google.appengine.tools.pipeline.impl.util.DIUtil;
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
    assertNull(getShardedJobService().getJobState(getDatastore(), jobId));

    assertEquals(0, getTasks().size());
    TestController controller = new TestController(getDatastore().getOptions(), expectedResult);
    getShardedJobService().startJob(getDatastore(), jobId, tasks, controller, settings);
    assertEquals(new Status(RUNNING), getShardedJobService().getJobState(getDatastore(), jobId).getStatus());
    // 5 initial tasks
    assertEquals(5, getTasks().size());
    assertEquals(5, getShardedJobService().getJobState(getDatastore(), jobId).getActiveTaskCount());
    assertEquals(5, getShardedJobService().getJobState(getDatastore(), jobId).getTotalTaskCount());

    // Starting again should not add any tasks.
    controller = new TestController(getDatastore().getOptions(), expectedResult);
    getShardedJobService().startJob(getDatastore(), jobId, tasks, controller, settings);
    assertEquals(new Status(RUNNING), getShardedJobService().getJobState(getDatastore(), jobId).getStatus());
    assertEquals(5, getTasks().size());
    assertEquals(5, getShardedJobService().getJobState(getDatastore(), jobId).getActiveTaskCount());
    assertEquals(5, getShardedJobService().getJobState(getDatastore(), jobId).getTotalTaskCount());

    executeTasksUntilEmpty();

    ShardedJobState state = getShardedJobService().getJobState(getDatastore(), jobId);
    assertEquals(new Status(DONE), state.getStatus());
    assertEquals(0, state.getActiveTaskCount());
    assertEquals(5, state.getTotalTaskCount());
  }


  private static class TestController1 extends TestController {
    private static final long serialVersionUID = 8297824686146604329L;

    public TestController1(DatastoreOptions datastoreOptions, int expectedResult) {
      super(datastoreOptions, expectedResult);
    }

    @Override
    public void completed(Iterator<TestTask> results) {
      Datastore datastore = getDatastoreOptions().getService();
      Query<Entity> query = Query.newEntityQueryBuilder().setKind("mr-entity").build();
      QueryResults<Entity> entities = datastore.run(query);
      assertEquals(7, Iterators.size(entities));
      super.completed(results);
    }
  }

  @Test
  public void testJobFinalization() throws Exception {
    byte[] bytes = new byte[1024 * 1024];
    new Random().nextBytes(bytes);
    TestTask task = new TestTask(0, 1, 1, 1, bytes);
    String jobId = "job1";
    TestController controller = new TestController1( getDatastore().getOptions(), 1);
    getShardedJobService().startJob(getDatastore(), jobId, ImmutableList.of(task), controller, settings);
    assertEquals(new Status(RUNNING), getShardedJobService().getJobState(getDatastore(), jobId).getStatus());
    executeTasksUntilEmpty();
    ShardedJobState state = getShardedJobService().getJobState(getDatastore(), jobId);
    assertEquals(new Status(DONE), state.getStatus());
    IncrementalTaskState<IncrementalTask> it = Iterators.getOnlyElement(getShardedJobService().lookupTasks(getDatastore().newTransaction(), state));
    assertNull(((TestTask) it.getTask()).getPayload());

    Query<Entity> query = Query.newEntityQueryBuilder().setKind("mr-entity").build();
    QueryResults<Entity> results = getDatastore().run(query);
    assertEquals(2, Iterators.size(results));
  }

  // TODO(ohler): Test idempotence of startJob() in more depth, especially in
  // the case of errors (incomplete initializations).
  @Test
  public void testNoTasks() throws Exception {
    String jobId = "job1";
    assertNull(getShardedJobService().getJobState(getDatastore(), jobId));
    TestController controller = new TestController(getDatastore().getOptions(), 0);
    getShardedJobService().startJob(getDatastore(), jobId, ImmutableList.of(), controller, settings);
    ShardedJobState state = getShardedJobService().getJobState(getDatastore(), jobId);
    assertEquals(new Status(DONE), state.getStatus());
    assertEquals(0, state.getActiveTaskCount());
    assertEquals(0, state.getTotalTaskCount());
  }
}
