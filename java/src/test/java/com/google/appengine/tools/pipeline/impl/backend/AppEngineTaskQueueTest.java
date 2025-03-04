package com.google.appengine.tools.pipeline.impl.backend;

import com.google.appengine.tools.pipeline.DatastoreExtension;
import com.google.cloud.datastore.Key;
import com.google.appengine.api.taskqueue.TaskHandle;
import com.google.appengine.tools.development.testing.LocalModulesServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalServiceTestHelper;
import com.google.appengine.tools.development.testing.LocalTaskQueueTestConfig;
import com.google.appengine.tools.pipeline.impl.QueueSettings;
import com.google.appengine.tools.pipeline.impl.tasks.RunJobTask;
import com.google.appengine.tools.pipeline.impl.tasks.Task;
import com.google.appengine.tools.pipeline.impl.util.GUIDGenerator;

import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.*;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author tkaitchuck
 */
@ExtendWith(DatastoreExtension.class)
public class AppEngineTaskQueueTest {

  private LocalServiceTestHelper helper;

  @BeforeEach
  public void setUp() throws Exception {
    LocalTaskQueueTestConfig taskQueueConfig = new LocalTaskQueueTestConfig();
    taskQueueConfig.setDisableAutoTaskExecution(true);
    taskQueueConfig.setShouldCopyApiProxyEnvironment(true);
    helper = new LocalServiceTestHelper(taskQueueConfig, new LocalModulesServiceTestConfig());
    helper.setUp();
  }

  @AfterEach
  public void tearDown() throws Exception {
    helper.tearDown();
  }

  @Test
  public void testEnqueueSingleTask() {
    AppEngineTaskQueue queue = new AppEngineTaskQueue();
    Task task = createTask();
    List<PipelineTaskQueue.TaskReference> handles = queue.addToQueue(Collections.singletonList(task));

    assertEquals(1, handles.size());
    assertEquals(task.getName(), handles.get(0).getTaskName());

    //behavior change; 2nd enqueue of same task now returns it again, even if duplicated
    handles = queue.addToQueue(Collections.singletonList(task));
    assertEquals(1, handles.size());
  }

  @Test
  public void testEnqueueBatchTasks() {
    AppEngineTaskQueue queue = new AppEngineTaskQueue();
    List<Task> tasks = new ArrayList<>(AppEngineTaskQueue.MAX_TASKS_PER_ENQUEUE);
    for (int i = 0; i < AppEngineTaskQueue.MAX_TASKS_PER_ENQUEUE; i++) {
      Task task = createTask();
      tasks.add(task);
    }
    List<PipelineTaskQueue.TaskReference> handles = queue.addToQueue(tasks);
    assertEquals(AppEngineTaskQueue.MAX_TASKS_PER_ENQUEUE, handles.size());
    for (int i = 0; i < AppEngineTaskQueue.MAX_TASKS_PER_ENQUEUE; i++) {
      assertEquals(tasks.get(i).getName(), handles.get(i).getTaskName());
    }

    handles = queue.addToQueue(tasks);
    assertEquals(tasks.size(), handles.size());
  }

  @Test
  public void testEnqueueLargeBatchTasks() {
    AppEngineTaskQueue queue = new AppEngineTaskQueue();
    int batchSize = AppEngineTaskQueue.MAX_TASKS_PER_ENQUEUE * 2 + 10;
    List<Task> tasks = new ArrayList<>(batchSize);
    for (int i = 0; i < batchSize; i++) {
      Task task = createTask();
      tasks.add(task);
    }
    List<PipelineTaskQueue.TaskReference> handles = queue.addToQueue(tasks);
    assertEquals(tasks.size(), handles.size());
    for (int i = 0; i < tasks.size(); i++) {
      assertEquals(tasks.get(i).getName(), handles.get(i).getTaskName());
    }

    // NOTE: this is behavior change from legacy GAE pipelines; it used to NOT return handles of anything that had be enqueued previously
   handles = queue.addToQueue(tasks);
    assertEquals(tasks.size(), handles.size());
    for (int i = 0; i < tasks.size(); i++) {
      assertEquals(tasks.get(i).getName(), handles.get(i).getTaskName());
    }
  }

  @Test
  public void testEnqueueBatchTwoStages() {
    AppEngineTaskQueue queue = new AppEngineTaskQueue();
    int batchSize = AppEngineTaskQueue.MAX_TASKS_PER_ENQUEUE * 2;
    List<Task> tasks = new ArrayList<>(batchSize);
    for (int i = 0; i < batchSize; i++) {
      Task task = createTask();
      tasks.add(task);
    }

    int firstBatchSize = AppEngineTaskQueue.MAX_TASKS_PER_ENQUEUE;
    List<PipelineTaskQueue.TaskReference> handles = queue.addToQueue(tasks.subList(0, firstBatchSize));

    assertEquals(firstBatchSize, handles.size());
    for (int i = 0; i < firstBatchSize; i++) {
      assertEquals(tasks.get(i).getName(), handles.get(i).getTaskName());
    }

    handles = queue.addToQueue(tasks);

    assertEquals(tasks.size(), handles.size());
    Set<String> names = handles.stream().map(PipelineTaskQueue.TaskReference::getTaskName).collect(Collectors.toCollection(HashSet::new));
    for (int i = 0; i < tasks.size(); i++) {
      names.remove(tasks.get(i).getName());
    }
    assertEquals(0, names.size()); //everything in names has 1:1 match in tasks
  }

  private Task createTask() {
    String name = GUIDGenerator.nextGUID();
    Key key = Key.newBuilder("test-project", "testType", name).build();
    Task task = new RunJobTask(key, new QueueSettings().setOnServiceVersion("m1"));
    return task;
  }
}
