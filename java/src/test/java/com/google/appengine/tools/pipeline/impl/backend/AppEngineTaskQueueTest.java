package com.google.appengine.tools.pipeline.impl.backend;

import com.google.appengine.tools.pipeline.DatastoreExtension;
import com.google.cloud.datastore.Key;
import com.google.appengine.tools.development.testing.LocalServiceTestHelper;
import com.google.appengine.tools.development.testing.LocalTaskQueueTestConfig;
import com.google.appengine.tools.pipeline.impl.QueueSettings;
import com.google.appengine.tools.pipeline.impl.tasks.RunJobTask;
import com.google.appengine.tools.pipeline.impl.tasks.PipelineTask;
import com.google.appengine.tools.pipeline.impl.util.GUIDGenerator;

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

  private AppEngineServicesService appEngineServicesService;
  private LocalServiceTestHelper helper;
  private AppEngineTaskQueue queue;

  @BeforeEach
  public void setUp() throws Exception {
    LocalTaskQueueTestConfig taskQueueConfig = new LocalTaskQueueTestConfig();
    taskQueueConfig.setDisableAutoTaskExecution(true);
    taskQueueConfig.setShouldCopyApiProxyEnvironment(true);
    helper = new LocalServiceTestHelper(taskQueueConfig);
    helper.setUp();

    appEngineServicesService = new AppEngineServicesService() {
      @Override
      public String getLocation() {
        return "us-central1";
      }

      @Override
      public String getDefaultService() {
        return "default";
      }

      @Override
      public String getDefaultVersion(String service) {
        return "1";
      }

      @Override
      public String getWorkerServiceHostName(String service, String version) {
        return "worker-dot-" + service + "-dot-" + version + ".localhost";
      }
    };

    queue = new AppEngineTaskQueue(appEngineServicesService);
  }

  @AfterEach
  public void tearDown() throws Exception {
    helper.tearDown();
  }

  @Test
  public void testEnqueueSingleTask() {
    PipelineTask pipelineTask = createTask();
    List<PipelineTaskQueue.TaskReference> handles = queue.addToQueue(Collections.singletonList(pipelineTask));

    assertEquals(1, handles.size());
    assertEquals(pipelineTask.getTaskName(), handles.get(0).getTaskName());

    //behavior change; 2nd enqueue of same task now returns it again, even if duplicated
    handles = queue.addToQueue(Collections.singletonList(pipelineTask));
    assertEquals(1, handles.size());
  }

  @Test
  public void testEnqueueBatchTasks() {
    List<PipelineTask> pipelineTasks = new ArrayList<>(AppEngineTaskQueue.MAX_TASKS_PER_ENQUEUE);
    for (int i = 0; i < AppEngineTaskQueue.MAX_TASKS_PER_ENQUEUE; i++) {
      PipelineTask pipelineTask = createTask();
      pipelineTasks.add(pipelineTask);
    }
    List<PipelineTaskQueue.TaskReference> handles = queue.addToQueue(pipelineTasks);
    assertEquals(AppEngineTaskQueue.MAX_TASKS_PER_ENQUEUE, handles.size());
    for (int i = 0; i < AppEngineTaskQueue.MAX_TASKS_PER_ENQUEUE; i++) {
      assertEquals(pipelineTasks.get(i).getTaskName(), handles.get(i).getTaskName());
    }

    handles = queue.addToQueue(pipelineTasks);
    assertEquals(pipelineTasks.size(), handles.size());
  }

  @Test
  public void testEnqueueLargeBatchTasks() {
    int batchSize = AppEngineTaskQueue.MAX_TASKS_PER_ENQUEUE * 2 + 10;
    List<PipelineTask> pipelineTasks = new ArrayList<>(batchSize);
    for (int i = 0; i < batchSize; i++) {
      PipelineTask pipelineTask = createTask();
      pipelineTasks.add(pipelineTask);
    }
    List<PipelineTaskQueue.TaskReference> handles = queue.addToQueue(pipelineTasks);
    assertEquals(pipelineTasks.size(), handles.size());
    for (int i = 0; i < pipelineTasks.size(); i++) {
      assertEquals(pipelineTasks.get(i).getTaskName(), handles.get(i).getTaskName());
    }

    // NOTE: this is behavior change from legacy GAE pipelines; it used to NOT return handles of anything that had be enqueued previously
   handles = queue.addToQueue(pipelineTasks);
    assertEquals(pipelineTasks.size(), handles.size());
    for (int i = 0; i < pipelineTasks.size(); i++) {
      assertEquals(pipelineTasks.get(i).getTaskName(), handles.get(i).getTaskName());
    }
  }

  @Test
  public void testEnqueueBatchTwoStages() {
    int batchSize = AppEngineTaskQueue.MAX_TASKS_PER_ENQUEUE * 2;
    List<PipelineTask> pipelineTasks = new ArrayList<>(batchSize);
    for (int i = 0; i < batchSize; i++) {
      PipelineTask pipelineTask = createTask();
      pipelineTasks.add(pipelineTask);
    }

    int firstBatchSize = AppEngineTaskQueue.MAX_TASKS_PER_ENQUEUE;
    List<PipelineTaskQueue.TaskReference> handles = queue.addToQueue(pipelineTasks.subList(0, firstBatchSize));

    assertEquals(firstBatchSize, handles.size());
    for (int i = 0; i < firstBatchSize; i++) {
      assertEquals(pipelineTasks.get(i).getTaskName(), handles.get(i).getTaskName());
    }

    handles = queue.addToQueue(pipelineTasks);

    assertEquals(pipelineTasks.size(), handles.size());
    Set<String> names = handles.stream().map(PipelineTaskQueue.TaskReference::getTaskName).collect(Collectors.toCollection(HashSet::new));
    for (int i = 0; i < pipelineTasks.size(); i++) {
      names.remove(pipelineTasks.get(i).getTaskName());
    }
    assertEquals(0, names.size()); //everything in names has 1:1 match in tasks
  }

  private PipelineTask createTask() {
    String name = GUIDGenerator.nextGUID();
    Key key = Key.newBuilder("test-project", "testType", name).build();
    PipelineTask pipelineTask = new RunJobTask(key, QueueSettings.builder().onServiceVersion("m1").build());
    return pipelineTask;
  }
}
