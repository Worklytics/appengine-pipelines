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

package com.google.appengine.tools.pipeline.impl.backend;

import com.github.rholder.retry.*;

import com.google.appengine.api.modules.ModulesException;
import com.google.appengine.api.modules.ModulesService;
import com.google.appengine.api.modules.ModulesServiceFactory;
import com.google.appengine.api.taskqueue.Queue;
import com.google.appengine.api.taskqueue.QueueConstants;
import com.google.appengine.api.taskqueue.QueueFactory;
import com.google.appengine.api.taskqueue.TaskAlreadyExistsException;
import com.google.appengine.api.taskqueue.TaskHandle;
import com.google.appengine.api.taskqueue.TaskOptions;
import com.google.appengine.tools.pipeline.impl.QueueSettings;
import com.google.appengine.tools.pipeline.impl.servlets.TaskHandler;
import com.google.appengine.tools.pipeline.impl.tasks.Task;
import com.google.apphosting.api.ApiProxy;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

/**
 * Encapsulates access to the App Engine Task Queue API
 *
 * @author rudominer@google.com (Mitch Rudominer)
 *
 */
public class AppEngineTaskQueue implements PipelineTaskQueue {

  private static final Logger logger = Logger.getLogger(AppEngineTaskQueue.class.getName());

  static final int MAX_TASKS_PER_ENQUEUE = QueueConstants.maxTasksPerAdd();

  //approximates default Retry policy from GAE GCS lib RetryParams class, which this pipelines lib was originally
  // coupled to
  private static final Retryer<String> retryer = RetryerBuilder.<String>newBuilder()
          .retryIfExceptionOfType(ModulesException.class)
          .withStopStrategy(StopStrategies.stopAfterAttempt(6))
          .withWaitStrategy(WaitStrategies.incrementingWait(1000L, TimeUnit.MILLISECONDS, 1000L, TimeUnit.MILLISECONDS))
          .build();

  final String taskHandlerUrl;

  public AppEngineTaskQueue() {
    this.taskHandlerUrl = TaskHandler.handleTaskUrl();
  }

  @Override
  public void enqueue(Task task) {
    logger.finest("Enqueueing: " + task);
    TaskOptions taskOptions = toTaskOptions(task);
    Queue queue = getQueue(task.getQueueSettings().getOnQueue());
    try {
      queue.add(taskOptions);
    } catch (TaskAlreadyExistsException ignore) {
      // ignore
    }
  }

  private static Queue getQueue(String queueName) {
    if (queueName == null) {
      Map<String, Object> attributes = ApiProxy.getCurrentEnvironment().getAttributes();
      queueName = (String) attributes.get(TaskHandler.TASK_QUEUE_NAME_HEADER);
    }
    //good idea? seems needed to make tests pass in CI now; risk is that if someone uses a queue named 'default', but
    // it's not the *default* queue
    if (Objects.equals(queueName, "default")) {
      queueName = null;
    }
    return queueName == null ? QueueFactory.getDefaultQueue() : QueueFactory.getQueue(queueName);
  }

  @Override
  public void enqueue(final Collection<Task> tasks) {
    addToQueue(tasks);
  }

  //VisibleForTesting
  List<TaskHandle> addToQueue(final Collection<Task> tasks) {
    List<TaskHandle> handles = new ArrayList<>();
    Map<String, List<TaskOptions>> queueNameToTaskOptions = new HashMap<>();
    for (Task task : tasks) {
      logger.finest("Enqueueing: " + task);
      String queueName = task.getQueueSettings().getOnQueue();
      TaskOptions taskOptions = toTaskOptions(task);


      // seen in logs : "Negative countdown is not allowed"
      if (taskOptions.getCountdownMillis() != null && taskOptions.getCountdownMillis() < 0) {
        logger.warning("Task countdownMillis is  " + taskOptions.getCountdownMillis() + ". Setting to 0 to avoid error.");
        taskOptions.countdownMillis(0);
      }
      Instant now = Instant.now();
      if (taskOptions.getEtaMillis() != null && taskOptions.getEtaMillis() <= now.toEpochMilli()) {
        logger.warning("Task etaMillis is  " + (now.toEpochMilli() - taskOptions.getEtaMillis()) + " before now. Setting to now + 30s to avoid error.");
        taskOptions.etaMillis(now.toEpochMilli() + Duration.ofSeconds(30) .toMillis());
      }

      List<TaskOptions> taskOptionsList = queueNameToTaskOptions.get(queueName);
      if (taskOptionsList == null) {
        taskOptionsList = new ArrayList<>();
        queueNameToTaskOptions.put(queueName, taskOptionsList);
      }
      taskOptionsList.add(taskOptions);
    }
    for (Map.Entry<String, List<TaskOptions>> entry : queueNameToTaskOptions.entrySet()) {
      Queue queue = getQueue(entry.getKey());
      handles.addAll(addToQueue(queue, entry.getValue()));
    }
    return handles;
  }

  private List<TaskHandle> addToQueue(Queue queue, List<TaskOptions> tasks) {
    int limit = tasks.size();
    int start = 0;
    List<Future<List<TaskHandle>>> futures = new ArrayList<>(limit / MAX_TASKS_PER_ENQUEUE + 1);
    while (start < limit) {
      int end = Math.min(limit, start + MAX_TASKS_PER_ENQUEUE);
      futures.add(queue.addAsync(tasks.subList(start, end)));
      start = end;
    }

    List<TaskHandle> taskHandles = new ArrayList<>(limit);
    for (Future<List<TaskHandle>> future : futures) {
      try {
        taskHandles.addAll(future.get());
      } catch (InterruptedException e) {
        logger.throwing("AppEngineTaskQueue", "addToQueue", e);
        Thread.currentThread().interrupt();
        throw new RuntimeException("addToQueue failed", e);
      } catch (ExecutionException e) {
        if (e.getCause() instanceof TaskAlreadyExistsException) {
          // Ignore, as that suggests all non-duplicate tasks were sent successfully
        } else {
          throw new RuntimeException("addToQueue failed", e.getCause());
        }
      }
    }
    return taskHandles;
  }

  private TaskOptions toTaskOptions(Task task) {
    final QueueSettings queueSettings = task.getQueueSettings();

    TaskOptions taskOptions = TaskOptions.Builder.withUrl(taskHandlerUrl);

    String versionHostname;

    //annoyingly, guava Retryer throws Exceptions, rather than RuntimeExceptions
    try {
      versionHostname = retryer.call(() -> {
        //TODO: Modules are now called 'Services', but there's no "ServicesService" in GAE SDK, afaik
        ModulesService service = ModulesServiceFactory.getModulesService();
        String module = queueSettings.getOnService();
        String version = queueSettings.getOnServiceVersion();
        if (module == null) {
          module = service.getCurrentModule();
          version = service.getCurrentVersion();
        }
        return service.getVersionHostname(module, version);
      });
    } catch (ExecutionException e) {
      //avoid excessive wrapping; re-throw the underlying cause
      throw new RuntimeException(e.getCause());
    } catch (RetryException e) {
      throw new RuntimeException(e);
    }

    taskOptions.header("Host", versionHostname);


    Long delayInSeconds = queueSettings.getDelayInSeconds();
    if (null != delayInSeconds) {
      taskOptions.countdownMillis(delayInSeconds * 1000L);
      queueSettings.setDelayInSeconds(null);
    }
    addProperties(taskOptions, task.toProperties());
    String taskName = task.getName();
    if (null != taskName) {
      taskOptions.taskName(taskName);
    }
    return taskOptions;
  }

  private static void addProperties(TaskOptions taskOptions, Properties properties) {
    for (String paramName : properties.stringPropertyNames()) {
      String paramValue = properties.getProperty(paramName);
      taskOptions.param(paramName, paramValue);
    }
  }
}
