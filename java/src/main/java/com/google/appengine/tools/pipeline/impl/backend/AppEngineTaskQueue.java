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

import com.google.appengine.api.taskqueue.*;
import com.google.appengine.api.taskqueue.Queue;
import com.google.appengine.tools.pipeline.impl.QueueSettings;
import com.google.appengine.tools.pipeline.impl.servlets.TaskHandler;
import com.google.appengine.tools.pipeline.impl.tasks.PipelineTask;
import com.google.apphosting.api.ApiProxy;
import com.google.cloud.datastore.Transaction;
import com.google.common.base.Preconditions;
import lombok.RequiredArgsConstructor;
import lombok.extern.java.Log;

import javax.inject.Inject;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.stream.Collectors;

/**
 * Encapsulates access to the App Engine Task Queue API
 *
 * @author rudominer@google.com (Mitch Rudominer)
 *
 */
@RequiredArgsConstructor
@Log
public class AppEngineTaskQueue implements PipelineTaskQueue {

  static final int MAX_TASKS_PER_ENQUEUE = QueueConstants.maxTasksPerAdd();

  //approximates default Retry policy from GAE GCS lib RetryParams class, which this pipelines lib was originally
  // coupled to
  private static final Retryer<String> retryer = RetryerBuilder.<String>newBuilder()
          .retryIfExceptionOfType(TransientFailureException.class)
          .retryIfExceptionOfType(InternalFailureException.class)
          .withStopStrategy(StopStrategies.stopAfterAttempt(6))
          .withWaitStrategy(WaitStrategies.incrementingWait(1000L, TimeUnit.MILLISECONDS, 1000L, TimeUnit.MILLISECONDS))
          .build();

  final AppEngineEnvironment environment;
  final AppEngineServicesService servicesService;

  final String taskHandlerUrl;

  public AppEngineTaskQueue(AppEngineServicesService appEngineServicesService) {
    this.environment = new AppEngineStandardGen2();
    this.servicesService = appEngineServicesService;
    this.taskHandlerUrl = TaskHandler.handleTaskUrl();
  }

  @Inject
  public AppEngineTaskQueue(AppEngineEnvironment environment, AppEngineServicesService servicesService) {
    this.environment = environment;
    this.servicesService = servicesService;
    this.taskHandlerUrl = TaskHandler.handleTaskUrl();
  }

  @Override
  public void deleteTasks(Collection<TaskReference> taskReferences) {
    Map<String, List<String>> queueToTaskNames = taskReferences.stream()
      .collect(Collectors.groupingBy(TaskReference::getQueue, Collectors.mapping(TaskReference::getTaskName, Collectors.toList())));
    for (Map.Entry<String, List<String>> entry : queueToTaskNames.entrySet()) {
      Queue queue = getQueue(entry.getKey());
      
      try {
        entry.getValue().stream()
          .forEach(queue::deleteTaskAsync);
      } catch (RuntimeException ignored) {
        // weren't even bothering with this previously, so prob OK
        log.log(Level.WARNING, "Pipeline framework failed to delete tasks from queue", ignored);
      }
    }
  }

  static final int MAX_ENQUEUE_ATTEMPTS = 3;

  @Override
  public TaskReference enqueue(PipelineTask pipelineTask) {
    log.finest("Enqueueing: " + pipelineTask);
    TaskOptions taskOptions = toTaskOptions(pipelineTask);
    Queue queue = getQueue(pipelineTask.getQueueSettings().getOnQueue());
    PipelineTaskQueue.TaskReference taskReference = null;
    int pastAttempts = 0;
    do {
      try {
        TaskHandle handle = queue.add(taskOptions);
        taskReference = taskHandleToReference(handle);
      } catch (TaskAlreadyExistsException alreadyExistsException) {
        //taskOptions.taskName(pipelineTask.getTaskName() + "-" + pastAttempts);
        log.log(Level.WARNING, "Pipeline framework - task already exists", alreadyExistsException);
        taskReference = TaskReference.of(queue.getQueueName(), pipelineTask.getTaskName());
      }
    } while (taskReference == null && ++pastAttempts < MAX_ENQUEUE_ATTEMPTS);

    // in case of multiple already-exists, fake the return with the first one (similar to legacy behavior)
    taskReference = taskReference != null ? taskReference : TaskReference.of(queue.getQueueName(), pipelineTask.getTaskName());

    return taskReference;
  }

  @Override
  public Collection<TaskReference> enqueue(String queueName, Collection<TaskSpec> taskSpecs) {
    log.finest("Enqueueing: " + taskSpecs.size() + " tasks");
    List<TaskOptions> taskOptionsList = taskSpecs.stream().map(this::toTaskOptions).toList();
    Queue queue = getQueue(queueName);
    List<TaskReference> taskReferences = new ArrayList<>();
    for (TaskOptions taskOptions : taskOptionsList) {
      try {
        TaskHandle handle = queue.add(taskOptions);
        taskReferences.add(taskHandleToReference(handle));
      } catch (TaskAlreadyExistsException ignore) {
        // ignore
        taskReferences.add(TaskReference.of(queue.getQueueName(), ignore.getTaskNames().get(0)));
      }
    }
    return taskReferences;
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
  public Collection<TaskReference> enqueue(final Collection<PipelineTask> pipelineTasks) {
    return addToQueue(pipelineTasks);
  }

  @Override
  public Collection<TaskReference> enqueue(Transaction txn, Collection<PipelineTask> pipelineTasks) {
    //TODO: try to fake the txn here; not implemented bc this implementation won't be used in prod
    return addToQueue(pipelineTasks);
  }

  TaskReference taskHandleToReference(TaskHandle taskHandle) {
    return TaskReference.of(taskHandle.getQueueName(), taskHandle.getName());
  }

  //VisibleForTesting
  List<TaskReference> addToQueue(final Collection<PipelineTask> pipelineTasks) {
    List<TaskReference> handles = new ArrayList<>();
    Map<String, List<TaskOptions>> queueNameToTaskOptions = new HashMap<>();
    for (PipelineTask pipelineTask : pipelineTasks) {
      log.finest("Enqueueing: " + pipelineTask);
      String queueName = pipelineTask.getQueueSettings().getOnQueue();
      TaskOptions taskOptions = toTaskOptions(pipelineTask);


      // seen in logs : "Negative countdown is not allowed"
      if (taskOptions.getCountdownMillis() != null && taskOptions.getCountdownMillis() < 0) {
        log.warning("Task countdownMillis is  " + taskOptions.getCountdownMillis() + ". Setting to 0 to avoid error.");
        taskOptions.countdownMillis(0);
      }
      Instant now = Instant.now();
      if (taskOptions.getEtaMillis() != null && taskOptions.getEtaMillis() <= now.toEpochMilli()) {
        log.warning("Task etaMillis is  " + (now.toEpochMilli() - taskOptions.getEtaMillis()) + " before now. Setting to now + 30s to avoid error.");
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

  private List<TaskReference> addToQueue(Queue queue, List<TaskOptions> tasks) {
    int limit = tasks.size();
    int start = 0;
    List<Future<List<TaskHandle>>> futures = new ArrayList<>(limit / MAX_TASKS_PER_ENQUEUE + 1);
    while (start < limit) {
      int end = Math.min(limit, start + MAX_TASKS_PER_ENQUEUE);
      futures.add(queue.addAsync(tasks.subList(start, end)));
      start = end;
    }

    List<TaskReference> taskReferences = new ArrayList<>(limit);
    for (Future<List<TaskHandle>> future : futures) {
      try {
        future.get().stream().map(this::taskHandleToReference).forEach(taskReferences::add);
      } catch (InterruptedException e) {
        log.throwing("AppEngineTaskQueue", "addToQueue", e);
        Thread.currentThread().interrupt();
        throw new RuntimeException("addToQueue failed", e);
      } catch (ExecutionException e) {
        if (e.getCause() instanceof TaskAlreadyExistsException) {
          List<String> existingTaskNames = ((TaskAlreadyExistsException) e.getCause()).getTaskNames();
          existingTaskNames.stream()
            .map(taskName -> TaskReference.of(queue.getQueueName(), taskName))
            .forEach(taskReferences::add);
          // Ignore, as that suggests all non-duplicate tasks were sent successfully
        } else {
          throw new RuntimeException("addToQueue failed", e.getCause());
        }
      }
    }
    return taskReferences;
  }

  private TaskOptions toTaskOptions(PipelineTask pipelineTask) {
    final QueueSettings queueSettings = pipelineTask.getQueueSettings();

    TaskOptions taskOptions = TaskOptions.Builder.withUrl(taskHandlerUrl);

    String versionHostname;

    //annoyingly, guava Retryer throws Exceptions, rather than RuntimeExceptions
    try {
      versionHostname = retryer.call(() -> {
        //TODO: Modules are now called 'Services', but there's no "ServicesService" in GAE SDK, afaik
        String module = queueSettings.getOnService();
        String version = queueSettings.getOnServiceVersion();
        if (module == null) {
          module = environment.getService();
          version = environment.getVersion();
        }
        return servicesService.getWorkerServiceHostName(module, version);
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
    addProperties(taskOptions, pipelineTask.toProperties());
    String taskName = pipelineTask.getName();
    if (null != taskName) {
      // named tasks ARE used in the following cases ...
      //handleSlotFilled_*
      //runJob_*
      taskOptions.taskName(taskName);
    }
    return taskOptions;
  }

  private TaskOptions toTaskOptions(TaskSpec spec) {
    TaskOptions taskOptions = TaskOptions.Builder.withUrl(spec.getCallbackPath());

    Optional.ofNullable(spec.getScheduledExecutionTime())
      .ifPresent(eta -> taskOptions.etaMillis(eta.toEpochMilli()));

    taskOptions.method(spec.getMethod() == TaskSpec.Method.POST ? TaskOptions.Method.POST : TaskOptions.Method.GET);
    spec.getHeaders().forEach(taskOptions::header);
    spec.getParams().forEach(taskOptions::param);

    Preconditions.checkArgument(spec.getHost() != null, "Host must be set");

    taskOptions.header("Host", spec.getHost());

    return taskOptions;
  }


  private static void addProperties(TaskOptions taskOptions, Properties properties) {
    for (String paramName : properties.stringPropertyNames()) {
      String paramValue = properties.getProperty(paramName);
      taskOptions.param(paramName, paramValue);
    }
  }
}
