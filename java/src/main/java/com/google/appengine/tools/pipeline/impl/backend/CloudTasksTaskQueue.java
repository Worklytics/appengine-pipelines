package com.google.appengine.tools.pipeline.impl.backend;

import com.google.appengine.tools.pipeline.impl.servlets.TaskHandler;
import com.google.appengine.tools.pipeline.impl.tasks.PipelineTask;

import com.google.cloud.tasks.v2.*;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import lombok.AllArgsConstructor;

import javax.inject.Inject;
import javax.inject.Provider;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;

import static com.google.appengine.tools.pipeline.impl.PipelineManager.DEFAULT_QUEUE_NAME;

/**
 * implementation of PipelineTaskQueue backed by Cloud Tasks
 */
@AllArgsConstructor
public class CloudTasksTaskQueue implements PipelineTaskQueue {

  @Inject
  AppEngineEnvironment appEngineEnvironment;

  @Inject
  Provider<CloudTasksClient> cloudTasksClientProvider;

  @Inject
  AppEngineServicesService appEngineServicesService;

  @Override
  public TaskReference enqueue(PipelineTask pipelineTask) {
    return enqueue(List.of(pipelineTask)).iterator().next();
  }

  @Override
  public TaskReference enqueue(String queueName, TaskSpec build) {
    return enqueue(queueName, List.of(build)).iterator().next();
  }

  @Override
  public Collection<TaskReference> enqueue(Collection<PipelineTask> pipelineTasks) {

    Map<String, List<PipelineTask>> tasksByQueue = pipelineTasks.stream()
      .collect(Collectors.groupingBy(pipelineTask -> Optional.ofNullable(pipelineTask.getQueueSettings().getOnQueue()).orElse(DEFAULT_QUEUE_NAME)));

    /// probably could use parallelStream here, but in practice don't *really* expect to have multiple queues in the batch
   return tasksByQueue.entrySet().stream()
      .map(e -> enqueue(e.getKey(), e.getValue().stream()
        .map(pipelineTask -> {
          String host = appEngineServicesService.getWorkerServiceHostName(pipelineTask.getQueueSettings().getOnService(), pipelineTask.getQueueSettings().getOnServiceVersion());
          return pipelineTask.toTaskSpec(host, TaskHandler.handleTaskUrl());
        }).collect(Collectors.toList())))
     .flatMap(Collection::stream)
     .collect(Collectors.toList());
  }

  Collection<TaskReference> enqueue(String queue, Collection<TaskSpec> taskSpecs) {
    QueueName queueName = QueueName.of(appEngineEnvironment.getProjectId(), appEngineEnvironment.getLocation(), queue);
    try (CloudTasksClient cloudTasksClient = cloudTasksClientProvider.get()) {
      return taskSpecs.parallelStream() //q: this safe? efficient?
        .map(taskSpec -> cloudTasksClient.createTask(queueName, toCloudTask(taskSpec)))
        .map(task -> TaskReference.of(queue, TaskName.parse(task.getName()).getTask()))
        .collect(Collectors.toList());
    }
  }

  @Override
  public void deleteTasks(Collection<TaskReference> taskReferences) {
    try (CloudTasksClient cloudTasksClient = cloudTasksClientProvider.get()) {
      taskReferences.parallelStream().forEach(taskReference -> {
        TaskName taskName = TaskName.of(appEngineEnvironment.getProjectId(), appEngineEnvironment.getLocation(), taskReference.getQueue(), taskReference.getTaskName());
        cloudTasksClient.deleteTask(taskName);
      });
    }
  }

  Task toCloudTask(TaskSpec taskSpec) {
    Task.Builder builder = Task.newBuilder();

    Optional.ofNullable(taskSpec.getName())
        .ifPresent(builder::setName);

    Optional.ofNullable(taskSpec.getScheduledExecutionTime())
        .map(instant -> Timestamp.newBuilder()
          .setSeconds(instant.getEpochSecond())
          .setNanos(instant.getNano())
          .build())
          .ifPresent(builder::setScheduleTime);

    String paramString = taskSpec.getParams().entrySet().stream()
      .map(e -> e.getKey() + "=" + URLEncoder.encode(e.getValue(), StandardCharsets.UTF_8))
        .collect(Collectors.joining("&"));

    AppEngineHttpRequest.Builder callbackRequest = AppEngineHttpRequest.newBuilder()
      .putAllHeaders(taskSpec.getHeaders());

    if (taskSpec.getMethod() == TaskSpec.Method.POST) {
      callbackRequest.setHttpMethod(HttpMethod.POST);
      callbackRequest.putHeaders("Content-Type", "application/x-www-form-urlencoded");
      callbackRequest.setBody(ByteString.copyFrom(paramString.getBytes(StandardCharsets.UTF_8)));
      callbackRequest.setRelativeUri(taskSpec.getCallbackPath());
    } else if (taskSpec.getMethod() == TaskSpec.Method.GET) {
      callbackRequest.setHttpMethod(HttpMethod.GET);
      callbackRequest.setRelativeUri(taskSpec.getCallbackPath() + "?" + paramString);
    } else {
      throw new Error("Unsupported method: " + taskSpec.getMethod());
    }

    builder.setAppEngineHttpRequest(callbackRequest.build());

    return builder.build();
  }
}
