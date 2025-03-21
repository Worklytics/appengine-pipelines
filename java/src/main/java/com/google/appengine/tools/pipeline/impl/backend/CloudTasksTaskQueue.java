package com.google.appengine.tools.pipeline.impl.backend;

import com.google.appengine.tools.pipeline.impl.servlets.TaskHandler;
import com.google.appengine.tools.pipeline.impl.tasks.PipelineTask;

import com.google.cloud.location.ListLocationsRequest;
import com.google.cloud.location.Location;
import com.google.cloud.tasks.v2.*;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import lombok.AllArgsConstructor;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;

import javax.inject.Inject;
import javax.inject.Provider;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static com.google.appengine.tools.pipeline.impl.PipelineManager.DEFAULT_QUEUE_NAME;

/**
 * implementation of PipelineTaskQueue backed by Cloud Tasks
 *
 * TODO: what happens when tasks exists?
 * TODO: retries for transients + internal errors
 *
 */
@RequiredArgsConstructor(onConstructor_ = @Inject)
public class CloudTasksTaskQueue implements PipelineTaskQueue {

  @NonNull
  AppEngineEnvironment appEngineEnvironment;

  @NonNull
  Provider<CloudTasksClient> cloudTasksClientProvider;

  @NonNull
  AppEngineServicesService appEngineServicesService;

  // GAE location -> Cloud Tasks location name
  Cache<String, String> locationCache =
          CacheBuilder.newBuilder().initialCapacity(1).build();

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
          String service = Optional.ofNullable(pipelineTask.getQueueSettings().getOnService())
                  .orElseGet(appEngineServicesService::getDefaultService);
          String version = Optional.ofNullable(pipelineTask.getQueueSettings().getOnServiceVersion())
                  .orElseGet(() -> appEngineServicesService.getDefaultVersion(service));
          String host = appEngineServicesService.getWorkerServiceHostName(service, version);
          return pipelineTask.toTaskSpec(host, TaskHandler.handleTaskUrl());
        }).collect(Collectors.toList())))
     .flatMap(Collection::stream)
     .collect(Collectors.toList());
  }

  Collection<TaskReference> enqueue(@NonNull String queueName, Collection<TaskSpec> taskSpecs) {
    String queueLocation = cloudTasksLocationFromAppEngineLocation(appEngineServicesService.getLocation());
    QueueName queue = QueueName.of(appEngineEnvironment.getProjectId(), queueLocation, queueName);
    try (CloudTasksClient cloudTasksClient = cloudTasksClientProvider.get()) {
      return taskSpecs.parallelStream() //q: this safe? efficient?
        .map(taskSpec -> createIgnoringExisting(cloudTasksClient, queue, taskSpec))
        .map(task -> TaskReference.of(queueName, TaskName.parse(task.getName()).getTask()))
        .collect(Collectors.toList());
    }
  }


  @SneakyThrows
  String cloudTasksLocationFromAppEngineLocation(@NonNull String appEngineLocation) {
    return locationCache.get(appEngineLocation, () -> {
      try (CloudTasksClient cloudTasksClient = cloudTasksClientProvider.get()) {
        CloudTasksClient.ListLocationsPagedResponse locations =
                cloudTasksClient.listLocations(ListLocationsRequest.newBuilder().
                        setName("projects/" + appEngineEnvironment.getProjectId())
                        .build());
        // this is picking ~the first location in list in the GAE region  (eg, us-central --> us-central1)
        // afaik, queues always end up here by default (we don't specify location in our queue.yaml)
        // but in theory might be somewhere else ... so probably should have a CLOUD_TASK_QUEUE_LOCATION env var or something
        // that would be taken in preference to doing this API call
        Optional<Location> queueLocation =
                locations.getPage().streamAll().filter(location -> location.getLocationId().startsWith(appEngineLocation)).findFirst();
        return queueLocation.map(Location::getLocationId).orElseThrow(() -> new Error("No queue location matching " + appEngineLocation));
      }
    });
  }


  private Task createIgnoringExisting(CloudTasksClient cloudTasksClient, QueueName queue, TaskSpec taskSpec) {
    Task task = toCloudTask(queue, taskSpec);
    try {
      return cloudTasksClient.createTask(queue, task);
    } catch (com.google.api.gax.rpc.AlreadyExistsException e) {
      //ignore
      return task;
    }
  }

  @Override
  public void deleteTasks(Collection<TaskReference> taskReferences) {
    try (CloudTasksClient cloudTasksClient = cloudTasksClientProvider.get()) {
      taskReferences.parallelStream().forEach(taskReference -> {
        TaskName taskName = TaskName.newBuilder()
                .setProject(appEngineEnvironment.getProjectId())
                .setLocation(cloudTasksLocationFromAppEngineLocation(appEngineServicesService.getLocation()))
                .setQueue(taskReference.getQueue())
                .setTask(taskReference.getTaskName())
                .build();
        cloudTasksClient.deleteTask(taskName);
      });
    }
  }

  Task toCloudTask(QueueName queue, TaskSpec taskSpec) {
    Task.Builder builder = Task.newBuilder();

    Optional.ofNullable(taskSpec.getName())
            .map(taskName -> TaskName.newBuilder()
                   .setProject(queue.getProject())
                    .setLocation(queue.getLocation())
                    .setQueue(queue.getQueue())
                    .setTask(taskName).build())
             .map(TaskName::toString)
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
