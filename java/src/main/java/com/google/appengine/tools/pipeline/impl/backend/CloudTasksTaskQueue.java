package com.google.appengine.tools.pipeline.impl.backend;

import com.google.appengine.tools.pipeline.impl.servlets.TaskHandler;
import com.google.appengine.tools.pipeline.impl.tasks.PipelineTask;
import com.google.cloud.location.ListLocationsRequest;
import com.google.cloud.location.Location;
import com.google.cloud.tasks.v2.*;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.java.Log;

import javax.inject.Inject;
import javax.inject.Provider;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.logging.Level;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.appengine.tools.pipeline.impl.PipelineManager.DEFAULT_QUEUE_NAME;

/**
 * implementation of PipelineTaskQueue backed by Cloud Tasks
 *
 * TODO: retries for transients + internal errors
 *
 */
@Log
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
  public Collection<TaskReference> enqueue(Collection<PipelineTask> pipelineTasks) {
    return enqueue(pipelineTasks, false);
  }

  @Override
  public Multimap<String, TaskSpec> asTaskSpecs(Collection<PipelineTask> pipelineTasks) {
    Multimap<String, TaskSpec> taskSpecs = HashMultimap.create();
    pipelineTasks.forEach(pipelineTask -> {
        String service = Optional.ofNullable(pipelineTask.getQueueSettings().getOnService())
          .orElseGet(appEngineServicesService::getDefaultService);
        String version = Optional.ofNullable(pipelineTask.getQueueSettings().getOnServiceVersion())
          .orElseGet(() -> appEngineServicesService.getDefaultVersion(service));
        String host = appEngineServicesService.getWorkerServiceHostName(service, version);
        String queueName = Optional.ofNullable(pipelineTask.getQueueSettings().getOnQueue()).orElse(DEFAULT_QUEUE_NAME);
        taskSpecs.put(queueName, pipelineTask.toTaskSpec(host, TaskHandler.handleTaskUrl()));
    });
    return taskSpecs;
  }

  private Collection<TaskReference> enqueue(Collection<PipelineTask> pipelineTasks, boolean addDelay) {
    //how can we fake a transaction here?
    //  - add a delay, to give us time to delete tasks before they've executed in rollback case
    //  - take the txn context

    Map<String, List<PipelineTask>> tasksByQueue = pipelineTasks.stream()
      .collect(Collectors.groupingBy(pipelineTask -> Optional.ofNullable(pipelineTask.getQueueSettings().getOnQueue()).orElse(DEFAULT_QUEUE_NAME)));

    /// probably could use parallelStream here, but in practice don't *really* expect to have multiple queues in the batch
   return tasksByQueue.entrySet().stream()
      .map(tasksForQueue -> {
        Stream<TaskSpec> specs = tasksForQueue.getValue().stream()
                .map(pipelineTask -> {
                  String service = Optional.ofNullable(pipelineTask.getQueueSettings().getOnService())
                          .orElseGet(appEngineServicesService::getDefaultService);
                  String version = Optional.ofNullable(pipelineTask.getQueueSettings().getOnServiceVersion())
                          .orElseGet(() -> appEngineServicesService.getDefaultVersion(service));
                  String host = appEngineServicesService.getWorkerServiceHostName(service, version);
                  return pipelineTask.toTaskSpec(host, TaskHandler.handleTaskUrl());
                });
        if (addDelay) {
            specs = specs.map(taskSpec -> ensureDelay(taskSpec, Duration.ofSeconds(10)));
        }
        return enqueue(tasksForQueue.getKey(), specs.collect(Collectors.toList()));
      })
     .flatMap(Collection::stream)
     .collect(Collectors.toList());
  }

  private TaskSpec ensureDelay(TaskSpec taskSpec, Duration delay) {
    if (taskSpec.getScheduledExecutionTime() == null || taskSpec.getScheduledExecutionTime().isBefore(Instant.now())) {
      return taskSpec.withScheduledExecutionTime(Instant.now().plus(delay));
    } else {
      return taskSpec;
    }
  }

  @Override
  public Collection<TaskReference> enqueue(@NonNull String queueName, Collection<TaskSpec> taskSpecs) {
    String queueLocation = cloudTasksLocationFromAppEngineLocation(appEngineServicesService.getLocation());
    QueueName queue = QueueName.of(appEngineEnvironment.getProjectId(), queueLocation, queueName);
    Collection<TaskReference> taskReferences = new ArrayList<>();
    try (CloudTasksClient cloudTasksClient = cloudTasksClientProvider.get()) {
      for (TaskSpec taskSpec : taskSpecs) {
        Task task = createIgnoringExisting(cloudTasksClient, queue, taskSpec);
        taskReferences.add(TaskReference.of(queueName, TaskName.parse(task.getName()).getTask()));
      }
      return taskReferences;
    } catch (Exception e) {
      // something went wrong - delete any task already created
      log.log(Level.SEVERE, String.format("Task creation failed out of %d - deleting all", taskReferences.size()));
      deleteTasks(taskReferences);
      throw e;
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


  private static final int MAX_ENQUEUE_ATTEMPTS = 3;

  @SneakyThrows
  private Task createIgnoringExisting(CloudTasksClient cloudTasksClient, QueueName queue, TaskSpec taskSpec) {

    Task task = toCloudTask(queue, taskSpec);
    int pastAttempts = 0;
    Exception lastException = null;
    do {
      try {
        return cloudTasksClient.createTask(queue, task);
      } catch (com.google.api.gax.rpc.AlreadyExistsException e) {
        // GAE-legacy version of the FW ignored this case. but I am not sure it's still safe to do so, now that enqueue is not transactional with the datastore writes
        log.log(Level.WARNING, "CloudTasksTaskQueue task already exists for {0}", taskSpec.getName());
        taskSpec = taskSpec.withName(taskSpec.getName()+ "-" + pastAttempts);
        task = toCloudTask(queue, taskSpec);
        lastException = e;
      } catch (com.google.api.gax.rpc.UnavailableException |
               com.google.api.gax.rpc.DeadlineExceededException |
               com.google.api.gax.rpc.ResourceExhaustedException e) {
        log.log(Level.WARNING, "Transient error occurred for {0}, retrying...", taskSpec.getName());
        lastException = e;
      }
    } while (++pastAttempts < MAX_ENQUEUE_ATTEMPTS);
    if (lastException == null) {
      throw new Error("Retry logic failed");
    } else if (lastException instanceof com.google.api.gax.rpc.AlreadyExistsException) {
      // avoid dead-end case, where all N variants of the task name are taken
      // alternative is that we use timestamp or something in name on retry
      log.log(Level.WARNING, "N versions of task name already taken, giving up for {0}; really would hope this doesn't happen in prod", taskSpec.getName());
      return task;
    } else {
      throw lastException;
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
