package com.google.appengine.tools.pipeline.impl.backend;

import com.google.appengine.tools.pipeline.impl.tasks.PipelineTask;

import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

// or forget about this, and use legacy AppEngineTaskQueue in tests??
// TODO: move to testing package or something, so not ultimately in JAR??
public class InProcessTaskQueue implements PipelineTaskQueue {

  Map<String, Stack<TaskSpec>> queues = new HashMap<>();

  @Override
  public TaskReference enqueue(PipelineTask pipelineTask) {
    String queueName = Optional.ofNullable(pipelineTask.getQueueSettings().getOnQueue()).orElse("default");

    TaskSpec.TaskSpecBuilder spec = TaskSpec.builder()
      .name(pipelineTask.getTaskName())
      .callbackPath("/_ah/pipeline/task_callback")
      .host("localhost:8080")
      .method(TaskSpec.Method.POST);

    pipelineTask.toProperties().entrySet()
      .forEach(p -> spec.param((String) p.getKey(), (String) p.getValue()));

    if (pipelineTask.getQueueSettings().getDelayInSeconds() != null) {
      spec.scheduledExecutionTime(Instant.now().plusSeconds(pipelineTask.getQueueSettings().getDelayInSeconds()));
    }

    return enqueue(queueName, spec.build());
  }

  @Override
  public TaskReference enqueue(String queueName, TaskSpec spec) {
    if (!queues.containsKey(queueName)) {
      queues.put(queueName, new Stack<>());
    }
    String taskName = Optional.ofNullable(spec.getName()).orElse(UUID.randomUUID().toString());
    queues.get(queueName).push(spec);
    return TaskReference.of(queueName, taskName);
  }

  @Override
  public Collection<TaskReference> enqueue(Collection<PipelineTask> pipelineTasks) {
    return pipelineTasks.stream().map(this::enqueue).collect(Collectors.toCollection(LinkedList::new));
  }

  @Override
  public void deleteTasks(Collection<TaskReference> taskReferences) {
    for (TaskReference taskReference : taskReferences) {
      if (queues.containsKey(taskReference.getQueue())) {
        Stack<TaskSpec> queue = queues.get(taskReference.getQueue());
        queue.removeIf(task -> task.getName().equals(taskReference.getTaskName()));
      }
    }
  }

  /*
    * For testing purposes only. Wipes out all tasks in all queues.
   */
  void reset() {
    queues.clear();
  }

}
