package com.google.appengine.tools.pipeline.impl.backend;

import com.google.appengine.tools.pipeline.impl.servlets.TaskHandler;
import com.google.appengine.tools.pipeline.impl.tasks.PipelineTask;
import com.google.cloud.datastore.Transaction;

import java.util.*;
import java.util.stream.Collectors;

// or forget about this, and use legacy AppEngineTaskQueue in tests??
// TODO: move to testing package or something, so not ultimately in JAR??
public class InProcessTaskQueue implements PipelineTaskQueue {

  Map<String, Stack<TaskSpec>> queues = new HashMap<>();

  @Override
  public TaskReference enqueue(PipelineTask pipelineTask) {
    String queueName = Optional.ofNullable(pipelineTask.getQueueSettings().getOnQueue()).orElse("default");
    return enqueue(queueName, pipelineTask.toTaskSpec("localhost", TaskHandler.handleTaskUrl()));
  }

  @Override
  public Collection<TaskReference> enqueue(String queueName, Collection<TaskSpec> taskSpecs) {
    if (!queues.containsKey(queueName)) {
      queues.put(queueName, new Stack<>());
    }
    List<TaskReference> taskReferences = new ArrayList<>();
    for (TaskSpec spec : taskSpecs) {
      String taskName = Optional.ofNullable(spec.getName()).orElse(UUID.randomUUID().toString());
      queues.get(queueName).push(spec);
      taskReferences.add(TaskReference.of(queueName, taskName));
    }
    return taskReferences;
  }

  @Override
  public Collection<TaskReference> enqueue(Collection<PipelineTask> pipelineTasks) {
    return pipelineTasks.stream().map(this::enqueue).collect(Collectors.toCollection(LinkedList::new));
  }

  @Override
  public Collection<TaskReference> enqueue(Transaction txn, Collection<PipelineTask> pipelineTasks) {
    return enqueue(pipelineTasks);
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
