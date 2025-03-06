package com.google.appengine.tools.pipeline.impl.backend;

import com.google.appengine.tools.pipeline.impl.tasks.Task;

import java.util.*;
import java.util.stream.Collectors;

// TODO: move to testing package or something, so not ultimately in JAR??
public class InProcessTaskQueue implements PipelineTaskQueue {

  Map<String, Stack<Task>> queues = new HashMap<>();

  @Override
  public TaskReference enqueue(Task task) {
    String queueName = Optional.ofNullable(task.getQueueSettings().getOnQueue()).orElse("default");
    String taskName = Optional.ofNullable(task.getName()).orElse(UUID.randomUUID().toString());
    if (!queues.containsKey(queueName)) {
      queues.put(queueName, new Stack<>());
    }
    queues.get(queueName).push(task);

    return TaskReference.of(queueName, taskName);
  }

  @Override
  public Collection<TaskReference> enqueue(Collection<Task> tasks) {
    return tasks.stream().map(this::enqueue).collect(Collectors.toCollection(LinkedList::new));
  }

  @Override
  public void deleteTasks(Collection<TaskReference> taskReferences) {
    for (TaskReference taskReference : taskReferences) {
      if (queues.containsKey(taskReference.getQueue())) {
        Stack<Task> queue = queues.get(taskReference.getQueue());
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
