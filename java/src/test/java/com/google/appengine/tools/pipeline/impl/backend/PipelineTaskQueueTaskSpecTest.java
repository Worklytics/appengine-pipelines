package com.google.appengine.tools.pipeline.impl.backend;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class PipelineTaskQueueTaskSpecTest {


  @Test
  public void testEquals() {
    PipelineTaskQueue.TaskSpec taskSpec = PipelineTaskQueue.TaskSpec.builder()
        .name("test")
        .method(PipelineTaskQueue.TaskSpec.Method.GET)
      .callbackPath("/test")
        .service("testService")

      .param("param1", "value1")
        .build();

    PipelineTaskQueue.TaskSpec taskSpec2 = PipelineTaskQueue.TaskSpec.builder()
        .name("test")
        .method(PipelineTaskQueue.TaskSpec.Method.GET)
        .service("testService")
        .callbackPath("/test")
      .param("param1", "value1")
        .build();

    assertEquals(taskSpec, taskSpec2);

    PipelineTaskQueue.TaskSpec taskSpec3 = PipelineTaskQueue.TaskSpec.builder()
        .name("test")
        .method(PipelineTaskQueue.TaskSpec.Method.GET)
        .service("testService")
        .callbackPath("/test")
      .param("param1", "value2")
        .build();

    assertNotEquals(taskSpec, taskSpec3);
  }

}