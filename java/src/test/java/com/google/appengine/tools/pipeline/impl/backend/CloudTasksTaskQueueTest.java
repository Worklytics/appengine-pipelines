package com.google.appengine.tools.pipeline.impl.backend;

import com.google.appengine.tools.pipeline.testutil.FakeAppEngineEnvironment;
import com.google.appengine.tools.pipeline.testutil.FakeAppEngineServicesService;
import com.google.cloud.tasks.v2.CloudTasksClient;
import com.google.cloud.tasks.v2.HttpMethod;
import com.google.cloud.tasks.v2.QueueName;
import com.google.cloud.tasks.v2.Task;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.inject.Provider;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class CloudTasksTaskQueueTest {

  CloudTasksTaskQueue cloudTasksTaskQueue;

  AppEngineServicesService appEngineServicesService = FakeAppEngineServicesService.builder()
    .project("test-project")
    .defaultService("default")
    .version("fake-version")
    .domain("fake-domain")
    .location("us-central")
    .build();

  Provider<CloudTasksClient> cloudTasksClientProvider = () -> {
    try {
      return CloudTasksClient.create();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  };


  @BeforeEach
  void setUp() {
    AppEngineEnvironment environment = FakeAppEngineEnvironment.builder()
      .projectId("test-project")
      .service("default")
      .version("fake-version")
      .build();

    cloudTasksTaskQueue =
      new CloudTasksTaskQueue(environment, cloudTasksClientProvider, appEngineServicesService);
  }

  @Test
  public void queueLocation_systemProperty() {
    // something more exotic here, to ensure we haven't accidentally hardcoded somewhere
    System.setProperty(CloudTasksTaskQueue.ConfigProperty.CLOUDTASKS_QUEUE_LOCATION.name(), "us-west2");

    assertEquals("us-west2", cloudTasksTaskQueue.getQueueLocation());

    System.clearProperty(CloudTasksTaskQueue.ConfigProperty.CLOUDTASKS_QUEUE_LOCATION.name());
  }


  @Test
  public void taskSpec() {
    Task task = cloudTasksTaskQueue.toCloudTask(QueueName.of("test-project", "us-central", "test-queue"),
      CloudTasksTaskQueue.TaskSpec.builder()
        .host("test-host")
        .method(CloudTasksTaskQueue.TaskSpec.Method.GET)
        .callbackPath("/test/path")
        .headers(ImmutableMap.of("key", "value"))
        .build());

    assertEquals("test-host", task.getAppEngineHttpRequest().getHeaders().get("Host"));
    assertEquals(HttpMethod.GET, task.getAppEngineHttpRequest().getHttpMethod());
    assertEquals("/test/path?", task.getAppEngineHttpRequest().getRelativeUri());
    assertTrue( task.getAppEngineHttpRequest().getHeadersMap().containsKey("key"));
  }


}
