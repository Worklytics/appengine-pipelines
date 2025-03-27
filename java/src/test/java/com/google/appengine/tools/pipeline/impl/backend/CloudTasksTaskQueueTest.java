package com.google.appengine.tools.pipeline.impl.backend;

import com.google.appengine.tools.pipeline.testutil.FakeAppEngineEnvironment;
import com.google.appengine.tools.pipeline.testutil.FakeAppEngineServicesService;
import com.google.cloud.tasks.v2.CloudTasksClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.inject.Provider;

import static org.junit.jupiter.api.Assertions.assertEquals;

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
    System.setProperty(CloudTasksTaskQueue.ConfigProperty.CLOUDTASKS_QUEUE_LOCATION.name(), "us-central1");

    assertEquals("us-central1", cloudTasksTaskQueue.getQueueLocation());

    System.clearProperty(CloudTasksTaskQueue.ConfigProperty.CLOUDTASKS_QUEUE_LOCATION.name());
  }


}
