package com.google.appengine.tools.pipeline.impl.backend;

import com.google.appengine.tools.pipeline.testutil.FakeAppEngineEnvironment;
import com.google.appengine.tools.pipeline.testutil.FakeAppEngineServicesService;
import com.google.cloud.tasks.v2.CloudTasksClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;


import javax.inject.Provider;
import java.time.Instant;
import java.util.Collection;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * integration test of CloudTasksTaskQueue against a remote project
 *
 * to enable:
 *   1.) update PROJECT_ID below (or ensure GOOGLE_CLOUD_PROJECT env var is set)
 *   2.) ensure Cloud Tasks API is enabled for the project, and a default queue in the location exists
 *   3.) ensure default gcloud credentials have perms to create/delete tasks in the project's default queue
 *
 *
 */
@Disabled
class CloudTasksTaskQueueIntegrationTest {

  //update to static
  final String PROJECT_ID = System.getProperty("GOOGLE_CLOUD_PROJECT");

  final String LOCATION = "us-central1";

  final String SERVICE = "fake-service";

  CloudTasksTaskQueue cloudTasksTaskQueue;

  AppEngineServicesService appEngineServicesService = FakeAppEngineServicesService.builder()
    .project(PROJECT_ID)
    .defaultService(SERVICE)
    .version("fake-version")
    .domain("fake-domain")
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
      .projectId(PROJECT_ID)
      .service(SERVICE)
      .version("fake-version")
      .build();

      cloudTasksTaskQueue =
        new CloudTasksTaskQueue(environment, cloudTasksClientProvider, appEngineServicesService);
  }


  @Test
  void testEnqueueTask() {
    PipelineTaskQueue.TaskSpec spec = PipelineTaskQueue.TaskSpec.builder()
      .param("taskName", "fake-task-name")
      .host(appEngineServicesService.getWorkerServiceHostName("fake-service", "fake-version"))
      .callbackPath("/fake-callback-path")
      .param("a", "value")
      .scheduledExecutionTime(Instant.now().plusSeconds(60))
      .build();
    PipelineTaskQueue.TaskReference ref = cloudTasksTaskQueue.enqueue("default", spec);

    cloudTasksTaskQueue.deleteTasks(Collections.singletonList(ref));

    //multiple
    Collection<PipelineTaskQueue.TaskReference> refs = cloudTasksTaskQueue.enqueue("default", Collections.nCopies(10, spec));
    assertEquals(10, refs.size());
    cloudTasksTaskQueue.deleteTasks(refs);
  }
}