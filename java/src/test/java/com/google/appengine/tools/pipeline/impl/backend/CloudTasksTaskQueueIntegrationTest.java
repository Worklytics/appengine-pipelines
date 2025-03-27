package com.google.appengine.tools.pipeline.impl.backend;

import com.google.appengine.tools.pipeline.testutil.FakeAppEngineEnvironment;
import com.google.appengine.tools.pipeline.testutil.FakeAppEngineServicesService;
import com.google.cloud.tasks.v2.CloudTasksClient;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.inject.Provider;
import java.time.Instant;
import java.util.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * integration test of CloudTasksTaskQueue against a remote project
 *
 * to enable:
 *   1.) update PROJECT_ID below (or ensure GOOGLE_CLOUD_PROJECT env var is set)
 *   2.) ensure Cloud Tasks API is enabled for the project, and a default queue in the location exists
 *   3.) ensure default gcloud credentials have perms to create/delete tasks in the project's default queue
 *
 * enable if GOOGLE_CLOUD_PROJECT env var is set to something other than "test-project"
 */
class CloudTasksTaskQueueIntegrationTest {

  static final String PROJECT_ID = System.getenv("GOOGLE_CLOUD_PROJECT");

  @BeforeAll
  static void checkProjectIsSetAndNotATestValue() {
    assumeTrue(PROJECT_ID != null,
      "Test disabled because GOOGLE_CLOUD_PROJECT is not set");

    // hacky alternative to @EnabledIfEnvironmentVariable, which wasn't working with negative regex for me
    assumeTrue(!"test-project".equals(PROJECT_ID),
      "Test disabled because GOOGLE_CLOUD_PROJECT is 'test-project'");
  }


  final String LOCATION = "us-central";

  final String SERVICE = "fake-service";

  CloudTasksTaskQueue cloudTasksTaskQueue;

  AppEngineServicesService appEngineServicesService = FakeAppEngineServicesService.builder()
    .project(PROJECT_ID)
    .defaultService(SERVICE)
    .version("fake-version")
    .domain("fake-domain")
    .location(LOCATION)
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
    if ( PROJECT_ID.equals("test-project")) {
      throw new IllegalStateException("GOOGLE_CLOUD_PROJECT env var must be set to a real project");
    }
    AppEngineEnvironment environment = FakeAppEngineEnvironment.builder()
      .projectId(PROJECT_ID)
      .service(SERVICE)
      .version("fake-version")
      .build();

      cloudTasksTaskQueue =
        new CloudTasksTaskQueue(environment, cloudTasksClientProvider, appEngineServicesService);
  }


  @Test
  void testEnqueueTaskSpec() {
    PipelineTaskQueue.TaskSpec spec = PipelineTaskQueue.TaskSpec.builder()
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

  @Test
  void testEnqueueTaskSpec_named() {
    String named = "named-" + UUID.randomUUID();
    PipelineTaskQueue.TaskSpec spec = PipelineTaskQueue.TaskSpec.builder()
      .name(named)
      .host(appEngineServicesService.getWorkerServiceHostName("fake-service", "fake-version"))
      .callbackPath("/fake-callback-path")
      .param("a", "value")
      .scheduledExecutionTime(Instant.now().plusSeconds(60))
      .build();
    PipelineTaskQueue.TaskReference ref = cloudTasksTaskQueue.enqueue("default", spec);

    // no longer ignoring already exists, so this should have extra suffix
    PipelineTaskQueue.TaskReference ref2 = cloudTasksTaskQueue.enqueue("default", spec);
    assertEquals(ref.getTaskName() + "-0", ref2.getTaskName());

    // mainly for cleanup
    cloudTasksTaskQueue.deleteTasks(Arrays.asList(ref, ref2));
  }

  //TODO: test enqueue(PipelineTask) case ... it's just nasty bc abstract and kinda weird
  // issue with that is QueueSettings without queue, and without version
}