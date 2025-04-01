package com.google.appengine.tools.pipeline.impl.backend;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIfEnvironmentVariable;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;


/**
 * integration test for AppEngineServicesServiceImpl
 *
 * you must run this in an environment with application-default credentials (eg, gcloud CLI is auth'd) that can read the target
 * project's appengine services and versions; then fill in the constants below with the values for the service you want to test against
 *
 * and fill constants below with the values for the service you want to test against
 */
class AppEngineServicesServiceImplIntegrationTest {

  static final String PROJECT_ID = System.getenv("GOOGLE_CLOUD_PROJECT");

  // NOTE: update these values to match your project's set-up
  static final String INTEGRATION_TEST_SERVICE = "pipelines";
  static final String INTEGRATION_TEST_SERVICE_VERSION = "1";

  @BeforeAll
  static void checkProjectIsSetAndNotATestValue() {
    assumeTrue(PROJECT_ID != null,
      "Test disabled because GOOGLE_CLOUD_PROJECT is not set");

    // hacky alternative to @EnabledIfEnvironmentVariable, which wasn't working with negative regex for me
    assumeTrue(!"test-project".equals(PROJECT_ID),
      "Test disabled because GOOGLE_CLOUD_PROJECT iis 'test-project'");
  }



  AppEngineEnvironment environment;
  AppEngineServicesServiceImpl appEngineServicesServiceImpl;

  @BeforeEach
  public void setup() {
    System.setProperty("GOOGLE_CLOUD_PROJECT",  PROJECT_ID);
    appEngineServicesServiceImpl = AppEngineServicesServiceImpl.defaults();
  }

  @Test
  void getDefaultVersion() {
    assertEquals(INTEGRATION_TEST_SERVICE_VERSION,
      appEngineServicesServiceImpl.getDefaultVersion(INTEGRATION_TEST_SERVICE));
  }

  @Test
  void getWorkerServiceHostName() {
    assertEquals(appEngineServicesServiceImpl.getDefaultVersion(INTEGRATION_TEST_SERVICE) + "-dot-" + INTEGRATION_TEST_SERVICE + "-dot-" + PROJECT_ID + ".uc.r.appspot.com",
      appEngineServicesServiceImpl.getWorkerServiceHostName(INTEGRATION_TEST_SERVICE, appEngineServicesServiceImpl.getDefaultVersion(INTEGRATION_TEST_SERVICE)));
  }

  @Test
  void getLocation() {
    assertEquals("us-central",
      appEngineServicesServiceImpl.getLocation());
  }
}