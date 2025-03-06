package com.google.appengine.tools.pipeline.impl.backend;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIfEnvironmentVariable;

import static org.junit.jupiter.api.Assertions.assertEquals;


/**
 * integration test for AppEngineServicesServiceImpl
 *
 * you must run this in an environment with application-default credentials (eg, gcloud CLI is auth'd) that can read the target
 * project's appengine services and versions; then fill in the constants below with the values for the service you want to test against
 *
 * and fill constants below with the values for the service you want to test against
 */
@Disabled // enable if you want to run an integration test
class AppEngineServicesServiceImplIntegrationTest {

  static final String INTEGRATION_TEST_PROJECT = "test-project";
  static final String INTEGRATION_TEST_SERVICE = "ci-example-service";
  static final String INTEGRATION_TEST_SERVICE_VERSION = "v1a";

  AppEngineEnvironment environment;
  AppEngineServicesServiceImpl appEngineServicesServiceImpl;

  @BeforeEach
  public void setup() {
    System.setProperty("GOOGLE_CLOUD_PROJECT",  INTEGRATION_TEST_PROJECT);
    appEngineServicesServiceImpl = AppEngineServicesServiceImpl.defaults();
  }

  @Test
  void getDefaultVersion() {
    assertEquals(INTEGRATION_TEST_SERVICE_VERSION,
      appEngineServicesServiceImpl.getDefaultVersion(INTEGRATION_TEST_SERVICE));
  }

  @Test
  void getWorkerServiceHostName() {
    assertEquals(INTEGRATION_TEST_SERVICE_VERSION + "-dot-" + INTEGRATION_TEST_SERVICE + "-dot-" + INTEGRATION_TEST_PROJECT + ".uc.r.appspot.com",
      appEngineServicesServiceImpl.getWorkerServiceHostName(INTEGRATION_TEST_SERVICE, INTEGRATION_TEST_SERVICE_VERSION));
  }
}