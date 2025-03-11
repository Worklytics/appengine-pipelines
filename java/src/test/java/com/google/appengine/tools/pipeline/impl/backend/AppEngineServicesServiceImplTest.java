package com.google.appengine.tools.pipeline.impl.backend;

import com.google.appengine.tools.pipeline.testutil.FakeAppEngineEnvironment;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class AppEngineServicesServiceImplTest {

  AppEngineServicesServiceImpl appEngineServicesServiceImpl;

  @BeforeEach
  public void setup() {
    appEngineServicesServiceImpl = new AppEngineServicesServiceImpl(
      FakeAppEngineEnvironment.builder()
        .service("default")
        .version("v123")
        .projectId("test-project")
        .build(),
      AppEngineServicesServiceImpl::getServicesClientProvider,
      AppEngineServicesServiceImpl::getVersionsClientProvider
    );
  }

  @Test
  void getDefaultService() {
    appEngineServicesServiceImpl.getDefaultService();
  }

  @Test
  void getDefaultVersion_current() {
    assertEquals("v123",
      appEngineServicesServiceImpl.getDefaultVersion("default"));
  }

  // prove that will retrieve value from cache, rather than go remote (which would fail)
  @Test
  void getDefaultVersion_cached() {
    appEngineServicesServiceImpl.fillCache("non-default", "v5");

    assertEquals("v5",
      appEngineServicesServiceImpl.getDefaultVersion("non-default"));
  }
}