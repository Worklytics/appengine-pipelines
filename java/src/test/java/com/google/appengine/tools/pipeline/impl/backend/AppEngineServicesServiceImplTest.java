package com.google.appengine.tools.pipeline.impl.backend;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class AppEngineServicesServiceImplTest {

  AppEngineServicesServiceImpl appEngineServicesServiceImpl;

  @BeforeEach
  public void setup() {
    appEngineServicesServiceImpl = new AppEngineServicesServiceImpl(
      new AppEngineEnvironment() {
        @Override
        public String getProjectId() {
          return "test-project";
        }

        @Override
        public String getService() {
          return "default";
        }

        @Override
        public String getVersion() {
          return "v123";
        }
      },
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
    assertEquals("v182",
      appEngineServicesServiceImpl.getDefaultVersion("default"));
  }


  @Test
  void getDefaultVersion_cached() {
    appEngineServicesServiceImpl.fillCache("non-default", "v5");

    assertEquals("v5",
      appEngineServicesServiceImpl.getDefaultVersion("non-default"));
  }
}