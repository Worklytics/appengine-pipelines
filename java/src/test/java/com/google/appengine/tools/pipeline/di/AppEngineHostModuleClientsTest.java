package com.google.appengine.tools.pipeline.di;

import com.google.auth.oauth2.GoogleCredentials;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Cloud Tasks and App Engine Admin API clients are shared by the whole process: components (and so modules) may be
 * built more than once, and a client per component would never be closed, as callers don't close shared clients.
 */
public class AppEngineHostModuleClientsTest {

  @Test
  public void clientsAreSharedAcrossModules() {
    // creating these clients requires Application Default Credentials, which CI doesn't have
    assumeTrue(hasApplicationDefaultCredentials(),
      "Test disabled because Application Default Credentials are not available");

    AppEngineHostModule first = new AppEngineHostModule();
    AppEngineHostModule second = new AppEngineHostModule();

    assertSame(first.cloudTasksClient(), second.cloudTasksClient());
    assertSame(first.servicesClient(), second.servicesClient());
    assertSame(first.versionsClient(), second.versionsClient());
    assertSame(first.applicationsClient(), second.applicationsClient());
  }

  @Test
  public void multiTenantComponentIsSharedAcrossInstances() {
    assertSame(MultiTenantComponentContainer.getInstance(), MultiTenantComponentContainer.getInstance());
  }

  private static boolean hasApplicationDefaultCredentials() {
    try {
      GoogleCredentials.getApplicationDefault();
      return true;
    } catch (IOException e) {
      return false;
    }
  }
}
