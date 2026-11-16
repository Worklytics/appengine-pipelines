package com.google.appengine.tools.pipeline.di;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertSame;

/**
 * Cloud Tasks and App Engine Admin API clients are shared by the whole process: components (and so modules) may be
 * built more than once, and a client per component would never be closed, as callers don't close shared clients.
 */
public class AppEngineHostModuleClientsTest {

  @Test
  public void clientsAreSharedAcrossModules() {
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
}
