package com.google.appengine.tools.pipeline.di;

import dagger.Component;

import javax.inject.Singleton;

@Singleton // expect exactly one of these per process
@Component(
  modules = {
    AppEngineHostModule.class,
  }
)
public interface MultiTenantComponent {

  TenantComponent clientComponent(TenantModule module);
}
