package com.google.appengine.tools.pipeline.di;

import dagger.Component;

import javax.inject.Singleton;

@Singleton // expect exactly one of these per process
@Component
public interface MultiTenantComponent {

  TenantComponent clientComponent(TenantModule clientModule);

}
