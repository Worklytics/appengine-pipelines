package com.google.appengine.tools.pipeline.di;

import com.google.appengine.v1.ServicesClient;
import com.google.appengine.v1.VersionsClient;
import dagger.Component;
import dagger.Provides;
import lombok.SneakyThrows;

import javax.inject.Singleton;

@Singleton // expect exactly one of these per process
@Component
public interface MultiTenantComponent {

  TenantComponent clientComponent(TenantModule clientModule);

}
