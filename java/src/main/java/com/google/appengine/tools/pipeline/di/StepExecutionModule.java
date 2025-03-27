package com.google.appengine.tools.pipeline.di;

import com.google.appengine.tools.mapreduce.impl.util.RequestUtils;
import com.google.cloud.datastore.DatastoreOptions;

import dagger.Module;
import dagger.Provides;
import lombok.RequiredArgsConstructor;

import javax.servlet.http.HttpServletRequest;

/**
 * essentially, request-scoped dependencies
 */
@StepExecutionScoped
@RequiredArgsConstructor
@Module(
  includes = {
    AppEngineBackendModule.class,
    PipelinesBindings.class
  }
)
public class StepExecutionModule {

  // this is really what 'scopes' the step; it's request-scoped
  private final HttpServletRequest request;

  @Provides @StepExecutionScoped
  DatastoreOptions datastoreOptions(RequestUtils requestUtils) {
    return requestUtils.buildDatastoreFromRequest(request);
  }
}
