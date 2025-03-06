package com.google.appengine.tools.pipeline.di;

import com.google.appengine.tools.mapreduce.impl.shardedjob.ShardedJobRunner;
import com.google.appengine.tools.mapreduce.impl.util.RequestUtils;
import com.google.appengine.tools.pipeline.*;
import com.google.appengine.tools.pipeline.impl.PipelineManager;
import com.google.appengine.tools.pipeline.impl.PipelineServiceImpl;
import com.google.appengine.tools.pipeline.impl.backend.AppEngineBackEnd;
import com.google.appengine.tools.pipeline.impl.backend.AppEngineServicesService;
import com.google.appengine.tools.pipeline.impl.backend.PipelineBackEnd;
import com.google.appengine.tools.pipeline.impl.backend.PipelineTaskQueue;
import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.DatastoreOptions;
import dagger.Binds;
import dagger.Module;
import dagger.Provides;
import lombok.RequiredArgsConstructor;

import javax.servlet.http.HttpServletRequest;

/**
 * essentially, request-scoped dependencies
 */
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
