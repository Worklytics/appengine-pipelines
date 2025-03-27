package com.google.appengine.tools.pipeline.di;

import com.google.appengine.tools.mapreduce.impl.handlers.MapReduceServletImpl;
import com.google.appengine.tools.mapreduce.impl.util.RequestUtils;
import com.google.appengine.tools.pipeline.impl.backend.AppEngineBackEnd;
import com.google.appengine.tools.pipeline.impl.backend.AppEngineServicesService;
import com.google.appengine.tools.pipeline.impl.backend.PipelineBackEnd;
import com.google.appengine.tools.pipeline.impl.backend.PipelineTaskQueue;
import com.google.appengine.tools.pipeline.impl.servlets.*;
import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.DatastoreOptions;
import dagger.Component;
import dagger.Module;
import dagger.Provides;
import dagger.Subcomponent;
import lombok.SneakyThrows;

import javax.inject.Singleton;

/**
 * dagger2 DI component to represent the service that runs jobs.
 *
 * following pattern of https://praveer09.github.io/technology/2016/01/23/scoped-objects-in-dagger-2/
 * analogus to `ApplicationComponent` in the above example
 *
 */
@Singleton // expect exactly one of these per process
@Component(
  modules = {
    AppEngineHostModule.class,
  }
)
public interface JobRunServiceComponent {

  StepExecutionComponent stepExecutionComponent(StepExecutionModule stepExecutionModule);
  RequestUtils requestUtils();

  TaskHandler taskHandler();

  AbortJobHandler abortJobHandler();

  DeleteJobHandler deleteJobHandler();

  JsonClassFilterHandler jsonClassFilterHandler();

  JsonListHandler jsonListHandler();

  JsonTreeHandler jsonTreeHandler();

  MapReduceServletImpl mapReduceServletImpl();

}
