/**
 * Copyright 2025 Worklytics, Co.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */
package com.google.appengine.tools.test;

import com.google.appengine.tools.mapreduce.impl.shardedjob.ShardedJobRunner;
import com.google.appengine.tools.pipeline.*;
import com.google.appengine.tools.pipeline.di.DaggerJobRunServiceComponent;
import com.google.appengine.tools.pipeline.di.JobRunServiceComponent;
import com.google.appengine.tools.pipeline.di.StepExecutionComponent;
import com.google.appengine.tools.pipeline.di.StepExecutionModule;
import com.google.appengine.tools.pipeline.impl.PipelineManager;
import com.google.appengine.tools.pipeline.impl.backend.AppEngineBackEnd;
import com.google.appengine.tools.pipeline.impl.backend.AppEngineTaskQueue;
import com.google.appengine.tools.pipeline.impl.servlets.PipelineServlet;
import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.DatastoreOptions;

import org.junit.jupiter.api.extension.*;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.Arrays;
import java.util.List;

@Target({ ElementType.TYPE, ElementType.METHOD })
@Retention(RetentionPolicy.RUNTIME)
@ExtendWith({
  DatastoreExtension.class,
  DatastoreExtension.ParameterResolver.class,
  PipelineComponentsExtension.class,
  PipelineComponentsExtension.ParameterResolver.class,
  CloudTasksExtension.class,
  CloudTasksExtension.ParameterResolver.class
})
public @interface PipelineSetupExtensions {

}

class PipelineComponentsExtension implements BeforeAllCallback, BeforeEachCallback {

  Datastore datastore;

  DatastoreOptions datastoreOptions;

  JobRunServiceComponent component;

  enum ContextStoreKey {
    PIPELINE_SERVICE,
    PIPELINE_MANAGER,
    APP_ENGINE_BACKEND,
    SHARDED_JOB_RUNNER,
    JOB_RUN_SERVICE_COMPONENT,
  }

  static final List<Class<?>> PARAMETER_CLASSES = Arrays.asList(
    PipelineManager.class,
    PipelineService.class,
    AppEngineBackEnd.class,
    PipelineOrchestrator.class,
    PipelineRunner.class,
    JobRunServiceComponent.class,
    ShardedJobRunner.class
  );

  @Override
  public void beforeAll(ExtensionContext extensionContext) throws Exception {
    component = DaggerJobRunServiceComponent.create();
  }

  @Override
  public void beforeEach(ExtensionContext extensionContext) throws Exception {
    // can be serialized, then used to re-constitute connection to datastore emulator on another thread/process
    datastore = (Datastore) extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(DatastoreExtension.DS_CONTEXT_KEY);
    datastoreOptions = (DatastoreOptions) extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(DatastoreExtension.DS_OPTIONS_CONTEXT_KEY);

    AppEngineBackEnd appEngineBackend = new AppEngineBackEnd(datastore, new AppEngineTaskQueue());

    StepExecutionComponent stepExecutionComponent
      = component.stepExecutionComponent(new StepExecutionModule(appEngineBackend));

    extensionContext.getStore(ExtensionContext.Namespace.GLOBAL)
      .put(ContextStoreKey.PIPELINE_SERVICE.name(), stepExecutionComponent.pipelineService());
    extensionContext.getStore(ExtensionContext.Namespace.GLOBAL)
      .put(ContextStoreKey.PIPELINE_MANAGER.name(), stepExecutionComponent.pipelineManager());
    extensionContext.getStore(ExtensionContext.Namespace.GLOBAL)
      .put(ContextStoreKey.APP_ENGINE_BACKEND.name(), appEngineBackend);
    extensionContext.getStore(ExtensionContext.Namespace.GLOBAL)
      .put(ContextStoreKey.JOB_RUN_SERVICE_COMPONENT.name(), component);
    extensionContext.getStore(ExtensionContext.Namespace.GLOBAL)
      .put(ContextStoreKey.SHARDED_JOB_RUNNER, stepExecutionComponent.shardedJobRunner());
  }

  public static class ParameterResolver implements org.junit.jupiter.api.extension.ParameterResolver {

    @Override
    public boolean supportsParameter(ParameterContext parameterContext,
                                     ExtensionContext extensionContext) throws ParameterResolutionException {
      return PARAMETER_CLASSES.contains(parameterContext.getParameter().getType());
    }

    @Override
    public Object resolveParameter(ParameterContext parameterContext,
                                   ExtensionContext extensionContext) throws ParameterResolutionException {
      if (parameterContext.getParameter().getType() == PipelineManager.class) {
        return extensionContext.getStore(ExtensionContext.Namespace.GLOBAL)
          .get(ContextStoreKey.PIPELINE_MANAGER.name());
      } else if (parameterContext.getParameter().getType() == PipelineService.class) {
        return extensionContext.getStore(ExtensionContext.Namespace.GLOBAL)
          .get(ContextStoreKey.PIPELINE_SERVICE.name());
      } else if (parameterContext.getParameter().getType() == AppEngineBackEnd.class) {
        return extensionContext.getStore(ExtensionContext.Namespace.GLOBAL)
          .get(ContextStoreKey.APP_ENGINE_BACKEND.name());
      } else if (parameterContext.getParameter().getType() == DatastoreOptions.class) {
        return extensionContext.getStore(ExtensionContext.Namespace.GLOBAL)
          .get(DatastoreExtension.DS_OPTIONS_CONTEXT_KEY);
      } else if (parameterContext.getParameter().getType() == PipelineServlet.class) {
        throw new Error("Not supported!");
      } else if (parameterContext.getParameter().getType() == PipelineOrchestrator.class) {
        return extensionContext.getStore(ExtensionContext.Namespace.GLOBAL)
          .get(ContextStoreKey.PIPELINE_MANAGER.name());
      } else if (parameterContext.getParameter().getType() == PipelineRunner.class) {
        return extensionContext.getStore(ExtensionContext.Namespace.GLOBAL)
          .get(ContextStoreKey.PIPELINE_MANAGER.name());
      } else if (parameterContext.getParameter().getType() == JobRunServiceComponent.class) {
        return extensionContext.getStore(ExtensionContext.Namespace.GLOBAL)
          .get(ContextStoreKey.JOB_RUN_SERVICE_COMPONENT.name());
      } else if (parameterContext.getParameter().getType() == ShardedJobRunner.class) {
        return extensionContext.getStore(ExtensionContext.Namespace.GLOBAL)
          .get(ContextStoreKey.SHARDED_JOB_RUNNER);
      } else {
        throw new Error("Shouldn't be reached");
      }
    }
  }

}
