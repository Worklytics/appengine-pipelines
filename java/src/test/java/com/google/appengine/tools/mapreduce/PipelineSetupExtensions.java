package com.google.appengine.tools.mapreduce;

import com.google.appengine.tools.mapreduce.impl.shardedjob.ShardedJobRunner;
import com.google.appengine.tools.pipeline.*;
import com.google.appengine.tools.pipeline.di.DaggerJobRunServiceComponent;
import com.google.appengine.tools.pipeline.di.JobRunServiceComponent;
import com.google.appengine.tools.pipeline.di.StepExecutionComponent;
import com.google.appengine.tools.pipeline.di.StepExecutionModule;
import com.google.appengine.tools.pipeline.impl.PipelineManager;
import com.google.appengine.tools.pipeline.impl.backend.AppEngineBackEnd;
import com.google.appengine.tools.pipeline.impl.backend.AppEngineTaskQueue;
import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.DatastoreOptions;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.extension.*;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.*;

@Target({ ElementType.TYPE, ElementType.METHOD })
@Retention(RetentionPolicy.RUNTIME)
@ExtendWith({
  DatastoreExtension.class,
  DatastoreExtension.ParameterResolver.class,
  PipelineComponentsExtension.class,
  PipelineComponentsExtension.ParameterResolver.class,
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


  static Map<Class<?>, String> PARAMETER_CLASSES_CONTEXT_KEY_MAP = ImmutableMap.of(
    PipelineManager.class, ContextStoreKey.PIPELINE_MANAGER.name(),
    PipelineOrchestrator.class, ContextStoreKey.PIPELINE_MANAGER.name(),
    PipelineRunner.class, ContextStoreKey.PIPELINE_MANAGER.name(),
    PipelineService.class, ContextStoreKey.PIPELINE_SERVICE.name(),
    AppEngineBackEnd.class, ContextStoreKey.APP_ENGINE_BACKEND.name(),
    DatastoreOptions.class, DatastoreExtension.DS_OPTIONS_CONTEXT_KEY,
    JobRunServiceComponent.class, ContextStoreKey.JOB_RUN_SERVICE_COMPONENT.name(),
    ShardedJobRunner.class, ContextStoreKey.SHARDED_JOB_RUNNER.name()
    // PipelineServlet.class not supported
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
    ShardedJobRunner shardedJobRunner = stepExecutionComponent.shardedJobRunner();
    shardedJobRunner.setLockCheckTaskDelay(5_000);
    extensionContext.getStore(ExtensionContext.Namespace.GLOBAL)
      .put(ContextStoreKey.SHARDED_JOB_RUNNER, shardedJobRunner);
  }

  public static class ParameterResolver implements org.junit.jupiter.api.extension.ParameterResolver {

    @Override
    public boolean supportsParameter(ParameterContext parameterContext,
                                     ExtensionContext extensionContext) throws ParameterResolutionException {
      return PARAMETER_CLASSES_CONTEXT_KEY_MAP.keySet().contains(parameterContext.getParameter().getType());
    }

    @Override
    public Object resolveParameter(ParameterContext parameterContext,
                                   ExtensionContext extensionContext) throws ParameterResolutionException {
      return Optional.ofNullable(PARAMETER_CLASSES_CONTEXT_KEY_MAP.get(parameterContext.getParameter().getType()))
        .map(extensionContext.getStore(ExtensionContext.Namespace.GLOBAL)::get)
        .orElseThrow(() -> new Error("Not Supported"));
    }
  }

}
