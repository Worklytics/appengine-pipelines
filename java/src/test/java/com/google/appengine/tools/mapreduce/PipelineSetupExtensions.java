package com.google.appengine.tools.mapreduce;

import com.google.appengine.tools.mapreduce.impl.shardedjob.ShardedJobRunner;
import com.google.appengine.tools.mapreduce.impl.util.RequestUtils;
import com.google.appengine.tools.pipeline.*;
import com.google.appengine.tools.pipeline.di.DaggerJobRunServiceComponent;
import com.google.appengine.tools.pipeline.di.JobRunServiceComponent;
import com.google.appengine.tools.pipeline.di.StepExecutionComponent;
import com.google.appengine.tools.pipeline.di.StepExecutionModule;
import com.google.appengine.tools.pipeline.impl.PipelineManager;
import com.google.appengine.tools.pipeline.impl.backend.AppEngineServicesService;
import com.google.appengine.tools.pipeline.impl.backend.AppEngineBackEnd;
import com.google.appengine.tools.pipeline.impl.backend.AppEngineTaskQueue;
import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.DatastoreOptions;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.extension.*;

import javax.servlet.http.HttpServletRequest;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.time.Duration;
import java.util.*;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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


  static Map<Class<?>, ContextStoreKey> PARAMETER_CLASSES_CONTEXT_KEY_MAP = ImmutableMap.of(
    PipelineManager.class, ContextStoreKey.PIPELINE_MANAGER,
    PipelineOrchestrator.class, ContextStoreKey.PIPELINE_MANAGER,
    PipelineRunner.class, ContextStoreKey.PIPELINE_MANAGER,
    PipelineService.class, ContextStoreKey.PIPELINE_SERVICE,
    AppEngineBackEnd.class, ContextStoreKey.APP_ENGINE_BACKEND,
    JobRunServiceComponent.class, ContextStoreKey.JOB_RUN_SERVICE_COMPONENT,
    ShardedJobRunner.class, ContextStoreKey.SHARDED_JOB_RUNNER
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

    AppEngineServicesService appEngineServicesService =  new AppEngineServicesService() {
      @Override
      public String getDefaultService() {
        return "default";
      }

      @Override
      public String getDefaultVersion(String service) {
        return "1";
      }

      @Override
      public String getWorkerServiceHostName(String service, String version) {
        return "1.default.localhost";
      }
    };

    //TODO: clearly fugly; cleanup with better DI, but saving for TaskQueue modernization
    AppEngineBackEnd appEngineBackend = new AppEngineBackEnd(datastore, new AppEngineTaskQueue(appEngineServicesService), appEngineServicesService);

    HttpServletRequest mockRequest = mock(HttpServletRequest.class);
    when(mockRequest.getParameter(RequestUtils.Params.DATASTORE_PROJECT_ID)).thenReturn(datastoreOptions.getProjectId());
    when(mockRequest.getParameter(RequestUtils.Params.DATASTORE_DATABASE_ID)).thenReturn(datastoreOptions.getDatabaseId());
    when(mockRequest.getParameter(RequestUtils.Params.DATASTORE_NAMESPACE)).thenReturn(datastoreOptions.getNamespace());
    when(mockRequest.getParameter(RequestUtils.Params.DATASTORE_HOST)).thenReturn(datastoreOptions.getHost());

    StepExecutionComponent stepExecutionComponent
      = component.stepExecutionComponent(new StepExecutionModule(mockRequest));

    extensionContext.getStore(ExtensionContext.Namespace.GLOBAL)
      .put(ContextStoreKey.PIPELINE_SERVICE, stepExecutionComponent.pipelineService());
    extensionContext.getStore(ExtensionContext.Namespace.GLOBAL)
      .put(ContextStoreKey.PIPELINE_MANAGER, stepExecutionComponent.pipelineManager());
    extensionContext.getStore(ExtensionContext.Namespace.GLOBAL)
      .put(ContextStoreKey.APP_ENGINE_BACKEND, appEngineBackend);
    extensionContext.getStore(ExtensionContext.Namespace.GLOBAL)
      .put(ContextStoreKey.JOB_RUN_SERVICE_COMPONENT, component);
    ShardedJobRunner shardedJobRunner = stepExecutionComponent.shardedJobRunner();
    //TODO: set worker/controller task delays to ZERO?? speed up tests
    shardedJobRunner.setLockCheckTaskDelay(Duration.ofSeconds(5));
    extensionContext.getStore(ExtensionContext.Namespace.GLOBAL)
      .put(ContextStoreKey.SHARDED_JOB_RUNNER, shardedJobRunner);
  }

  public static class ParameterResolver implements org.junit.jupiter.api.extension.ParameterResolver {

    @Override
    public boolean supportsParameter(ParameterContext parameterContext,
                                     ExtensionContext extensionContext) throws ParameterResolutionException {
      return PARAMETER_CLASSES_CONTEXT_KEY_MAP.containsKey(parameterContext.getParameter().getType());
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
