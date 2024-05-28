package com.google.appengine.tools.mapreduce;

import com.google.appengine.tools.pipeline.*;
import com.google.appengine.tools.pipeline.impl.PipelineManager;
import com.google.appengine.tools.pipeline.impl.backend.AppEngineBackEnd;
import com.google.appengine.tools.pipeline.impl.backend.AppEngineTaskQueue;
import com.google.appengine.tools.pipeline.impl.servlets.PipelineServlet;
import com.google.appengine.tools.pipeline.impl.util.DIUtil;
import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.DatastoreOptions;
import dagger.Component;
import dagger.Module;
import dagger.Provides;
import lombok.AllArgsConstructor;
import lombok.Setter;
import org.junit.jupiter.api.extension.*;

import javax.inject.Inject;
import javax.inject.Singleton;
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
  //AppEngineEnvironmentExtension.class,
  PipelineComponentsExtension.class,
  PipelineComponentsExtension.ParameterResolver.class,
})
public @interface PipelineSetupExtensions {

}

class PipelineComponentsExtension implements BeforeEachCallback {

  Datastore datastore;

  DatastoreOptions datastoreOptions;

  @Inject
  protected PipelineService pipelineService;
  @Inject
  protected PipelineManager pipelineManager;
  @Inject
  protected AppEngineBackEnd appEngineBackend;
  @Inject
  protected PipelineServlet pipelineServlet;

  enum ContextStoreKey {
    PIPELINE_SERVICE,
    PIPELINE_MANAGER,
    APP_ENGINE_BACKEND,
    PIPELINE_SERVLET;
  }

  static final List<Class<?>> PARAMETER_CLASSES = Arrays.asList(
    PipelineManager.class,
    PipelineService.class,
    AppEngineBackEnd.class,
    DatastoreOptions.class,
    PipelineServlet.class
  );


  @Override
  public void beforeEach(ExtensionContext extensionContext) throws Exception {
    datastore = (Datastore) extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(DatastoreExtension.DS_CONTEXT_KEY);

    // can be serialized, then used to re-constitute connection to datastore emulator on another thread/process
    datastoreOptions = (DatastoreOptions) extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(DatastoreExtension.DS_OPTIONS_CONTEXT_KEY);


    TestDIModule.setDatastore(datastore);
    TestDIModule.setDatastoreOptions(datastoreOptions);

    PipelineSetupTestContainer container = DaggerPipelineSetupTestContainer.create();


    container.injectExtension(this);

    extensionContext.getStore(ExtensionContext.Namespace.GLOBAL)
      .put(ContextStoreKey.PIPELINE_SERVICE.name(), pipelineService);
    extensionContext.getStore(ExtensionContext.Namespace.GLOBAL)
      .put(ContextStoreKey.PIPELINE_MANAGER.name(), pipelineManager);
    extensionContext.getStore(ExtensionContext.Namespace.GLOBAL)
      .put(ContextStoreKey.APP_ENGINE_BACKEND.name(), appEngineBackend);
    extensionContext.getStore(ExtensionContext.Namespace.GLOBAL)
      .put(ContextStoreKey.PIPELINE_SERVLET.name(), pipelineServlet);


    DIUtil.overrideComponentInstanceForTests(DaggerDefaultContainer.class, container);
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
        return extensionContext.getStore(ExtensionContext.Namespace.GLOBAL)
          .get(ContextStoreKey.PIPELINE_SERVLET.name());
      }
      throw new Error("Shouldn't be reached");
    }
  }

}

@Singleton
@Component(
  modules = {
    TestDIModule.class,
  }
)
interface PipelineSetupTestContainer {

  //NOTE: have to do this for every class that may need to be injected (eg, Jobs, Tests, etc)
  //void injectPipelineServlet(PipelineServlet servlet);

  void injectExtension(PipelineComponentsExtension extension);
}


@Module
@AllArgsConstructor
class TestDIModule {

  @Setter
  static Datastore datastore;
  @Setter
  static DatastoreOptions datastoreOptions;

  @Provides @Singleton
  Datastore datastore() {
    return datastore;
  }

  @Provides @Singleton
  DatastoreOptions datastoreOptions() {
    return datastoreOptions;
  }
  @Provides @Singleton
  PipelineService pipelineService(AppEngineBackEnd appEngineBackend) {
    return PipelineServiceFactory.newPipelineService(appEngineBackend);
  }
  @Provides @Singleton
  PipelineManager pipelineManager(AppEngineBackEnd appEngineBackend) {
    return new PipelineManager(appEngineBackend);
  }

  @Provides @Singleton
  AppEngineBackEnd appEngineBackend(Datastore datastore) {
    return new AppEngineBackEnd(datastore, new AppEngineTaskQueue());
  }

  //generic, could be a binding
  @Provides @Singleton
  PipelineOrchestrator pipelineOrchestrator(PipelineManager pipelineManager) {
    return pipelineManager;
  }

  //generic, could be a binding
  @Provides @Singleton
  PipelineRunner pipelineRunner(PipelineManager pipelineManager) {
    return pipelineManager;
  }
}