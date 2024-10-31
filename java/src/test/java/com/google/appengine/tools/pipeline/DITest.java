package com.google.appengine.tools.pipeline;

import com.google.appengine.tools.pipeline.di.DIContainer;
import com.google.appengine.tools.pipeline.impl.backend.AppEngineEnvironment;
import com.google.appengine.tools.pipeline.impl.util.DIUtil;
import dagger.Component;
import dagger.Module;
import javax.inject.Inject; // jakarta.inject.Inject not working with dagger yet (maybe in 2.0??)

import dagger.Provides;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;


import static com.google.appengine.tools.pipeline.TestUtils.waitForJobToComplete;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class DITest extends PipelineTest {

  @Component(
    modules = {
      DIModule.class,
    }
  )
  public interface Container extends DIContainer<DITest> {

    //NOTE: have to do this for every class that may need to be injected (eg, Jobs, Tests, etc)
    void inject(JobWithDependency job);
  }


  @Module
  public static class DIModule {

    @Provides
    AppEngineEnvironment appEngineEnvironment() {
      return new AppEngineEnvironment() {
        @Override
        public String getService() {
          return "service";
        }

        @Override
        public String getVersion() {
          return "v0.1-test";
        }
      };
    }
  }

  @BeforeEach
  public void setup() {
    Container container = DaggerDITest_Container.create();
    container.inject(this);
    DIUtil.overrideComponentInstanceForTests(DaggerDITest_Container.class, container);
  }


  @NoArgsConstructor
  @Injectable(DaggerDITest_Container.class)
  public static class JobWithDependency extends Job0<String> {

    @Inject
    transient AppEngineEnvironment appEngineEnvironment;

    @Override
    public Value<String> run() throws Exception {
      return immediate(appEngineEnvironment.getVersion());
    }
  }

  @Test
  public void directInjection() {
    JobWithDependency job = new JobWithDependency();

    DIUtil.inject(DaggerDITest_Container.class, job);
    assertNotNull(job.appEngineEnvironment);
  }

  @SneakyThrows
  @Test
  public void testJobWithDependency(PipelineService pipelineService) {
    JobId pipelineId= pipelineService.startNewPipeline(new JobWithDependency());
    String value = waitForJobToComplete(pipelineService, pipelineId);
    assertEquals("v0.1-test", value);
  }
}
