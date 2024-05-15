package com.google.appengine.tools.pipeline;

import com.google.appengine.tools.pipeline.impl.backend.AppEngineEnvironment;
import com.google.appengine.tools.pipeline.impl.util.DIUtil;
import dagger.Module;
import javax.inject.Inject; // jakarta.inject.Inject not working with dagger yet (maybe in 2.0??)

import dagger.Provides;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;


import static com.google.appengine.tools.pipeline.TestUtils.waitForJobToComplete;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class DITest extends PipelineTest {


  @Module(
    injects = {
      JobWithDependency.class,
    },
    includes = {
      DefaultDIModule.class,
    },
    complete = false,
    library = true,
    overrides = true
  )
  public static class DIModule {

    @Provides
    AppEngineEnvironment appEngineEnvironment() {
      return new AppEngineEnvironment() {
        @Override
        public String getService() {
          return null;
        }

        @Override
        public String getVersion() {
          return "dsaf";
        }
      };
    }



  }

  @NoArgsConstructor
  @Injectable(DIModule.class)
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

    DIUtil.inject(DIModule.class.getName(), job);
    assertNotNull(job.appEngineEnvironment);
  }

  @SneakyThrows
  @Test
  public void test(PipelineService pipelineService) {
    String pipelineId = pipelineService.startNewPipeline(new JobWithDependency());
    String value = waitForJobToComplete(pipelineService, pipelineId);
    assertEquals("dsaf", value);
  }
}
