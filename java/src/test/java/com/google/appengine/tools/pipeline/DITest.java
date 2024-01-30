package com.google.appengine.tools.pipeline;

import com.google.appengine.tools.pipeline.impl.backend.AppEngineEnvironment;
import com.google.common.collect.ImmutableList;
import dagger.Module;
import jakarta.inject.Inject;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;

import java.util.List;

import static com.google.appengine.tools.pipeline.TestUtils.waitForJobToComplete;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class DITest extends PipelineTest {


  @Module(
    injects = {
      JobWithDependency.class,
    },
    includes = {
      DefaultDIModule.class,
    },
    complete = false,
    library = true
  )
  public static class DIModule {

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

  @SneakyThrows
  @Test
  public void test(PipelineService pipelineService) {
    String pipelineId = pipelineService.startNewPipeline(new JobWithDependency());
    String value = waitForJobToComplete(pipelineService, pipelineId);
    assertEquals("dsaf", value);
  }
}
