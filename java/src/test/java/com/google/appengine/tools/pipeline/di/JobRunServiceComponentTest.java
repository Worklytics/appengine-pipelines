package com.google.appengine.tools.pipeline.di;

import com.google.appengine.tools.cloudtasktest.FakeHttpServletRequest;
import com.google.appengine.tools.pipeline.PipelineService;
import com.google.appengine.tools.pipeline.impl.backend.AppEngineServicesServiceImpl;
import org.junit.jupiter.api.Test;


import static org.junit.jupiter.api.Assertions.*;

class JobRunServiceComponentTest {


  @Test
  public void testJobRunServiceComponent() {
    // basic DI setup that will really be done at runtime
    JobRunServiceComponent component = DaggerJobRunServiceComponent.create();

    assertEquals(0, AppEngineServicesServiceImpl.instanceCount);

    StepExecutionComponent requestScopedComponent = component.stepExecutionComponent(new StepExecutionModule(new FakeHttpServletRequest()));

    PipelineService serviceA = requestScopedComponent.pipelineService();
    assertEquals(1, AppEngineServicesServiceImpl.instanceCount);

    PipelineService serviceB = requestScopedComponent.pipelineService();
    assertEquals(1, AppEngineServicesServiceImpl.instanceCount);

    // ensure that the same PipelineService for all requests
    assertEquals(serviceA, serviceB);

    // different request
    StepExecutionComponent requestScopedComponent2 = component.stepExecutionComponent(new StepExecutionModule(new FakeHttpServletRequest()));

    PipelineService serviceC = requestScopedComponent2.pipelineService();
     assertNotEquals(serviceA, serviceC);
    assertEquals(1, AppEngineServicesServiceImpl.instanceCount);

  }

}