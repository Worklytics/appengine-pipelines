package com.google.appengine.tools.pipeline.di;

import com.google.appengine.tools.cloudtasktest.FakeHttpServletRequest;
import com.google.appengine.tools.pipeline.PipelineService;
import com.google.appengine.tools.pipeline.impl.backend.AppEngineServicesServiceImpl;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;


import static org.junit.jupiter.api.Assertions.*;

class JobRunServiceComponentTest {


  // flaky, each test should have a fresh object graph, so here is expecting 0 instances, but being static, it
  // could be more
  @Disabled
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

    assertSame(serviceA, serviceB, "pipelineServices instances for given job-step execution (request)");

    // different step execution
    StepExecutionComponent requestScopedComponent2 = component.stepExecutionComponent(new StepExecutionModule(new FakeHttpServletRequest()));

    PipelineService serviceC = requestScopedComponent2.pipelineService();
    assertNotSame(serviceA, serviceC, "pipelineServices instances for different job-step execution (request) should be different");
    assertEquals(1, AppEngineServicesServiceImpl.instanceCount);
  }


  @Test
  public void testJobRunServiceComponent_servletReuse() {
    JobRunServiceComponent component = DaggerJobRunServiceComponent.create();

    JobRunServiceComponent component2 = DaggerJobRunServiceComponent.create();

    assertNotSame(component, component2, "jobRunServiceComponent from create() expected not the same");

    JobRunServiceComponent component3 = JobRunServiceComponentContainer.getInstance();

    JobRunServiceComponent component4 = JobRunServiceComponentContainer.getInstance();

    assertSame(component3, component4, "jobRunServiceComponent via container expected the same");
  }

}