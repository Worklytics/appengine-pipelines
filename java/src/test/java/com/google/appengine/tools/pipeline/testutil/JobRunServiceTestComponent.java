package com.google.appengine.tools.pipeline.testutil;


import com.google.appengine.tools.pipeline.di.JobRunServiceComponent;
import dagger.Binds;
import dagger.Component;
import dagger.Module;

import javax.inject.Singleton;

/**
 * test equivalent of {@link com.google.appengine.tools.pipeline.di.JobRunServiceComponent}
 *
 */
@Singleton // expect exactly one of these per process
@Component(
  modules = {
    AppEngineHostTestModule.class,
    JobRunServiceTestComponent.Bindings.class,
  }
)
public interface JobRunServiceTestComponent extends JobRunServiceComponent {


  @Module
  interface Bindings {
    @Binds
    JobRunServiceComponent jobRunServiceComponent(JobRunServiceTestComponent testComponent);
  }

}
