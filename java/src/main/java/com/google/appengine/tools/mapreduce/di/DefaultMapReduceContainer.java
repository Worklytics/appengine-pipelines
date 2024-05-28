package com.google.appengine.tools.mapreduce.di;

import com.google.appengine.tools.mapreduce.MapReduceServlet;
import com.google.appengine.tools.mapreduce.impl.shardedjob.ShardedJobService;
import com.google.appengine.tools.mapreduce.impl.shardedjob.ShardedJobServiceFactory;
import com.google.appengine.tools.mapreduce.impl.util.RequestUtils;
import com.google.appengine.tools.pipeline.DefaultDIModule;
import com.google.appengine.tools.pipeline.PipelineService;
import com.google.appengine.tools.pipeline.impl.PipelineManager;
import com.google.appengine.tools.pipeline.impl.PipelineServiceImpl;
import dagger.Component;
import dagger.Module;
import dagger.Provides;

import javax.inject.Singleton;

@Singleton
@Component(
    modules = {
      DefaultDIModule.class,
      DefaultMapReduceContainer.MapReduceModule.class,
    }
)
public interface DefaultMapReduceContainer {

  void inject(MapReduceServlet servlet);

  @Module
  class MapReduceModule  {

    @Provides @Singleton
    ShardedJobServiceFactory provideShardedJobServiceFactory(PipelineService pipelineService) {
      return new ShardedJobServiceFactory(pipelineService);
    }

    @Provides @Singleton
    ShardedJobService provideShardedJobService(ShardedJobServiceFactory factory) {
      return factory.getShardedJobService();
    }

    @Provides @Singleton PipelineService providePipelineService(PipelineManager pipelineManager) {
      return new PipelineServiceImpl(pipelineManager);
    }

    @Provides @Singleton
    RequestUtils provideRequestUtils() {
      return new RequestUtils();
    }
  }
}
