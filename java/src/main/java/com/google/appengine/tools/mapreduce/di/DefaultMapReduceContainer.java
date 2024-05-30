package com.google.appengine.tools.mapreduce.di;

import com.google.appengine.tools.mapreduce.MapReduceServlet;
import com.google.appengine.tools.mapreduce.servlets.ShufflerServlet;
import dagger.Component;
import dagger.Module;
import dagger.Provides;

import javax.inject.Singleton;

@Singleton
@Component(
    modules = {
      com.google.appengine.tools.pipeline.DefaultDIModule.class,
      DefaultMapReduceContainer.MapReduceModule.class,
    }
)
public interface DefaultMapReduceContainer {

  void inject(MapReduceServlet servlet);

  void inject(ShufflerServlet servlet);

  @Module
  class MapReduceModule  {


  }
}
