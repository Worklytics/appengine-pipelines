package com.google.appengine.tools.pipeline.di;

public interface DIContainer<T> {

  void inject(T instance);
}
