package com.google.appengine.tools.pipeline;

public interface DIContainer<T> {

  void inject(T instance);
}
