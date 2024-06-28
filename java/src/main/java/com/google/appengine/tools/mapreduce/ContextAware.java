package com.google.appengine.tools.mapreduce;

/**
 * a class that is aware of its execution Context.
 *
 * TODO: better as an annotation/mix-in?
 */
public interface ContextAware {

  Context getContext();

  void setContext(Context context);
}
