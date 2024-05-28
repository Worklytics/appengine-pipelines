package com.google.appengine.tools.mapreduce;

/**
 * an MR pipeline element that is aware it's being called within a ShardContext, so can inherit that context
 *
 * use-case: incrementing counters inside createReaders(), which normally does not have access to a shard context on
 * which to count.
 *
 * TODO: better as an annotation/mix-in?
 */
public interface ShardContextAware {

  void setContext(ShardContext context);

  ShardContext getContext();

}

