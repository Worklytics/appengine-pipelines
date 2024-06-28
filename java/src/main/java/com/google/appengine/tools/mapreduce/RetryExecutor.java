package com.google.appengine.tools.mapreduce;

import com.github.rholder.retry.RetryException;
import com.github.rholder.retry.RetryerBuilder;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

public class RetryExecutor {

  public static <V> V call(RetryerBuilder<V> retryerBuilder, Callable<V> callable) {
    try {
      return retryerBuilder.build().call(callable);
    } catch (ExecutionException e) {
      throw new RuntimeException(e.getCause());
    } catch (RetryException e) {
      throw new RuntimeException(e);
    }
  }

}
