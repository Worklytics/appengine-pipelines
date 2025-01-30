package com.google.appengine.tools.mapreduce;

import com.github.rholder.retry.WaitStrategies;
import com.github.rholder.retry.WaitStrategy;

import java.util.concurrent.TimeUnit;

public class WaitStrategiesUtils {

  public static WaitStrategy defaultWaitStrategy() {
    return WaitStrategies.join(
      WaitStrategies.fixedWait(1_000, TimeUnit.MILLISECONDS),
      WaitStrategies.exponentialWait(30_000, TimeUnit.MILLISECONDS)
    );
  }

}
