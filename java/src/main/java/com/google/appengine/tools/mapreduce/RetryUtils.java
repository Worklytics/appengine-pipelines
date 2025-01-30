package com.google.appengine.tools.mapreduce;

import com.github.rholder.retry.Attempt;
import com.github.rholder.retry.RetryListener;
import com.github.rholder.retry.WaitStrategies;
import com.github.rholder.retry.WaitStrategy;

import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;


public class RetryUtils {

  public static WaitStrategy defaultWaitStrategy() {
    return WaitStrategies.join(
      WaitStrategies.exponentialWait(1_000,30_000, TimeUnit.MILLISECONDS)
    );
  }

  public static RetryListener logRetry(final Logger log, String className) {
    return new RetryListener() {
      @Override
      public <V> void onRetry(Attempt<V> attempt) {
        if (attempt.hasException()) {
          log.log(Level.WARNING, "%s, Retry attempt: %d, wait: %d".formatted(className, attempt.getAttemptNumber(), attempt.getDelaySinceFirstAttempt()), attempt.getExceptionCause());
        } else {
          log.log(Level.WARNING, "%s, Retry attempt: %d, wait: %d. No exception?".formatted(className, attempt.getAttemptNumber(), attempt.getDelaySinceFirstAttempt()));
        }
      }
    };
  }


}
