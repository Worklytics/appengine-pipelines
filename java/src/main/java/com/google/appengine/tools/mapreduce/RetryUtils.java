package com.google.appengine.tools.mapreduce;

import com.github.rholder.retry.Attempt;
import com.github.rholder.retry.RetryListener;
import com.github.rholder.retry.WaitStrategies;
import com.github.rholder.retry.WaitStrategy;
import com.google.cloud.datastore.DatastoreException;
import com.google.common.base.Predicate;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;

import java.util.Iterator;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;


public class RetryUtils {

  public static final int SYMBOLIC_FOREVER = 200;

  public static WaitStrategy defaultWaitStrategy() {
    return WaitStrategies.join(
      WaitStrategies.exponentialWait(1_000,30_000, TimeUnit.MILLISECONDS)
    );
  }

  public static RetryListener logRetry(final Logger log, String className) {
    return new RetryListener() {
      @Override
      public <V> void onRetry(Attempt<V> attempt) {
        if (attempt.getAttemptNumber() > 1 || attempt.hasException()) {
          if (attempt.hasException()) {
            log.log(Level.WARNING, "%s, Retry attempt: %d, wait: %d".formatted(className, attempt.getAttemptNumber(), attempt.getDelaySinceFirstAttempt()), attempt.getExceptionCause());
          } else {
            log.log(Level.WARNING, "%s, Retry attempt: %d, wait: %d. No exception?".formatted(className, attempt.getAttemptNumber(), attempt.getDelaySinceFirstAttempt()));
          }
        }
      }
    };
  }

  public static Predicate<Throwable> handleDatastoreExceptionRetry() {
    return t -> {
      Iterator<DatastoreException> datastoreExceptionIterator = Iterables.filter(Throwables.getCausalChain(t), DatastoreException.class).iterator();
      if (datastoreExceptionIterator.hasNext()) {
        DatastoreException de = datastoreExceptionIterator.next();
        return de.isRetryable() ||
          (de.getMessage() != null && de.getMessage().toLowerCase().contains("retry the transaction"));
      }
      return false;
    };
  }
}
