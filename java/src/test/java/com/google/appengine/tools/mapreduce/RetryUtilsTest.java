package com.google.appengine.tools.mapreduce;

import com.google.cloud.BaseServiceException;
import com.google.cloud.datastore.DatastoreException;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Test;

import java.util.function.IntSupplier;

import static org.junit.jupiter.api.Assertions.*;

class RetryUtilsTest {

  @Test
  void handleDatastoreExceptionRetry() {

    // copied from DatastoreException.RETRYABLE_ERROR_CODES (private)
    ImmutableSet<BaseServiceException.Error> retryableErrors = ImmutableSet.of(
      new BaseServiceException.Error(10, "ABORTED", false),
      new BaseServiceException.Error(4, "DEADLINE_EXCEEDED", false),
      new BaseServiceException.Error(14, "UNAVAILABLE", true));
    ImmutableSet<BaseServiceException.Error> nonRetryableErrors = ImmutableSet.of(
      new BaseServiceException.Error(52222, "WHATEVER", false),
      new BaseServiceException.Error(989898, "WHO_KNOWS", false));

    for (BaseServiceException.Error error : retryableErrors) {
      DatastoreException de = new DatastoreException(error.getCode(), "something broke", error.getReason(), true, null);
      assertTrue(RetryUtils.handleDatastoreExceptionRetry().test(de));
    }
    for (BaseServiceException.Error error : nonRetryableErrors) {
      DatastoreException de = new DatastoreException(error.getCode(), "something broke", error.getReason(), false, null);
      assertFalse(RetryUtils.handleDatastoreExceptionRetry().test(de));
    }

    // Test with message containing "retry the transaction"
    for (BaseServiceException.Error error : nonRetryableErrors) {
      DatastoreException de = new DatastoreException(error.getCode(), "RETRY THE TRANSACTION", error.getReason(), false, null);
      assertTrue(RetryUtils.handleDatastoreExceptionRetry().test(de));
    }

    // embedded DatastoreException
    retryableErrors.forEach(error -> {
      DatastoreException root = new DatastoreException(error.getCode(), "something broken", error.getReason(), true, null);
      assertTrue(RetryUtils.handleDatastoreExceptionRetry().test(root));
      Exception chain1 = new Exception("Test Exception 1", root);
      Exception chain2 = new Exception("Test Exception 2", chain1);
      assertTrue(RetryUtils.handleDatastoreExceptionRetry().test(chain2));
    });

  }
}