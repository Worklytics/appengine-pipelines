// Copyright 2014 Google Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy of
// the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations under
// the License.

package com.google.appengine.tools.pipeline;

import static com.google.appengine.tools.pipeline.Job.immediate;
import static com.google.appengine.tools.pipeline.Job.waitFor;

import com.google.appengine.api.taskqueue.DeferredTask;
import com.google.appengine.api.taskqueue.DeferredTaskContext;
import com.google.cloud.datastore.Key;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import lombok.NonNull;
import lombok.SneakyThrows;

import java.io.Serializable;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.servlet.http.HttpServletRequest;

/**
 * A collection of common jobs and utilities.
 *
 * @author ozarov@google.com (Arie Ozarov)
 */
public class Jobs {

  private Jobs() {
    // A utility class
  }


  /**
   * An helper job to transform a {@link Value} result.
   * @param <F> input value.
   * @param <T> transformed value.
   */
  public static class Transform<F, T> extends Job1<T, F> {

    private static final long serialVersionUID = 1280795955105207728L;
    private Function<F, T> function;

    public Transform(Function<F, T> function) {
      Preconditions.checkArgument(function instanceof Serializable, "Function not serializable");
      this.function = function;
    }

    @Override
    public Value<T> run(F from) throws Exception {
      return immediate(function.apply(from));
    }
  }

  private static class WaitForAllJob<T> extends Job1<T, T> {

    private static final long serialVersionUID = 3151677893523195265L;

    @Override
    public Value<T> run(T value) {
      return immediate(value);
    }
  }

  public static JobSetting.WaitForSetting[] createWaitForSettingArray(Value<?>... values) {
    JobSetting.WaitForSetting[] settings = new JobSetting.WaitForSetting[values.length];
    int i = 0;
    for (Value<?> value : values) {
      settings[i++] = waitFor(value);
    }
    return settings;
  }

  public static <T> Value<T> waitForAll(Job<?> caller, Value<T> value, Value<?>... values) {
    return caller.futureCall(new WaitForAllJob<T>(), value, createWaitForSettingArray(values));
  }

  public static <T> Value<T> waitForAll(Job<?> caller, T value, Value<?>... values) {
    return caller.futureCall(new WaitForAllJob<T>(), immediate(value),
        createWaitForSettingArray(values));
  }


  // this is somewhat nonsensical operation; semantics are that caller waits, within its run() method, for some values
  // and deletes its pipeline records.
  // but conceptually, this is within the "run()" method of the caller ... so if deletion is immediate, the pipeline
  // framework's executor (PipelineManager) can't write the completion state of caller to datastore, bc records are
  // gone
  // this worked historically in Google's implementation of PipelineFramework bc actual deletion of pipeline records
  // was async via queued task outside of pipelines, executed with 10s delay, giving a window for FW executor to
  // complete run() and write state before records deleted.
  // this method working is in practice coupled to that delay AND presumes something about the FW's execution behavior

  // a better name for this method would be `waitForAllAndScheduleDeletion`
  @Deprecated // not supported; could in theory corrupt any real pipeline you use it in
  public static <T> Value<T> waitForAllAndDelete(
      Job<?> caller, Value<T> value, Value<?>... values) {
    return caller.futureCall(
        new DeletePipelineJob<T>(caller.getPipelineKey().toUrlSafe()),
        value, createWaitForSettingArray(values));
  }

  @Deprecated // not supported
  public static <T> Value<T> waitForAllAndDelete(Job<?> caller, T value, Value<?>... values) {
    throw new UnsupportedOperationException("Not supported");

    //return caller.futureCall(new DeletePipelineJob<T>(caller.getPipelineKey().toUrlSafe()),
    //    immediate(value), createWaitForSettingArray(values));
  }

  private static class DeletePipelineJob<T> extends Job1<T, T> {

    private static final long serialVersionUID = -5440838671291502355L;
    private static final Logger log = Logger.getLogger(DeletePipelineJob.class.getName());

    //URL-safe key of the root job
    private final String key;

    DeletePipelineJob(@NonNull String rootJobKey) {
      Key.fromUrlSafe(rootJobKey); //validate key (throws IllegalArgumentException if bad)
      this.key = rootJobKey;
    }

    @SneakyThrows
    @Override
    public Value<T> run(T value) {

      // TODO: ugh, they're using a deferred task to delete the pipeline, so we have to somehow serialize the pipelineRunner
      DeferredTask deleteRecordsTask = new DeferredTask() {
        private static final long serialVersionUID = -7510918963650055768L;

        @Override
        public void run() {
          try {
            log.info("Deleting pipeline: " + key);
            getPipelineRunner().deletePipelineRecords(key, false);
            log.info("Deleted pipeline: " + key);
          } catch (IllegalStateException e) {
            log.info("Failed to delete pipeline: " + key);
            // only dep on javax servlet
            // how can we access request context otherwise
            HttpServletRequest request = DeferredTaskContext.getCurrentRequest();
            if (request != null) {
              int attempts = request.getIntHeader("X-AppEngine-TaskExecutionCount");
              if (attempts <= 5) {
                log.info("Request to retry deferred task #" + attempts);
                DeferredTaskContext.markForRetry();
                return;
              }
            }
            try {
              getPipelineRunner().deletePipelineRecords(key, true);
              log.info("Force deleted pipeline: " + key);
            } catch (Exception ex) {
              log.log(Level.WARNING, "Failed to force delete pipeline: " + key, ex);
            }
          } catch (NoSuchObjectException e) {
            // Already done
          }
        }
      };

      //
      Thread.sleep(10_000);

      deleteRecordsTask.run();

      // TODO: previous behavior enqueued this. recover that behavior? or do we care?
      //String queueName = Optional.fromNullable(getOnQueue()).or("default");
      //Queue queue = QueueFactory.getQueue(queueName);
      //queue.add(TaskOptions.Builder.withPayload(deleteRecordsTask).countdownMillis(10000)
      //    .retryOptions(RetryOptions.Builder.withMinBackoffSeconds(2).maxBackoffSeconds(20)));
      return immediate(value);
    }
  }
}
