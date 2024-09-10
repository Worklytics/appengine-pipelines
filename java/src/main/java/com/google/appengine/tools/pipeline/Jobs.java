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

import com.google.appengine.api.taskqueue.*;
import com.google.appengine.tools.pipeline.impl.backend.AppEngineBackEnd;
import com.google.appengine.tools.pipeline.impl.backend.PipelineBackEnd;
import com.google.cloud.datastore.Key;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.java.Log;

import java.io.Serializable;
import java.util.Optional;
import java.util.logging.Level;

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
  // in practice, this is ONLY used in tests and via the pipeline UX (to delete job records)
  @Deprecated // not supported; could in theory corrupt any real pipeline you use it in
  public static <T> Value<T> waitForAllAndDelete(
      Job<?> caller, Value<T> value, Value<?>... values) {
    throw new UnsupportedOperationException("Not supported");
//    return caller.futureCall(
//        new DeletePipelineJob<>(caller.getPipelineKey(), 10_000L),
//        value, createWaitForSettingArray(values));
  }

  /**
   * slightly more semantically correct version of the above
   * @param caller job from which this is called; its records will be deleted too!!
   * @param delayMillis delay to delete with
   * @param value to wait on
   * @param values more values to wait on
   * @return finalized actual value of {@code value}
   * @param <T>
   */
  public static <T> Value<T> waitForAllAndDeleteWithDelay(Job<?> caller, Long delayMillis, Value<T> value, Value<?>... values) {
    return caller.futureCall(new DeletePipelineJob<>(caller.getPipelineKey(), delayMillis), value, createWaitForSettingArray(values));
  }

  @Deprecated // not supported
  public static <T> Value<T> waitForAllAndDelete(Job<?> caller, T value, Value<?>... values) {
    throw new UnsupportedOperationException("Not supported");

    //return caller.futureCall(new DeletePipelineJob<T>(caller.getPipelineKey().toUrlSafe()),
    //    immediate(value), createWaitForSettingArray(values));
  }

  /**
   * *attempt* to delete pipeline; successful completion of this job is NOT a guarantee of deletion, as deletion done
   * asynchronously via Cloud Task queue, as fire+forget
   *
   * this would be more correctly called 'EnqueuePipelineDeletionJob' or 'SchedulePipelineDeletionJob', bc that's all
   * completion of it ensures
   *
   * @param <T>
   */
  @RequiredArgsConstructor
  @Log
  private static class DeletePipelineJob<T> extends Job1<T, T> {

    private static final long serialVersionUID = 1L;

    /**
     * key of the root job of the pipeline to delete
     */
    @NonNull private final Key rootPipelineKey;

    /**
     * delay before deletion attempt, in milliseconds; historically, this was always 10s
     */
    @NonNull
    private final Long delayMillis;

    @SneakyThrows
    @Override
    public Value<T> run(T value) {

      //something that can be serialized
      PipelineBackEnd.Options options = getPipelineBackendOptions();

      DeferredTask deleteRecordsTask = new DeferredTask() {
        private static final long serialVersionUID = -7510918963650055768L;

        @Override
        public void run() {
          //recover backend from serialized options; in theory, all we *should* need is datastoreOptions part
          AppEngineBackEnd backend = new AppEngineBackEnd(options.as(AppEngineBackEnd.Options.class));

          try {
            log.info("Deleting pipeline: " + rootPipelineKey);
            backend.deletePipeline(rootPipelineKey, false);
            log.info("Deleted pipeline: " + rootPipelineKey);
          } catch (IllegalStateException e) {
            log.info("Failed to delete pipeline: " + rootPipelineKey);
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
              backend.deletePipeline(rootPipelineKey, true);
              log.info("Force deleted pipeline: " + rootPipelineKey);
            } catch (Exception ex) {
              log.log(Level.WARNING, "Failed to force delete pipeline: " + rootPipelineKey, ex);
            }
          }
        }
      };

      //NOTE: this MUST be async as long as this DeleteJob is called from within the same pipeline you're deleting
      // (which as of 2024-05, is how this is always being used)
      String queueName = Optional.ofNullable(getOnQueue()).orElse("default");
      Queue queue = QueueFactory.getQueue(queueName);
      queue.add(TaskOptions.Builder.withPayload(deleteRecordsTask).countdownMillis(delayMillis)
          .retryOptions(RetryOptions.Builder.withMinBackoffSeconds(2).maxBackoffSeconds(20)));

      return immediate(value);
    }
  }
}
