// Copyright 2014 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce;

import com.google.appengine.tools.mapreduce.impl.BaseContext;
import com.google.appengine.tools.mapreduce.impl.CountersImpl;
import com.google.appengine.tools.mapreduce.impl.MapOnlyShardTask;
import com.google.appengine.tools.mapreduce.impl.WorkerController;
import com.google.appengine.tools.mapreduce.impl.WorkerShardTask;
import com.google.appengine.tools.mapreduce.impl.pipeline.ExamineStatusAndReturnResult;
import com.google.appengine.tools.mapreduce.impl.pipeline.ResultAndStatus;
import com.google.appengine.tools.mapreduce.impl.pipeline.ShardedJob;
import com.google.appengine.tools.mapreduce.impl.shardedjob.ShardedJobRunId;
import com.google.appengine.tools.mapreduce.impl.shardedjob.ShardedJobSettings;
import com.google.appengine.tools.pipeline.FutureValue;
import com.google.appengine.tools.pipeline.Job0;
import com.google.appengine.tools.pipeline.JobSetting;
import com.google.appengine.tools.pipeline.PromisedValue;
import com.google.appengine.tools.pipeline.Value;
import com.google.cloud.datastore.DatastoreOptions;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import lombok.extern.java.Log;

import java.io.IOException;
import java.io.Serial;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.logging.Level;
import java.util.logging.Logger;

import static com.google.appengine.tools.pipeline.impl.PipelineManager.DEFAULT_QUEUE_NAME;


/**
 * A Pipeline job that runs a map jobs.
 *
 * @param <I> type of input values``
 * @param <O> type of output values
 * @param <R> type of final result
 */
@Log
public class MapJob<I, O, R> extends Job0<MapReduceResult<R>> {

  @Serial
  private static final long serialVersionUID = 1L;

  private final MapSpecification<I, O, R> specification;
  private final ShardedJobAbstractSettings settings;

  public MapJob(MapSpecification<I, O, R> specification, MapSettings settings) {
    this.specification = specification;
    this.settings = settings;
  }

  /**
   * Starts a {@link MapJob} with the given parameters in a new Pipeline.
   * Returns the pipeline id.
   */
  @Deprecated
  public static <I, O, R> String start(MapSpecification<I, O, R> specification,
      MapSettings settings) {
    throw new UnsupportedOperationException("Use PipelineOrchestrator:start");

  }

  @Override
  public Value<MapReduceResult<R>> run() {
    ShardedJobAbstractSettings settings = this.settings;
    if (settings.getWorkerQueueName() == null) {
      String queue = getOnQueue();
      if (queue == null) {
        log.warning("workerQueueName is null and current queue is not available in the pipeline"
            + " job, using 'default'");
        queue = DEFAULT_QUEUE_NAME;
      }
      if (settings instanceof MapSettings) {
        settings = ((MapSettings) settings).withWorkerQueueName(queue);
      } else {
        settings = ((MapReduceSettings) settings).withWorkerQueueName(queue);
      }
    }
    ShardedJobRunId jobId = getShardedJobId();
    Context context = new BaseContext(jobId);
    Input<I> input = specification.getInput();
    input.setContext(context);
    List<? extends InputReader<I>> readers;
    try {
      readers = input.createReaders();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    Output<O, R> output = specification.getOutput();
    output.setContext(context);
    List<? extends OutputWriter<O>> writers = output.createWriters(readers.size());
    Preconditions.checkState(readers.size() == writers.size(), "%s: %s readers, %s writers",
        jobId, readers.size(), writers.size());
    ImmutableList.Builder<WorkerShardTask<I, O, MapOnlyMapperContext<O>>> mapTasks =
        ImmutableList.builder();
    for (int i = 0; i < readers.size(); i++) {
      mapTasks.add(new MapOnlyShardTask<>(jobId, i, readers.size(), readers.get(i),
          specification.getMapper(), writers.get(i), settings.getMillisPerSlice()));
    }
    ShardedJobSettings shardedJobSettings = ShardedJobSettings.from(getPipelineService(), settings, getShardedJobId(), getPipelineRunId());
    PromisedValue<ResultAndStatus<R>> resultAndStatus = newPromise();
    WorkerController<I, O, R, MapOnlyMapperContext<O>> workerController = new WorkerController<>(
        jobId, new CountersImpl(), output, resultAndStatus.getHandle());
    ShardedJob<?> shardedJob =
        new ShardedJob<>(jobId, mapTasks.build(), workerController, shardedJobSettings);
    FutureValue<Void> shardedJobResult = futureCall(shardedJob, settings.toJobSettings());
    JobSetting[] jobSetting = settings.toJobSettings(waitFor(shardedJobResult),
            statusConsoleUrl(shardedJobSettings.getMapReduceStatusUrl()), maxAttempts(1));
    return futureCall(new ExamineStatusAndReturnResult<R>(jobId), resultAndStatus, jobSetting);
  }

  /**
   * @param ex The cancellation exception
   */
  public Value<MapReduceResult<R>> handleException(CancellationException ex) {
    getPipelineOrchestrator().abortJob(getShardedJobId());
    return null;
  }

  /**
   * @return shardedJobId for this job
   */
  private ShardedJobRunId getShardedJobId() {
    DatastoreOptions defaultDatastoreOptions = DatastoreOptions.getDefaultInstance();

    return ShardedJobRunId.of(
      java.util.Optional.ofNullable(settings.getProjectId()).orElseGet(defaultDatastoreOptions::getProjectId),
      java.util.Optional.ofNullable(settings.getDatabaseId()).orElseGet(defaultDatastoreOptions::getDatabaseId),
      java.util.Optional.ofNullable(settings.getNamespace()).orElseGet(defaultDatastoreOptions::getNamespace),
      getJobRunId().getJobId());
  }

  public Value<MapReduceResult<R>> handleException(Throwable t) throws Throwable {
    log.log(Level.SEVERE, "MapJob failed because of: ", t);
    throw t;
  }

  @Override
  public String getJobDisplayName() {
    return Optional.fromNullable(specification.getJobName()).or(super.getJobDisplayName());
  }
}
