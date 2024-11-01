package com.google.appengine.tools.mapreduce.impl;


import com.google.appengine.tools.mapreduce.Counters;
import com.google.appengine.tools.mapreduce.Output;
import com.google.appengine.tools.mapreduce.OutputWriter;
import com.google.appengine.tools.mapreduce.WorkerContext;
import com.google.appengine.tools.mapreduce.impl.pipeline.ResultAndStatus;
import com.google.appengine.tools.mapreduce.impl.shardedjob.ShardedJobController;
import com.google.appengine.tools.mapreduce.impl.shardedjob.ShardedJobRunId;
import com.google.appengine.tools.mapreduce.impl.shardedjob.Status;
import com.google.appengine.tools.pipeline.NoSuchObjectException;
import com.google.appengine.tools.pipeline.OrphanedObjectException;
import com.google.appengine.tools.pipeline.PipelineService;
import com.google.appengine.tools.pipeline.SlotId;
import com.google.common.collect.ImmutableList;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.extern.java.Log;

import java.io.IOException;
import java.io.Serial;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Logger;

@Log(topic = "com.google.appengine.tools.mapreduce.impl.WorkerController")
@RequiredArgsConstructor
public class WorkerController<I, O, R, C extends WorkerContext<O>> extends
    ShardedJobController<WorkerShardTask<I, O, C>> {

  @Serial
  private static final long serialVersionUID = 1L;

  @NonNull private final ShardedJobRunId mrJobId;
  @NonNull private final Counters totalCounters;
  @NonNull private final Output<O, R> output;
  @NonNull private final SlotId resultPromiseHandle;

  @Getter @Setter
  private transient PipelineService pipelineService;

  @Override
  public void completed(Iterator<WorkerShardTask<I, O, C>> workers) {
    ImmutableList.Builder<OutputWriter<O>> outputWriters = ImmutableList.builder();
    List<Counters> counters = new ArrayList<>();
    while (workers.hasNext()) {
      WorkerShardTask<I, O, C> worker = workers.next();
      if (worker.wasFinalized()) {
        log.info("Detected a finalized worker. Will ignore this, repeated, job completed call.");
        return;
      }
      outputWriters.add(worker.getOutputWriter());
      counters.add(worker.getContext().getCounters());
    }
    output.setContext(new BaseContext(mrJobId));
    R outputResult;
    try {
      outputResult = output.finish(outputWriters.build());
    } catch (IOException e) {
      throw new RuntimeException(output + ".finish() threw IOException");
    }
    // Total the counters only after {@link Output#finish} to capture any updates made by it
    for (Counters counter : counters) {
      totalCounters.addAll(counter);
    }
    Status status = new Status(Status.StatusCode.DONE);
    ResultAndStatus<R> resultAndStatus =
      new ResultAndStatus<>(new MapReduceResultImpl<>(outputResult, totalCounters), status);
    submitPromisedJob(resultAndStatus);
  }

  @Override
  public void failed(Status status) {
    submitPromisedJob(new ResultAndStatus<R>(null, status));
  }

  private void submitPromisedJob(final ResultAndStatus<R> resultAndStatus) {
    try {
      getPipelineService().submitPromisedValue(resultPromiseHandle, resultAndStatus);
    } catch (OrphanedObjectException e) {
      log.warning("Discarding an orphaned promiseHandle: " + resultPromiseHandle);
    } catch (NoSuchObjectException e) {
      // Let taskqueue retry.
      throw new RuntimeException(resultPromiseHandle + ": Handle not found, can't submit "
          + resultAndStatus + " going to retry.", e);
    }
  }
}
