// Copyright 2014 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce.impl;

import static com.google.appengine.tools.mapreduce.CounterNames.MAPPER_CALLS;
import static com.google.appengine.tools.mapreduce.CounterNames.MAPPER_WALLTIME_MILLIS;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.appengine.tools.mapreduce.InputReader;
import com.google.appengine.tools.mapreduce.MapOnlyMapper;
import com.google.appengine.tools.mapreduce.MapOnlyMapperContext;
import com.google.appengine.tools.mapreduce.OutputWriter;
import com.google.appengine.tools.mapreduce.Worker;
import com.google.appengine.tools.mapreduce.impl.shardedjob.ShardedJobRunId;
import com.google.appengine.tools.mapreduce.impl.shardedjob.Status;
import lombok.NonNull;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serial;

/**
 * @param <I> type of input values consumed by this mapper
 * @param <O> type of output values produced by this mapper
 */
public class MapOnlyShardTask<I, O> extends WorkerShardTask<I, O, MapOnlyMapperContext<O>> {

  @Serial
  private static final long serialVersionUID = 7235772804361681386L;

  private MapOnlyMapper<I, O> mapper;
  private final long millisPerSlice;
  private InputReader<I> in;
  private OutputWriter<O> out;

  private transient MapOnlyMapperContextImpl<O> context;

  public MapOnlyShardTask(ShardedJobRunId mrJobId,
                          int shardNumber,
                          int shardCount,
                          @NonNull InputReader<I> in,
                          @NonNull MapOnlyMapper<I, O> mapper,
                          @NonNull OutputWriter<O> out,
                          long millisPerSlice,
                          WorkerRunSettings workerRunSettings) {
    super(new IncrementalTaskContext(mrJobId, shardNumber, shardCount, MAPPER_CALLS,
        MAPPER_WALLTIME_MILLIS), workerRunSettings);
    this.in = in;
    this.out = out;
    this.mapper = mapper;
    this.millisPerSlice = millisPerSlice;
    fillContext();
  }

  @Override
  protected void callWorker(I input) {
    mapper.map(input);
  }

  @Override
  protected String formatLastWorkItem(I item) {
    return abbrev(item);
  }

  @Override
  protected boolean shouldCheckpoint(long timeElapsed) {
    return timeElapsed >= millisPerSlice;
  }

  @Override
  protected long estimateMemoryRequirement() {
    return in.estimateMemoryRequirement() + out.estimateMemoryRequirement()
        + mapper.estimateMemoryRequirement();
  }

  @Override
  protected Worker<MapOnlyMapperContext<O>> getWorker() {
    return mapper;
  }

  @Override
  public OutputWriter<O> getOutputWriter() {
    return out;
  }

  @Override
  public InputReader<I> getInputReader() {
    return in;
  }

  @Override
  public boolean allowSliceRetry(boolean abandon) {
    boolean skipWriterCheck = !abandon && !context.emitCalled();
    return (skipWriterCheck || out.allowSliceRetry()) && mapper.allowSliceRetry();
  }

  @Override
  public void jobCompleted(Status status) {
    mapper = null;
    in = null;
    if (out != null) {
      out.cleanup();
      out = null;
    }
    setFinalized();
  }

  private void readObject(ObjectInputStream stream) throws IOException, ClassNotFoundException {
    stream.defaultReadObject();
    if (!wasFinalized()) {
      fillContext();
    }
  }

  private void fillContext() {
    context = new MapOnlyMapperContextImpl<>(getContext(), out);
    in.setContext(context);
    out.setContext(context);
    mapper.setContext(context);
  }
}
