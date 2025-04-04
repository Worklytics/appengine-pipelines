// Copyright 2012 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce.impl;

import static com.google.appengine.tools.mapreduce.CounterNames.MAPPER_CALLS;
import static com.google.appengine.tools.mapreduce.CounterNames.MAPPER_WALLTIME_MILLIS;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.appengine.tools.mapreduce.InputReader;
import com.google.appengine.tools.mapreduce.KeyValue;
import com.google.appengine.tools.mapreduce.Mapper;
import com.google.appengine.tools.mapreduce.MapperContext;
import com.google.appengine.tools.mapreduce.OutputWriter;
import com.google.appengine.tools.mapreduce.Worker;
import com.google.appengine.tools.mapreduce.impl.shardedjob.ShardedJobRunId;
import com.google.appengine.tools.mapreduce.impl.shardedjob.Status;
import lombok.NonNull;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serial;

/**
 * @author ohler@google.com (Christian Ohler)
 *
 * @param <I> type of input values consumed by this mapper
 * @param <K> type of intermediate keys produced by this mapper
 * @param <V> type of intermediate values produced by this mapper
 */
public class MapShardTask<I, K, V> extends WorkerShardTask<I, KeyValue<K, V>, MapperContext<K, V>> {

  @Serial
  private static final long serialVersionUID = 978040803132974582L;

  private Mapper<I, K, V> mapper;
  private final long millisPerSlice;
  private InputReader<I> in;
  private OutputWriter<KeyValue<K, V>> out;

  private transient MapperContextImpl<K, V> context;

  public MapShardTask(@NonNull ShardedJobRunId mrJobId,
                      int shardNumber,
                      int shardCount,
                      @NonNull InputReader<I> in,
                      @NonNull Mapper<I, K, V> mapper,
                      @NonNull OutputWriter<KeyValue<K, V>> out,
                      long millisPerSlice,
                      WorkerRunSettings workerRunSettings) {
    super(new IncrementalTaskContext(mrJobId, shardNumber, shardCount, MAPPER_CALLS,
        MAPPER_WALLTIME_MILLIS), workerRunSettings  );
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
  protected Worker<MapperContext<K, V>> getWorker() {
    return mapper;
  }

  @Override
  public OutputWriter<KeyValue<K, V>> getOutputWriter() {
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
    context = new MapperContextImpl<>(getContext(), out);
    in.setContext(context);
    out.setContext(context);
    mapper.setContext(context);
  }
}
