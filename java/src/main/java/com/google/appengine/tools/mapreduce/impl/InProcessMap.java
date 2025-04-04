// Copyright 2014 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce.impl;

import com.google.appengine.tools.mapreduce.Counters;
import com.google.appengine.tools.mapreduce.Input;
import com.google.appengine.tools.mapreduce.InputReader;
import com.google.appengine.tools.mapreduce.MapOnlyMapper;
import com.google.appengine.tools.mapreduce.MapOnlyMapperContext;
import com.google.appengine.tools.mapreduce.MapReduceResult;
import com.google.appengine.tools.mapreduce.MapSpecification;
import com.google.appengine.tools.mapreduce.Output;
import com.google.appengine.tools.mapreduce.OutputWriter;
import com.google.appengine.tools.mapreduce.impl.shardedjob.InProcessShardedJobRunner;
import com.google.appengine.tools.mapreduce.impl.shardedjob.ShardedJobController;
import com.google.appengine.tools.mapreduce.impl.shardedjob.ShardedJobRunId;
import com.google.appengine.tools.mapreduce.impl.shardedjob.Status;
import com.google.appengine.tools.pipeline.PipelineService;
import com.google.appengine.tools.pipeline.impl.util.SerializationUtils;
import com.google.common.collect.ImmutableList;

import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.SneakyThrows;

import java.io.IOException;
import java.time.Instant;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.logging.Logger;

/**
 * Runs a Map only in the current process. Only for very small datasets. Easier to debug than a
 * parallel MapReduce.
 *
 * @param <I> type of input values
 * @param <O> type of output values
 * @param <R> type of result returned by the {@code Output}
 */
public class InProcessMap<I, O, R> {

  private static final Logger log = Logger.getLogger(InProcessMap.class.getName());

  @NonNull
  private final ShardedJobRunId id;
  private final Input<I> input;
  private final MapOnlyMapper<I, O> mapper;
  private final Output<O, R> output;
  private final PipelineService pipelineService;

  @SuppressWarnings("unchecked")
  public InProcessMap(PipelineService pipelineService,
                      @NonNull ShardedJobRunId id,
                      MapSpecification<I, O, R> mapSpec) {
    this.id = id;
    input = InProcessUtil.getInput(mapSpec);
    mapper = InProcessUtil.getMapper(mapSpec);
    output = InProcessUtil.getOutput(mapSpec);
    this.pipelineService = pipelineService;
  }

  @Override
  public String toString() {
    return "InProcessMapOnly.Impl(" + id + ")";
  }

  private MapReduceResultImpl<R> map() throws IOException {
    log.info("Map started");
    List<? extends InputReader<I>> readers = input.createReaders();
    List<? extends OutputWriter<O>> writers = output.createWriters(readers.size());
    ImmutableList.Builder<WorkerShardTask<I, O , MapOnlyMapperContext<O>>> tasks =
        ImmutableList.builder();
    for (int shard = 0; shard < readers.size(); shard++) {
      WorkerShardTask<I, O, MapOnlyMapperContext<O>> task = new MapOnlyShardTask<>(
          id, shard, readers.size(), readers.get(shard), getCopyOfMapper(), writers.get(shard),
          Long.MAX_VALUE, WorkerShardTask.WorkerRunSettings.defaults());
      tasks.add(task);
    }
    final Counters counters = new CountersImpl();
    InProcessShardedJobRunner.runJob(tasks.build(), new ShardedJobController<
        WorkerShardTask<I, O, MapOnlyMapperContext<O>>>() {
          // Not really meant to be serialized, but avoid warning.
          private static final long serialVersionUID = 661198005749484951L;

          @Override
          public void failed(Status status) {
            throw new UnsupportedOperationException();
          }

          @Getter @Setter
          PipelineService pipelineService = InProcessMap.this.pipelineService;

          @Override
          public void completed(Iterator<WorkerShardTask<I, O, MapOnlyMapperContext<O>>> tasks) {
            while (tasks.hasNext()) {
              counters.addAll(tasks.next().getContext().getCounters());
            }
          }
        });
    log.info("Map completed");
    log.info("combined counters=" + counters);
    return new MapReduceResultImpl<>(output.finish(writers), counters);
  }


  @SneakyThrows
  @SuppressWarnings("unchecked")
  private MapOnlyMapper<I, O> getCopyOfMapper() {
    return SerializationUtils.clone(mapper);
  }

  private static String getMapReduceId() {
    return "in-process-map-" + Instant.now().toString().replace(":", "") + "-" + new Random().nextInt(1000000);
  }

  public static <I, O, R> MapReduceResult<R> runMap(PipelineService pipelineService,
                                                    MapSpecification<I, O, R> mrSpec)
      throws IOException {
    ShardedJobRunId id = ShardedJobRunId.of("in-process", null, null, getMapReduceId());
    InProcessMap<I,  O, R> mapOnly = new InProcessMap<>(pipelineService, id, mrSpec);
    log.info(mapOnly + " started");
    MapReduceResult<R> result = mapOnly.map();
    log.info(mapOnly + " finished");
    return result;
  }
}
