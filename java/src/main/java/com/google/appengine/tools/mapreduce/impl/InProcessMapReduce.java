// Copyright 2011 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce.impl;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.appengine.tools.mapreduce.Counters;
import com.google.appengine.tools.mapreduce.Input;
import com.google.appengine.tools.mapreduce.InputReader;
import com.google.appengine.tools.mapreduce.KeyValue;
import com.google.appengine.tools.mapreduce.MapReduceResult;
import com.google.appengine.tools.mapreduce.MapReduceSpecification;
import com.google.appengine.tools.mapreduce.Mapper;
import com.google.appengine.tools.mapreduce.MapperContext;
import com.google.appengine.tools.mapreduce.Marshaller;
import com.google.appengine.tools.mapreduce.Output;
import com.google.appengine.tools.mapreduce.OutputWriter;
import com.google.appengine.tools.mapreduce.Reducer;
import com.google.appengine.tools.mapreduce.ReducerContext;
import com.google.appengine.tools.mapreduce.impl.shardedjob.InProcessShardedJobRunner;
import com.google.appengine.tools.mapreduce.impl.shardedjob.ShardedJobController;
import com.google.appengine.tools.mapreduce.impl.shardedjob.ShardedJobRunId;
import com.google.appengine.tools.mapreduce.impl.shardedjob.Status;
import com.google.appengine.tools.mapreduce.outputs.InMemoryOutput;
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
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.logging.Logger;

/**
 * Runs a MapReduce in the current process. Only for very small datasets. Easier to debug than a
 * parallel MapReduce.
 *
 * @author ohler@google.com (Christian Ohler)
 *
 * @param <I> type of input values
 * @param <K> type of intermediate keys
 * @param <V> type of intermediate values
 * @param <O> type of output values
 * @param <R> type of result returned by the {@link Output}
 */
public class InProcessMapReduce<I, K, V, O, R> {

  private static final Logger log = Logger.getLogger(InProcessMapReduce.class.getName());

  static class MapResult<K, V> {
    private final List<List<KeyValue<K, V>>> mapShardOutputs;
    private final Counters counters;

    public MapResult(List<List<KeyValue<K, V>>> mapShardOutputs, Counters counters) {
      this.mapShardOutputs = checkNotNull(mapShardOutputs, "Null mapShardOutputs");
      this.counters = checkNotNull(counters, "Null counters");
    }

    public List<List<KeyValue<K, V>>> getMapShardOutputs() {
      return mapShardOutputs;
    }

    public Counters getCounters() {
      return counters;
    }

    @Override
    public String toString() {
      return getClass().getSimpleName() + "(" + mapShardOutputs + ", " + counters + ")";
    }
  }

  @Getter
  private final ShardedJobRunId id;
  private final Input<I> input;
  private final Mapper<I, K, V> mapper;
  private final Marshaller<K> keyMarshaller;
  private final Reducer<K, V, O> reducer;
  private final Output<O, R> output;
  private final int numReducers;
  @Getter
  private final PipelineService pipelineService;

  @SuppressWarnings("unchecked")
  public InProcessMapReduce(@NonNull ShardedJobRunId id,
                            MapReduceSpecification<I, K, V, O, R> mrSpec,
                            PipelineService pipelineService) {
    this.id = id;
    input = InProcessUtil.getInput(mrSpec);
    mapper = InProcessUtil.getMapper(mrSpec);
    keyMarshaller = InProcessUtil.getKeyMarshaller(mrSpec);
    reducer = InProcessUtil.getReducer(mrSpec);
    output = InProcessUtil.getOutput(mrSpec);
    numReducers = InProcessUtil.getNumReducers(mrSpec);
    this.pipelineService = pipelineService;

  }

  @Override
  public String toString() {
    return "InProcessMapReduce.Impl(" + id + ")";
  }

  MapReduceResultImpl<List<List<KeyValue<K, V>>>> map(
      List<? extends InputReader<I>> inputs, InMemoryOutput<KeyValue<K, V>> output) {
    log.info("Map phase started");

    ImmutableList.Builder<WorkerShardTask<I, KeyValue<K, V>, MapperContext<K, V>>> tasks =
        ImmutableList.builder();
    List<? extends OutputWriter<KeyValue<K, V>>> writers = output.createWriters(inputs.size());
    for (int shard = 0; shard < inputs.size(); shard++) {
      WorkerShardTask<I, KeyValue<K, V>, MapperContext<K, V>> task =
        new MapShardTask<>(getId(), shard, inputs.size(), inputs.get(shard), getCopyOfMapper(), writers.get(shard), Long.MAX_VALUE, WorkerShardTask.WorkerRunSettings.defaults());
      tasks.add(task);
    }
    final Counters counters = new CountersImpl();


    final PipelineService finalPipelineService = getPipelineService();
    InProcessShardedJobRunner.runJob(tasks.build(), new ShardedJobController<
        WorkerShardTask<I, KeyValue<K, V>, MapperContext<K, V>>>() {
          // Not really meant to be serialized, but avoid warning.
          private static final long serialVersionUID = 661198005749484951L;

          @Override
          public void failed(Status status) {
            throw new UnsupportedOperationException();
          }

          @Getter @Setter PipelineService pipelineService = finalPipelineService;

           @Override
          public void completed(
              Iterator<WorkerShardTask<I, KeyValue<K, V>, MapperContext<K, V>>> tasks) {
            while (tasks.hasNext()) {
              counters.addAll(tasks.next().getContext().getCounters());
            }
          }
        });
    log.info("Map phase completed");
    log.info("combined counters=" + counters);
    return new MapReduceResultImpl<>(output.finish(writers), counters);
  }


  List<List<KeyValue<K, List<V>>>> shuffle(
      List<List<KeyValue<K, V>>> mapperOutputs, int reduceShardCount) {
    log.info("Shuffle phase started");
    List<List<KeyValue<K, List<V>>>> out =
        Shuffling.shuffle(mapperOutputs, keyMarshaller, reduceShardCount);
    log.info("Shuffle phase completed");
    return out;
  }

  InputReader<KeyValue<K, Iterator<V>>> getReducerInputReader(
      final List<KeyValue<K, List<V>>> data) {
    return new InputReader<KeyValue<K, Iterator<V>>>() {
      // Not really meant to be serialized, but avoid warning.
      private static final long serialVersionUID = 310424169122893265L;
      int i = 0;

      @Override
      public Double getProgress() {
        return null;
      }

      @Override
      public KeyValue<K, Iterator<V>> next() {
        if (i >= data.size()) {
          throw new NoSuchElementException();
        }
        KeyValue<K, Iterator<V>> result =
            KeyValue.of(data.get(i).getKey(), data.get(i).getValue().iterator());
        i++;
        return result;
      }
    };
  }

  @SneakyThrows
  @SuppressWarnings("unchecked")
  private Mapper<I, K, V> getCopyOfMapper() {
    return SerializationUtils.clone(mapper);
  }

  @SneakyThrows
  @SuppressWarnings("unchecked")
  private Reducer<K, V, O> getCopyOfReducer() {
    return SerializationUtils.clone(reducer);
  }

  MapReduceResult<R> reduce(List<List<KeyValue<K, List<V>>>> inputs, Output<O, R> output,
      Counters mapCounters) throws IOException {
    List<? extends OutputWriter<O>> outputs = output.createWriters(inputs.size());
    log.info("Reduce phase started");
    ImmutableList.Builder<WorkerShardTask<KeyValue<K, Iterator<V>>, O, ReducerContext<O>>> tasks =
        ImmutableList.builder();
    for (int shard = 0; shard < outputs.size(); shard++) {
      WorkerShardTask<KeyValue<K, Iterator<V>>, O, ReducerContext<O>> task =
          new ReduceShardTask<>(id, shard, outputs.size(), getReducerInputReader(inputs.get(shard)),
              getCopyOfReducer(), outputs.get(shard), Long.MAX_VALUE, WorkerShardTask.WorkerRunSettings.defaults());
      tasks.add(task);
    }
    final Counters counters = new CountersImpl();
    counters.addAll(mapCounters);
    InProcessShardedJobRunner.runJob(tasks.build(), new ShardedJobController<
        WorkerShardTask<KeyValue<K, Iterator<V>>, O, ReducerContext<O>>>() {
      // Not really meant to be serialized, but avoid warning.
      private static final long serialVersionUID = 575338448598450119L;

      @Override
      public void failed(Status status) {
        throw new UnsupportedOperationException();
      }

      @Getter @Setter
      PipelineService pipelineService;

      @Override
      public void completed(
          Iterator<WorkerShardTask<KeyValue<K, Iterator<V>>, O, ReducerContext<O>>> tasks) {
        while (tasks.hasNext()) {
          counters.addAll(tasks.next().getContext().getCounters());
        }
      }
    });
    log.info("Reduce phase completed, reduce counters=" + counters);
    log.info("combined counters=" + counters);
    return new MapReduceResultImpl<>(output.finish(outputs), counters);
  }

  public static <I, K, V, O, R> MapReduceResult<R> runMapReduce(
    PipelineService pipelineService, MapReduceSpecification<I, K, V, O, R> mrSpec) throws IOException {
    ShardedJobRunId mapReduceId = ShardedJobRunId.of("in-process", null,  null,"in-process-mr-" + Instant.now().toString().replace(":", "") + "-" + new Random().nextInt(1000000));
    InProcessMapReduce<I, K, V, O, R> mapReduce = new InProcessMapReduce<>(mapReduceId, mrSpec, pipelineService);
    log.info(mapReduce + " started");

    List<? extends InputReader<I>> mapInput = mapReduce.input.createReaders();
    InMemoryOutput<KeyValue<K, V>> mapOutput = new InMemoryOutput<>();
    MapReduceResult<List<List<KeyValue<K, V>>>> mapResult = mapReduce.map(mapInput, mapOutput);
    List<List<KeyValue<K, List<V>>>> reducerInputs =
        mapReduce.shuffle(mapResult.getOutputResult(), mapReduce.numReducers);
    MapReduceResult<R> result =
        mapReduce.reduce(reducerInputs, mapReduce.output, mapResult.getCounters());

    log.info(mapReduce + " finished");
    return result;
  }
}
