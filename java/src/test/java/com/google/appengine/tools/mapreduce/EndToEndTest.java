// Copyright 2011 Google Inc. All Rights Reserved.
package com.google.appengine.tools.mapreduce;

import static java.nio.charset.StandardCharsets.US_ASCII;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;

import com.google.appengine.tools.mapreduce.impl.HashingSharder;
import com.google.appengine.tools.mapreduce.impl.InProcessMap;
import com.google.appengine.tools.mapreduce.impl.InProcessMapReduce;
import com.google.appengine.tools.mapreduce.impl.shardedjob.ShardFailureException;
import com.google.appengine.tools.mapreduce.inputs.ConsecutiveLongInput;
import com.google.appengine.tools.mapreduce.inputs.NoInput;
import com.google.appengine.tools.mapreduce.inputs.RandomLongInput;
import com.google.appengine.tools.mapreduce.inputs.InMemoryInput;
import com.google.appengine.tools.mapreduce.outputs.GoogleCloudStorageFileOutput;
import com.google.appengine.tools.mapreduce.outputs.GoogleCloudStorageFileOutputWriter;
import com.google.appengine.tools.mapreduce.outputs.InMemoryOutput;
import com.google.appengine.tools.mapreduce.outputs.MarshallingOutput;
import com.google.appengine.tools.mapreduce.outputs.NoOutput;
import com.google.appengine.tools.mapreduce.outputs.SizeSegmentedGoogleCloudStorageFileOutput;
import com.google.appengine.tools.mapreduce.outputs.StringOutput;
import com.google.appengine.tools.mapreduce.reducers.KeyProjectionReducer;
import com.google.appengine.tools.mapreduce.reducers.NoReducer;
import com.google.appengine.tools.mapreduce.reducers.ValueProjectionReducer;
import com.google.appengine.tools.pipeline.JobRunId;
import com.google.appengine.tools.pipeline.JobInfo;
import com.google.cloud.ReadChannel;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import lombok.RequiredArgsConstructor;
import lombok.extern.java.Log;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

/**
 * @author ohler@google.com (Christian Ohler)
 */
@Log
public class EndToEndTest extends EndToEndTestCase {

  GoogleCloudStorageFileOutput.Options cloudStorageFileOutputOptions;
  MapReduceSettings testSettings;

  @BeforeEach
  public void setUp() throws Exception {
    cloudStorageFileOutputOptions = GoogleCloudStorageFileOutput.BaseOptions.defaults()
      .withServiceAccountKey(getStorageTestHelper().getBase64EncodedServiceAccountKey())
      .withProjectId(getStorageTestHelper().getProjectId()); //prob not really needed ..
    testSettings = MapReduceSettings.builder()
      .serviceAccountKey(getStorageTestHelper().getBase64EncodedServiceAccountKey())
      .bucketName(getStorageTestHelper().getBucket())
      .build();
  }

  private interface Verifier<R> {
    void verify(MapReduceResult<R> result) throws Exception;
  }

  private class NopVerifier<R> implements Verifier<R> {
    @Override
    public void verify(MapReduceResult<R> result) {
      // Do nothing
    }
  }

  private <I, K, V, O, R> void runWithPipeline(MapReduceSettings settings,
      MapReduceSpecification<I, K, V, O, R> mrSpec, Verifier<R> verifier) throws Exception {
    JobRunId jobRunId = getPipelineService().startNewPipeline(new MapReduceJob<>(mrSpec, settings));
    executeTasksUntilEmpty("default");
    JobInfo info = getPipelineService().getJobInfo(jobRunId);
    @SuppressWarnings("unchecked")
    MapReduceResult<R> result = (MapReduceResult<R>) info.getOutput();
    assertEquals(JobInfo.State.COMPLETED_SUCCESSFULLY, info.getJobState());
    assertNotNull(result);
    verifier.verify(result);
  }

  private <I, K, V, O, R> void runTest(MapReduceSpecification<I, K, V, O, R> mrSpec,
      Verifier<R> verifier) throws Exception {
    runTest(testSettings, mrSpec, verifier);
  }

  private <I, K, V, O, R> void runTest(MapReduceSettings settings,
      MapReduceSpecification<I, K, V, O, R> mrSpec, Verifier<R> verifier) throws Exception {
    verifier.verify(InProcessMapReduce.runMapReduce(getPipelineService(), mrSpec));
    runWithPipeline(settings, mrSpec, verifier);
  }

  private <I, O, R> void runWithPipeline(MapSettings settings, MapSpecification<I, O, R> mrSpec,
      Verifier<R> verifier) throws Exception {
    JobRunId jobRunId = getPipelineService().startNewPipeline(new MapJob<>(mrSpec, settings));
    executeTasksUntilEmpty("default");
    JobInfo info = getPipelineService().getJobInfo(jobRunId);
    @SuppressWarnings("unchecked")
    MapReduceResult<R> result = (MapReduceResult<R>) info.getOutput();
    assertEquals(JobInfo.State.COMPLETED_SUCCESSFULLY, info.getJobState());
    assertNotNull(result);
    verifier.verify(result);
  }

  private <I, O, R> void runTest(MapSpecification<I, O, R> mrSpec, Verifier<R> verifier)
      throws Exception {
    runTest(mrSpec, MapSettings.builder()
      .datastoreHost(getDatastore().getOptions().getHost())
      .projectId(getDatastore().getOptions().getProjectId())
      .databaseId(getDatastore().getOptions().getDatabaseId())
      .namespace(getDatastore().getOptions().getNamespace())
      .build(), verifier);
  }

  private <I, O, R> void runTest(MapSpecification<I, O, R> mrSpec, MapSettings settings,
      Verifier<R> verifier) throws Exception {
    verifier.verify(InProcessMap.runMap(getPipelineService(), mrSpec));
    runWithPipeline(settings, mrSpec, verifier);
  }

  public static class MapOnly extends MapOnlyMapper<Long, String> {

    private static final long serialVersionUID = 1L;

    @Override
    public void map(Long value) {
      emit(String.valueOf(value));
    }
  }

  @Test
  public void testMapOnlyJob() throws Exception {
    RandomLongInput input = new RandomLongInput(100, 2);
    InMemoryOutput<String> output = new InMemoryOutput<>();
    MapSpecification<Long, String, List<List<String>>> specification =
        new MapSpecification.Builder<>(input, new MapOnly(), output).setJobName("mr-only").build();
    runTest(specification, new Verifier<List<List<String>>>() {
      @Override
      public void verify(MapReduceResult<List<List<String>>> result) throws Exception {
        Counters counters = result.getCounters();
        assertEquals(100, counters.getCounter(CounterNames.MAPPER_CALLS).getValue());
        assertEquals(0, counters.getCounter(CounterNames.REDUCER_CALLS).getValue());
        List<List<String>> outputResult = result.getOutputResult();
        assertEquals(2, outputResult.size());
        assertEquals(50, outputResult.get(0).size());
        assertEquals(50, outputResult.get(1).size());
      }
    });
  }

  // The first file in every shard was getting overwritten and two references to the same files were
  // being returned. This was happening because the filecount was getting reset at the time of
  // serialization of MapJob.
  @Test
  public void testMapOnlyJobWithSizeSegmentedOutput() throws Exception {
    String mimeType = "application/json";
    String fileNamePattern = "MapOnlySegmentingTestShard-%04d/file-%04d";

    SizeSegmentedGoogleCloudStorageFileOutput output =
        new SizeSegmentedGoogleCloudStorageFileOutput(getStorageTestHelper().getBucket(), 30, fileNamePattern, mimeType, cloudStorageFileOutputOptions);
    MarshallingOutput<String, GoogleCloudStorageFileSet> op =
        new MarshallingOutput<>(output, Marshallers.getStringMarshaller());

    MapSpecification<Long, String, GoogleCloudStorageFileSet> spec = new MapSpecification.Builder<>(
        new ConsecutiveLongInput(0, 100, 1), new MapOnly(), op).setJobName("Test job").build();

    runTest(spec, new Verifier<GoogleCloudStorageFileSet>() {
      @Override
      public void verify(MapReduceResult<GoogleCloudStorageFileSet> result) throws Exception {
        List<GcsFilename> files = result.getOutputResult().getFiles();
        List<String> fileNames = new ArrayList<>();
        for (GcsFilename file : files) {
          fileNames.add(file.toString());
        }
        HashSet<String> uniqueNames = Sets.newHashSet(fileNames);
        assertEquals(fileNames.size(), uniqueNames.size());
      }
    });
  }

  public static class VoidKeyMapper extends Mapper<Long, Void, String> {

    private static final long serialVersionUID = 1L;
    private final AtomicBoolean[] beginShard = {new AtomicBoolean(), new AtomicBoolean()};
    private final AtomicBoolean[] endShard = {new AtomicBoolean(), new AtomicBoolean()};
    private final AtomicBoolean[] inSlice = {new AtomicBoolean(), new AtomicBoolean()};
    private final AtomicBoolean[] memory = {new AtomicBoolean(), new AtomicBoolean()};

    @Override
    public void beginShard() {
      MapperContext<Void, String> ctx = getContext();
      assertNotNull(ctx.getJobId());
      assertEquals(2, ctx.getShardCount());
      int shard = ctx.getShardNumber();
      assertFalse(inSlice[shard].get());
      assertFalse(endShard[shard].get());
      beginShard[shard].set(true);
    }

    @Override
    public void beginSlice() {
      MapperContext<Void, String> ctx = getContext();
      int shard = ctx.getShardNumber();
      assertFalse(inSlice[shard].get());
      assertFalse(endShard[shard].get());
      assertTrue(beginShard[shard].get());
      inSlice[shard].set(true);
    }

    @Override
    public void endSlice() {
      MapperContext<Void, String> ctx = getContext();
      int shard = ctx.getShardNumber();
      assertTrue(inSlice[shard].get());
      assertTrue(beginShard[shard].get());
      assertFalse(endShard[shard].get());
      inSlice[shard].set(false);
    }

    @Override
    public void endShard() {
      MapperContext<Void, String> ctx = getContext();
      int shard = ctx.getShardNumber();
      assertFalse(inSlice[shard].get());
      assertTrue(beginShard[ctx.getShardNumber()].get());
      assertFalse(endShard[shard].get());
      beginShard[shard].set(false);
      endShard[shard].set(true);
    }

    @Override
    public long estimateMemoryRequirement() {
      MapperContext<Void, String> ctx = getContext();
      int shard = ctx.getShardNumber();
      memory[shard].set(true);
      return 0;
    }

    @Override
    public void map(Long value) {
      MapperContext<Void, String> ctx = getContext();
      int shard = ctx.getShardNumber();
      assertTrue(inSlice[shard].get());
      assertTrue(beginShard[shard].get());
      assertFalse(endShard[shard].get());
      assertTrue(memory[shard].get());
      emit(null, String.valueOf(value));
      getContext().incrementCounter("my-counter");
    }
  }

  @Test
  public void testAdaptedMapOnlyJob() throws Exception {
    RandomLongInput input = new RandomLongInput(100, 2);
    InMemoryOutput<String> output = new InMemoryOutput<>();
    MapOnlyMapper<Long, String> mapper = MapOnlyMapper.forMapper(new VoidKeyMapper());
    MapSpecification<Long, String, List<List<String>>> specification =
        new MapSpecification.Builder<>(input, mapper, output).setJobName("adapted-mr-only").build();
    runTest(specification, new Verifier<List<List<String>>>() {
      @Override
      public void verify(MapReduceResult<List<List<String>>> result) throws Exception {
        Counters counters = result.getCounters();
        assertEquals(100, counters.getCounter(CounterNames.MAPPER_CALLS).getValue());
        assertEquals(0, counters.getCounter(CounterNames.REDUCER_CALLS).getValue());
        assertEquals(100, counters.getCounter("my-counter").getValue());
        List<List<String>> outputResult = result.getOutputResult();
        assertEquals(2, outputResult.size());
        assertEquals(50, outputResult.get(0).size());
        assertEquals(50, outputResult.get(1).size());
      }
    });
  }

  @Test
  public void testSomeShardsEmpty() throws Exception {
    runTest(new MapReduceSpecification.Builder<>(new ConsecutiveLongInput(0, 5, 3),
        new Mod37Mapper(), ValueProjectionReducer.<String, Long>create(),
        new InMemoryOutput<Long>())
        .setKeyMarshaller(Marshallers.getStringMarshaller())
        .setValueMarshaller(Marshallers.getLongMarshaller())
        .setJobName("Empty test MR")
        .setNumReducers(2)
        .build(), new Verifier<List<List<Long>>>() {
      @Override
      public void verify(MapReduceResult<List<List<Long>>> result) throws Exception {
        assertNotNull(result.getOutputResult());
        assertEquals(5, result.getCounters().getCounter(CounterNames.MAPPER_CALLS).getValue());
        assertEquals(5, result.getCounters().getCounter(CounterNames.REDUCER_CALLS).getValue());
        HashSet<Long> allOutput = new HashSet<>();
        for (List<Long> output : result.getOutputResult()) {
          allOutput.addAll(output);
        }
        assertEquals(5, allOutput.size());
      }
    });
  }

  @Test
  public void testMoreReducersThanFanout() throws Exception {
    MapReduceSpecification<Long, String, Long, Long, List<List<Long>>> spec =
        new MapReduceSpecification.Builder<>(new ConsecutiveLongInput(0, 50000, 10),
            new Mod37Mapper(), ValueProjectionReducer.<String, Long>create(),
            new InMemoryOutput<>())
            .setKeyMarshaller(Marshallers.getStringMarshaller())
            .setValueMarshaller(Marshallers.getLongMarshaller())
            .setJobName("Empty test MR")
            .setNumReducers(2)
            .build();
    Verifier<List<List<Long>>> verifier = new Verifier<List<List<Long>>>() {
      @Override
      public void verify(MapReduceResult<List<List<Long>>> result) throws Exception {
        assertNotNull(result.getOutputResult());
        assertEquals(50000, result.getCounters().getCounter(CounterNames.MAPPER_CALLS).getValue());
        assertEquals(37, result.getCounters().getCounter(CounterNames.REDUCER_CALLS).getValue());
        HashSet<Long> allOutput = new HashSet<>();
        for (List<Long> output : result.getOutputResult()) {
          allOutput.addAll(output);
        }
        assertEquals(50000, allOutput.size());
      }
    };
    runTest(testSettings.toBuilder().mapFanout(5).build(), spec, verifier);
    runTest(testSettings.toBuilder().mapFanout(2).build(), spec, verifier);
  }

  @Test
  public void testDoNothingWithEmptyReadersList() throws Exception {
    runTest(new MapReduceSpecification.Builder<>(new NoInput<Long>(0), new Mod37Mapper(),
        NoReducer.create(), new NoOutput<String, String>())
        .setKeyMarshaller(Marshallers.getStringMarshaller())
        .setValueMarshaller(Marshallers.getLongMarshaller()).setJobName("Empty test MR").build(),
        new Verifier<String>() {
          @Override
          public void verify(MapReduceResult<String> result) throws Exception {
            assertNull(result.getOutputResult());
            assertEquals(0, result.getCounters().getCounter(CounterNames.MAPPER_CALLS).getValue());
            assertEquals(0, result.getCounters().getCounter(CounterNames.REDUCER_CALLS).getValue());
          }
        });
  }

  @Test
  public void testDoNothing() throws Exception {
    runTest(new MapReduceSpecification.Builder<>(new NoInput<>(1), new Mod37Mapper(),
        NoReducer.create(), new NoOutput<String, String>())
        .setKeyMarshaller(Marshallers.getStringMarshaller())
        .setValueMarshaller(Marshallers.getLongMarshaller()).setJobName("Empty test MR").build(),
        new Verifier<String>() {
          @Override
          public void verify(MapReduceResult<String> result) throws Exception {
            assertNull(result.getOutputResult());
            assertEquals(0, result.getCounters().getCounter(CounterNames.MAPPER_CALLS).getValue());
            assertEquals(0, result.getCounters().getCounter(CounterNames.REDUCER_CALLS).getValue());
          }
        });
  }

  @SuppressWarnings("serial")
  private static class RougeMapper extends Mapper<Long, String, Long> {

    private static int[] beginShardCount;
    private static int[] endShardCount;
    private static int[] beginSliceCount;
    private static int[] endSliceCount;
    private static int[] totalMapCount;
    private static int[] successfulMapCount;
    private static Map<Integer, AtomicInteger>[] sliceAttemptsPerShardRetry;
    private static int[] shardFailureCount;
    private static int[] sliceFailureCount;
    private static boolean firstSlice; // fail at 2nd slice to avoid beginShard for slice failure
    private final int sliceFailures;
    private final int shardFailures;
    private int shardNum;
    private boolean allowSliceRetry = true;

    @SuppressWarnings("unchecked")
    private RougeMapper(int shards, int shardFailures, int sliceFailures) {
      this.shardFailures = shardFailures;
      this.sliceFailures = sliceFailures;
      beginShardCount = new int[shards];
      endShardCount = new int[shards];
      beginSliceCount = new int[shards];
      endSliceCount = new int[shards];
      totalMapCount = new int[shards];
      successfulMapCount = new int[shards];
      shardFailureCount = new int[shards];
      sliceFailureCount = new int[shards];
      sliceAttemptsPerShardRetry = new HashMap[shards];
      for (int i = 0; i < shards; i++) {
        sliceAttemptsPerShardRetry[i] = new HashMap<>();
      }
    }

    void setAllowSliceRetry(boolean allowSliceRetry) {
      this.allowSliceRetry = allowSliceRetry;
    }

    @Override
    public boolean allowSliceRetry() {
      return allowSliceRetry;
    }

    private void incrementCounter(String name) {
      getContext().incrementCounter(name + "[" + shardNum + "]");
    }

    private void incrementCounter(String name, int... indexes) {
      String counterName = name + "[" + shardNum + "]";
      for (int idx : indexes) {
        counterName += "[" + idx + "]";
      }
      getContext().incrementCounter(counterName);
    }

    @Override
    public void beginShard() {
      shardNum = getContext().getShardNumber();
      // We expect this to be one (as counter should be reset upon beginShard)
      incrementCounter("beginShard");
      sliceAttemptsPerShardRetry[shardNum].put(++beginShardCount[shardNum], new AtomicInteger());
      firstSlice = true;
    }

    @Override
    public void beginSlice() {
      shardNum = getContext().getShardNumber();
      sliceAttemptsPerShardRetry[shardNum].get(beginShardCount[shardNum]).getAndIncrement();
      incrementCounter("beginSlice", beginShardCount[shardNum]);
      beginSliceCount[shardNum]++;
    }

    @Override
    public void endSlice() {
      // We expect only endSlice[last_successful_attempt] to be available
      incrementCounter("endSlice", beginShardCount[shardNum]);
      endSliceCount[shardNum]++;
      firstSlice = false;
    }

    @Override
    public void endShard() {
      // We expect both to be equal to 1
      endShardCount[shardNum]++;
      incrementCounter("endShard");
    }

    @Override
    public void map(Long input) {
      totalMapCount[shardNum]++;
      incrementCounter("map");
      if (!firstSlice) {
        if (sliceFailureCount[shardNum] < sliceFailures) {
          sliceFailureCount[shardNum]++;
          throw new RuntimeException("bla");
        }
        if (shardFailureCount[shardNum] < shardFailures) {
          shardFailureCount[shardNum]++;
          throw new ShardFailureException("Bad state");
        }
      }
      successfulMapCount[shardNum]++;
    }
  }

  private static class RougeMapperVerifier implements Verifier<String> {

    private final int shards;
    private final int mapCount;
    private final int shardFailures;
    private final int sliceFailures;
    private final int shardFailuresDueToSliceRetry;
    private final int successfulMapCount; // we process one map call per slice
    private final int totalMapCount; // ShardFailure occur in the map
    private final int beginShardCount;
    private final int endShardCount;
    private final int beginSliceCount;
    private final int endSliceCount;
    private final Map<String, Integer> countersMap = new HashMap<>();

    private static final int SLICE_RETRIES = 20;
    private static final int SLICE_ATTEMPTS = 1 + SLICE_RETRIES;

    RougeMapperVerifier(int shards, int mapCount, int shardFailures, int sliceFailures) {
      this.shards = shards;
      this.mapCount = mapCount;
      this.sliceFailures = sliceFailures;
      this.shardFailuresDueToSliceRetry = sliceFailures / SLICE_ATTEMPTS;
      this.shardFailures = shardFailuresDueToSliceRetry + shardFailures;
      successfulMapCount = mapCount + this.shardFailures;
      totalMapCount = mapCount + 2 * shardFailures + (1 + SLICE_ATTEMPTS)
          * shardFailuresDueToSliceRetry + sliceFailures % SLICE_ATTEMPTS;
      beginShardCount = this.shardFailures + 1;
      endShardCount = 1; // always 1
      // 1 extra for eof (for both [begin/end]Slice)
      beginSliceCount = 1 + totalMapCount;
      endSliceCount = 1 + this.shardFailures + mapCount;

    }

    @Override
    public void verify(MapReduceResult<String> result) throws Exception {
      Counters counters = result.getCounters();
      assertEquals(shards * mapCount, counters.getCounter(CounterNames.MAPPER_CALLS).getValue());
      assertEquals(0, counters.getCounter(CounterNames.REDUCER_CALLS).getValue());
      countersMap.clear();
      for (Counter counter : counters.getCounters()) {
        countersMap.put(counter.getName(), (int) counter.getValue());
      }
      for (int i = 0; i < shards; i++) {
        verify(i);
      }
    }

    private int getCounter(int shard, String name, int... indexes) {
      String counterName = name + "[" + shard + "]";
      for (int index : indexes) {
        counterName += "[" + index + "]";
      }
      return countersMap.containsKey(counterName) ? countersMap.get(counterName) : -1;
    }

    private void verify(int shard) {
      assertEquals(beginShardCount, RougeMapper.beginShardCount[shard]);
      assertEquals(endShardCount, RougeMapper.endShardCount[shard]);
      assertEquals(beginSliceCount, RougeMapper.beginSliceCount[shard]);
      assertEquals(endSliceCount, RougeMapper.endSliceCount[shard]);
      assertEquals(successfulMapCount, RougeMapper.successfulMapCount[shard]);
      assertEquals(totalMapCount, RougeMapper.totalMapCount[shard]);

      int shardTry = 1;
      while (shardTry <= shardFailuresDueToSliceRetry) {
        // 1 go through, 1 fail + 20 retries fail
        assertEquals(1 + SLICE_ATTEMPTS,
            RougeMapper.sliceAttemptsPerShardRetry[shard].get(shardTry).intValue());
        shardTry++;
      }
      int failedSlices = sliceFailures % SLICE_ATTEMPTS;
      while (shardTry <= shardFailures) {
        if (failedSlices > 0) {
          assertEquals(1 + failedSlices + 1,
              RougeMapper.sliceAttemptsPerShardRetry[shard].get(shardTry).intValue());
          failedSlices = 0;
        } else {
          assertEquals(2, RougeMapper.sliceAttemptsPerShardRetry[shard].get(shardTry).intValue());
        }
        shardTry++;
      }
      while (shardTry <= beginShardCount) {
        assertEquals(failedSlices + mapCount + 1,
            RougeMapper.sliceAttemptsPerShardRetry[shard].get(shardTry).intValue());
        failedSlices = 0;
        shardTry++;
      }

      for (int i = 1; i < beginShardCount; i++) {
        assertEquals(-1, getCounter(shard, "beginSlice", i));
        assertEquals(-1, getCounter(shard, "endSlice", i));
      }
      // our MR process 1 map per slice (and failures do not count)
      assertEquals(mapCount + 1, getCounter(shard, "beginSlice", beginShardCount));
      assertEquals(mapCount + 1, getCounter(shard, "endSlice", beginShardCount));
      // always 1 because counters are reset per shard
      assertEquals(1, getCounter(shard, "beginShard"));
      assertEquals(1, getCounter(shard, "endShard"));
      assertEquals(mapCount, getCounter(shard, "map"));
    }
  }

  @Test
  public void testShardAndSliceRetriesSuccess() throws Exception {
    int shardsCount = 1;
    // {input-size, shard-failure, slice-failure}
    int[][] runs = { {10, 0, 0}, {10, 2, 0}, {10, 0, 10}, {10, 3, 5}, {10, 0, 22}, {10, 1, 50}};
    for (int[] run : runs) {
      RandomLongInput input = new RandomLongInput(run[0], shardsCount);
      runWithPipeline(testSettings.toBuilder().millisPerSlice(0)
          .build(), new MapReduceSpecification.Builder<>(input,
          new RougeMapper(shardsCount, run[1], run[2]), NoReducer.create(),
          new NoOutput<String, String>()).setKeyMarshaller(Marshallers.getStringMarshaller())
          .setValueMarshaller(Marshallers.getLongMarshaller()).setJobName("Shard-retry test")
          .build(), new RougeMapperVerifier(shardsCount, run[0], run[1], run[2]) {
        @Override
        public void verify(MapReduceResult<String> result) throws Exception {
          super.verify(result);
          assertNull(result.getOutputResult());
        }
      });
    }

    // Disallow slice-retry
    runs = new int[][] { {10, 0, 0}, {10, 4, 0}, {10, 0, 4}, {10, 3, 1}, {10, 1, 3}};
    for (int[] run : runs) {
      RandomLongInput input = new RandomLongInput(run[0], shardsCount);
      RougeMapper mapper = new RougeMapper(shardsCount, run[1], run[2]);
      mapper.setAllowSliceRetry(false);
      runWithPipeline(testSettings.toBuilder().millisPerSlice(0)
          .build(), new MapReduceSpecification.Builder<>(input, mapper,
          NoReducer.create(), new NoOutput<String, String>())
          .setKeyMarshaller(Marshallers.getStringMarshaller())
          .setValueMarshaller(Marshallers.getLongMarshaller()).setJobName("Shard-retry test")
          .build(), new RougeMapperVerifier(shardsCount, run[0], run[1] + run[2], 0) {
        @Override
        public void verify(MapReduceResult<String> result) throws Exception {
          super.verify(result);
          assertNull(result.getOutputResult());
        }
      });
    }
  }

  @Test
  public void testShardAndSliceRetriesFailure() throws Exception {
    int shardsCount = 1;
    // {shard-failure, slice-failure}
    int[][] runs = { {5, 0}, {4, 21}, {3, 50}};
    for (int[] run : runs) {
      RandomLongInput input = new RandomLongInput(10, shardsCount);
      MapReduceSettings mrSettings = testSettings.toBuilder()
        .millisPerSlice(0)
        .build();
      MapReduceSpecification<Long, String, Long, String, String> mrSpec =
          new MapReduceSpecification.Builder<>(input, new RougeMapper(shardsCount, run[0], run[1]),
              NoReducer.create(), new NoOutput<String, String>())
              .setJobName("Shard-retry failed").setKeyMarshaller(Marshallers.getStringMarshaller())
              .setValueMarshaller(Marshallers.getLongMarshaller()).build();
      JobRunId jobRunId = pipelineService.startNewPipeline(new MapReduceJob<>(mrSpec, mrSettings));
      executeTasksUntilEmpty("default");
      JobInfo info = pipelineService.getJobInfo(jobRunId);
      assertNull(info.getOutput());
      assertEquals(JobInfo.State.STOPPED_BY_ERROR, info.getJobState());
      assertTrue(info.getException().getMessage()
          .matches("Stage .*:::map-.* was not completed successfuly \\(status=ERROR, message=.*\\)"));
    }

    // Disallow slice-retry
    runs = new int[][] { {5, 0}, {0, 5}, {4, 1}, {1, 4}};
    for (int[] run : runs) {
      RandomLongInput input = new RandomLongInput(10, shardsCount);
      RougeMapper mapper = new RougeMapper(shardsCount, run[0], run[1]);
      mapper.setAllowSliceRetry(false);
      MapReduceSettings mrSettings = testSettings.toBuilder().millisPerSlice(0).build();
      MapReduceSpecification<Long, String, Long, String, String> mrSpec =
          new MapReduceSpecification.Builder<>(input, mapper,
              NoReducer.create(), new NoOutput<String, String>())
              .setJobName("Shard-retry failed").setKeyMarshaller(Marshallers.getStringMarshaller())
              .setValueMarshaller(Marshallers.getLongMarshaller()).build();
      JobRunId jobRunId = pipelineService.startNewPipeline(new MapReduceJob<>(mrSpec, mrSettings));
      executeTasksUntilEmpty("default");
      JobInfo info = pipelineService.getJobInfo(jobRunId);
      assertNull(info.getOutput());
      assertEquals(JobInfo.State.STOPPED_BY_ERROR, info.getJobState());
      assertTrue(info.getException().getMessage()
          .matches("Stage .*map-.* was not completed successfuly \\(status=ERROR, message=.*\\)"));
    }
  }

  @Test
  public void testPassThroughToString() throws Exception {
    final RandomLongInput input = new RandomLongInput(10, 1);
    input.setSeed(0L);
    runTest(new MapReduceSpecification.Builder<>(input, new Mod37Mapper(), ValueProjectionReducer
        .<String, Long>create(), new StringOutput<>(",", new GoogleCloudStorageFileOutput(getStorageTestHelper().getBucket(), "Foo-%02d", "text/plain", cloudStorageFileOutputOptions)))
        .setKeyMarshaller(Marshallers.getStringMarshaller())
        .setValueMarshaller(Marshallers.getLongMarshaller()).setJobName("TestPassThroughToString")
        .build(), new Verifier<GoogleCloudStorageFileSet>() {
      @Override
      public void verify(MapReduceResult<GoogleCloudStorageFileSet> result) throws Exception {
        assertEquals(1, result.getOutputResult().getNumFiles());
        assertEquals(10, result.getCounters().getCounter(CounterNames.MAPPER_CALLS).getValue());
        GcsFilename file = result.getOutputResult().getFile(0);
        ReadChannel channel = getStorageTestHelper().getStorage().reader(file.asBlobId());
        BufferedReader reader =
            new BufferedReader(Channels.newReader(channel, US_ASCII.newDecoder(), -1));
        String line = reader.readLine();
        List<String> strings = Arrays.asList(line.split(","));
        assertEquals(10, strings.size());
        input.setSeed(0L);
        InputReader<Long> source = input.createReaders().get(0);
        for (int i = 0; i < 10; i++) {
          assertTrue(strings.contains(source.next().toString()));
        }
      }
    });
  }


  @Test
  public void testPassByteBufferToGcsWithSliceRetries() throws Exception {
    applyTestPassByteBufferToGcs(true);
  }

  @Test
  public void testPassByteBufferToGcsWithoutSliceRetries() throws Exception {
    applyTestPassByteBufferToGcs(false);
  }

  private void applyTestPassByteBufferToGcs(boolean sliceRetry) throws Exception {
    final RandomLongInput input = new RandomLongInput(10, 1);
    input.setSeed(0L);
    MapReduceSpecification.Builder<Long, ByteBuffer, ByteBuffer, ByteBuffer,
        GoogleCloudStorageFileSet> builder = new MapReduceSpecification.Builder<>();
    builder.setJobName("TestPassThroughToByteBuffer");
    builder.setInput(input);
    builder.setMapper(new LongToBytesMapper());
    builder.setKeyMarshaller(Marshallers.getByteBufferMarshaller());
    builder.setValueMarshaller(Marshallers.getByteBufferMarshaller());
    builder.setReducer(ValueProjectionReducer.<ByteBuffer, ByteBuffer>create());
    builder.setOutput(new GoogleCloudStorageFileOutput(getStorageTestHelper().getBucket(), "fileNamePattern-%04d",
        "application/octet-stream", (GoogleCloudStorageFileOutput.Options) cloudStorageFileOutputOptions.withSupportSliceRetries(sliceRetry)));
    builder.setNumReducers(2);
    runTest(builder.build(), new Verifier<GoogleCloudStorageFileSet>() {
      @Override
      public void verify(MapReduceResult<GoogleCloudStorageFileSet> result) throws Exception {
        assertEquals(2, result.getOutputResult().getNumFiles());
        assertEquals(10, result.getCounters().getCounter(CounterNames.MAPPER_CALLS).getValue());
        ArrayList<Long> results = new ArrayList<>();
        ByteBuffer holder = ByteBuffer.allocate(8);
        for (GcsFilename file : result.getOutputResult().getFiles()) {
          assertEquals("application/octet-stream", getStorageTestHelper().getStorage().get(file.asBlobId()).getContentType());
          try (ReadChannel reader = getStorageTestHelper().getStorage().reader(file.asBlobId())) {
            int read = reader.read(holder);
            while (read != -1) {
              holder.rewind();
              results.add(holder.getLong());
              holder.rewind();
              read = reader.read(holder);
            }
          }
        }
        assertEquals(10, results.size());
        RandomLongInput input = new RandomLongInput(10, 1);
        input.setSeed(0L);
        InputReader<Long> source = input.createReaders().get(0);
        for (int i = 0; i < results.size(); i++) {
          Long expected = source.next();
          assertTrue(results.contains(expected));
        }
      }
    });
  }


  @SuppressWarnings({"serial", "rawtypes", "unchecked"})
  static class TestInput<T> extends Input<T> {

    static Input delegate;

    public TestInput(Input<T> delegate) {
      TestInput.delegate = delegate;
    }

    @Override
    public void setContext(Context context) {
      super.setContext(context);
      delegate.setContext(context);
    }

    @Override
    public List<? extends InputReader<T>> createReaders() throws IOException {
      return delegate.createReaders();
    }
  }

  @SuppressWarnings({"serial", "rawtypes", "unchecked"})
  static class TestOutput<O, R> extends Output<O, R> {

    static Output delegate;

    public TestOutput(Output<O, R> delegate) {
      TestOutput.delegate = delegate;
    }

    @Override
    public void setContext(Context context) {
      delegate.setContext(context);
    }

    @Override
    public List<? extends OutputWriter<O>> createWriters(int numShards) {
      return delegate.createWriters(numShards);
    }

    @Override
    public R finish(Collection<? extends OutputWriter<O>> writers) throws IOException {
      return (R) delegate.finish(writers);
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testLifeCycleMethodsCalled() throws Exception {

    // trying to make these serializable does not help serializaiton ... still fails *unless*
    // wrap with TestInput/TestOutput respectively
    Input<Long> input = mock(Input.class); //, Mockito.withSettings().serializable());
    Output<ByteBuffer, Void> output = mock(Output.class); //, Mockito.withSettings().serializable());

    InputReader<Long> inputReader = mock(InputReader.class, Mockito.withSettings().serializable());
    OutputWriter<ByteBuffer> outputWriter = mock(OutputWriter.class, Mockito.withSettings().serializable());

    when(input.createReaders())
      .thenAnswer((InvocationOnMock invocation) -> List.of(inputReader));

    when(inputReader.estimateMemoryRequirement())
      .thenReturn(0L);
    when(inputReader.next())
      .thenThrow(new NoSuchElementException());

    when(output.createWriters(eq(1)))
      .thenAnswer((InvocationOnMock invocation) -> List.of(outputWriter));

    when(output.finish(anyCollection()))
      .thenReturn(null);

    runWithPipeline(testSettings, new MapReduceSpecification.Builder<>(
        new TestInput<>(input),  //unless wrap this with TestInput, serialization to datastore fails???
        new LongToBytesMapper(),
        ValueProjectionReducer.create(),
        new TestOutput<>(output) //unless wrap this with TestOutput, serialization to datastore fails???
      ).setKeyMarshaller(Marshallers.getByteBufferMarshaller())
        .setValueMarshaller(Marshallers.getByteBufferMarshaller())
        .setJobName("testLifeCycleMethodsCalled").build(), new NopVerifier<>());


    verify(input, atLeastOnce()).createReaders();
    verify(inputReader, atLeastOnce()).setContext(any(ShardContext.class));

    verify(output, atLeastOnce()).createWriters(eq(1));
    verify(outputWriter, atLeastOnce()).setContext(any(ShardContext.class));

    // so below methods seem to be called when I debug; why isn't Mockito picking them up?
    //--> best guess is serialization!?!? (eg, we're still verifying the original; whereas fw is working on deserialized copies???)
    //verify(inputReader, atLeastOnce()).estimateMemoryRequirement();
    //verify(outputWriter, atLeastOnce()).estimateMemoryRequirement();
  }

  @Test
  public void testInMemoryData() throws Exception {
    final int SHARD_COUNT = 3;
    final int SHARD_SIZE = 30;

    List<List<Long>> data = new ArrayList<>();
    for (long i = 0; i < SHARD_COUNT; ++i) {
      List<Long> row = new ArrayList<>();
      for (long j = 0; j < SHARD_SIZE; ++j) {
        row.add(i*SHARD_SIZE + j);
      }
      data.add(row);
    }

    Input<Long> input = new InMemoryInput(data);

    runTest(new MapReduceSpecification.Builder<>(
        input,
        new TestMapper(),
        new TestReducer(), new InMemoryOutput<>())
        .setKeyMarshaller(Marshallers.getStringMarshaller())
        .setValueMarshaller(Marshallers.getLongMarshaller()).setJobName("Test MR").build(),
        new Verifier<List<List<KeyValue<String, List<Long>>>>>() {
          @Override
          public void verify(MapReduceResult<List<List<KeyValue<String, List<Long>>>>> result)
              throws Exception {
            Counters counters = result.getCounters();
            log.info("counters=" + counters);
            assertNotNull(counters);

            assertEquals(SHARD_COUNT*SHARD_SIZE, counters.getCounter("map").getValue());
            assertEquals(SHARD_COUNT, counters.getCounter("beginShard").getValue());
            assertEquals(SHARD_COUNT, counters.getCounter("endShard").getValue());
            assertEquals(SHARD_COUNT, counters.getCounter("beginSlice").getValue());
            assertEquals(SHARD_COUNT, counters.getCounter("endSlice").getValue());

            assertEquals(SHARD_COUNT*SHARD_SIZE, counters.getCounter(CounterNames.MAPPER_CALLS).getValue());
            assertTrue(counters.getCounter(CounterNames.MAPPER_WALLTIME_MILLIS).getValue() > 0);

            List<KeyValue<String, List<Long>>> output =
                Iterables.getOnlyElement(result.getOutputResult());
            assertEquals(2, output.size());
            assertEquals("even", output.get(0).getKey());
            List<Long> evenValues = new ArrayList<>(output.get(0).getValue());
            Collections.sort(evenValues);
            List<Long> expected = new ArrayList<>();
            for (long i = 0; i < SHARD_COUNT*SHARD_SIZE; i += 2) {
              expected.add(i);
            }
            assertEquals(expected, evenValues);
            assertEquals("multiple-of-ten", output.get(1).getKey());
            List<Long> multiplesOfTen = new ArrayList<>(output.get(1).getValue());
            Collections.sort(multiplesOfTen);
            assertEquals(ImmutableList.of(0L, 10L, 20L, 30L, 40L, 50L, 60L, 70L, 80L),
                multiplesOfTen);
          }
        });
  }

  @Test
  public void testNoData() throws Exception {

    List<List<Long>> data = Arrays.asList(
      new ArrayList<>(),
      new ArrayList<>()
    );

    runTest(new MapReduceSpecification.Builder<>(
        new InMemoryInput(data),
        new TestMapper(),
        NoReducer.create(), new NoOutput<Void, Void>())
        .setKeyMarshaller(Marshallers.getStringMarshaller())
        .setValueMarshaller(Marshallers.getLongMarshaller())
        .setJobName("Test MR")
        .build(),
        new Verifier<Void>() {
          @Override
          public void verify(MapReduceResult<Void> result) throws Exception {
            Counters counters = result.getCounters();
            assertNotNull(counters);

            assertEquals(0, counters.getCounter("map").getValue());
            long mapShards = counters.getCounter("beginShard").getValue();
            assertEquals(mapShards, counters.getCounter("beginShard").getValue());
            assertEquals(mapShards, counters.getCounter("endShard").getValue());
            assertEquals(mapShards, counters.getCounter("beginSlice").getValue());
            assertEquals(mapShards, counters.getCounter("endSlice").getValue());

            assertEquals(0, counters.getCounter(CounterNames.MAPPER_CALLS).getValue());
            assertEquals(0, counters.getCounter(CounterNames.MAPPER_WALLTIME_MILLIS).getValue());
          }
        });
  }

  @SuppressWarnings("serial")
  private static class Mod37Mapper extends Mapper<Long, String, Long> {

    static final int MOD_VALUE = 37;

    @Override
    public void map(Long input) {
      String mod37 = String.valueOf(Math.abs(input) % MOD_VALUE);
      emit(mod37, input);
    }
  }

  @SuppressWarnings("serial")
  private static class DummyValueMapper extends Mapper<Long, Long, String> {
    private final String value;

    DummyValueMapper(int size) {
      StringBuilder b = new StringBuilder();
      for (int i = 0; i < size; i++) {
        b.append('\0');
      }
      value = b.toString();
    }

    @Override
    public void map(Long input) {
      emit(input, value);
    }
  }

  @SuppressWarnings("serial")
  private static class LongToBytesMapper extends Mapper<Long, ByteBuffer, ByteBuffer> {

    @Override
    public void map(Long input) {
      ByteBuffer key = ByteBuffer.allocate(8);
      key.putLong(input);
      key.rewind();
      ByteBuffer value = ByteBuffer.allocate(8);
      value.putLong(input);
      value.rewind();
      emit(key, value);
    }
  }

  @Test
  public void testSomeNumbers() throws Exception {
    final int SHARD_COUNT = 5;
    MapReduceSpecification.Builder<Long, String, Long, KeyValue<String, List<Long>>,
        List<List<KeyValue<String, List<Long>>>>> mrSpecBuilder =
        new MapReduceSpecification.Builder<>();
    mrSpecBuilder.setJobName("Test MR");
    mrSpecBuilder.setInput(new ConsecutiveLongInput(-10000, 10000, SHARD_COUNT));
    mrSpecBuilder.setMapper(new Mod37Mapper());
    mrSpecBuilder.setKeyMarshaller(Marshallers.getStringMarshaller());
    mrSpecBuilder.setValueMarshaller(Marshallers.getLongMarshaller());
    mrSpecBuilder.setReducer(new TestReducer());
    mrSpecBuilder.setOutput(new InMemoryOutput<>());
    mrSpecBuilder.setNumReducers(5);

    runTest(mrSpecBuilder.build(), new Verifier<List<List<KeyValue<String, List<Long>>>>>() {
      @Override
      public void verify(MapReduceResult<List<List<KeyValue<String, List<Long>>>>> result)
          throws Exception {
        Counters counters = result.getCounters();
        assertEquals(20000, counters.getCounter(CounterNames.MAPPER_CALLS).getValue());
        assertEquals(Mod37Mapper.MOD_VALUE, counters.getCounter(CounterNames.REDUCER_CALLS).getValue());

        List<List<KeyValue<String, List<Long>>>> actualOutput = result.getOutputResult();
        List<ArrayListMultimap<String, Long>> expectedOutput = Lists.newArrayList();
        for (int i = 0; i < SHARD_COUNT; i++) {
          expectedOutput.add(ArrayListMultimap.<String, Long>create());
        }
        Marshaller<String> marshaller = Marshallers.getStringMarshaller();
        HashingSharder sharder = new HashingSharder(SHARD_COUNT);
        for (long l = -10000; l < 10000; l++) {
          String mod37 = String.valueOf(Math.abs(l) % Mod37Mapper.MOD_VALUE);
          expectedOutput.get(sharder.getShardForKey(marshaller.toBytes(mod37))).put(mod37, l);
        }
        for (int i = 0; i < SHARD_COUNT; i++) {
          assertEquals(expectedOutput.get(i).keySet().size(), actualOutput.get(i).size());
          for (KeyValue<String, List<Long>> actual : actualOutput.get(i)) {
            List<Long> value = new ArrayList<>(actual.getValue());
            Collections.sort(value);
            assertEquals("shard " + i + ", key " + actual.getKey(),
                expectedOutput.get(i).get(actual.getKey()), value);
          }
        }
      }
    });
  }

  /**
   * Makes sure the same key is not dupped, nor does the reduce go into an infinite loop if it
   * ignores the values.
   */
  @Test
  public void testReduceOnlyLooksAtKeys() throws Exception {
    MapReduceSpecification.Builder<Long, String, Long, String, List<List<String>>> builder =
        new MapReduceSpecification.Builder<>();
    builder.setJobName("Test MR");
    builder.setInput(new ConsecutiveLongInput(-10000, 10000, 10));
    builder.setMapper(new Mod37Mapper());
    builder.setKeyMarshaller(Marshallers.getStringMarshaller());
    builder.setValueMarshaller(Marshallers.getLongMarshaller());
    builder.setReducer(KeyProjectionReducer.<String, Long>create());
    builder.setOutput(new InMemoryOutput<String>());
    builder.setNumReducers(5);

    runTest(builder.build(), new Verifier<List<List<String>>>() {
      @Override
      public void verify(MapReduceResult<List<List<String>>> result) throws Exception {
        Counters counters = result.getCounters();
        assertEquals(20000, counters.getCounter(CounterNames.MAPPER_CALLS).getValue());
        assertEquals(Mod37Mapper.MOD_VALUE, counters.getCounter(CounterNames.REDUCER_CALLS).getValue());

        List<List<String>> actualOutput = result.getOutputResult();
        assertEquals(5, actualOutput.size());
        List<String> allKeys = new ArrayList<>();
        for (int shard = 0; shard < 5; shard++) {
          allKeys.addAll(actualOutput.get(shard));
        }
        assertEquals(Mod37Mapper.MOD_VALUE, allKeys.size());
        for (int i = 0; i < Mod37Mapper.MOD_VALUE; i++) {
          assertTrue(String.valueOf(i), allKeys.contains(String.valueOf(i)));
        }
      }
    });
  }

  @Test
  public void testManySortOutputFiles() throws Exception {
    MapReduceSpecification.Builder<Long, Long, String, Long, List<List<Long>>> builder =
        new MapReduceSpecification.Builder<>();
    builder.setJobName("Test MR");
    final long sortMem = 1000;
    final long inputItems = 10 * sortMem / 100; // Forces 3 levels of merging if mergeFanin==2
    builder.setInput(new RandomLongInput(inputItems, 1));
    builder.setMapper(new DummyValueMapper(100));
    builder.setKeyMarshaller(Marshallers.getLongMarshaller());
    builder.setValueMarshaller(Marshallers.getStringMarshaller());
    builder.setReducer(KeyProjectionReducer.create());
    builder.setOutput(new InMemoryOutput<>());
    builder.setNumReducers(1);

    runWithPipeline(
        testSettings.toBuilder().maxSortMemory(sortMem).mergeFanin(2)
          .build(),
        builder.build(), (MapReduceResult<List<List<Long>>> result) -> {
            Counters counters = result.getCounters();
            assertEquals(inputItems, counters.getCounter(CounterNames.MAPPER_CALLS).getValue());
            assertEquals(inputItems, counters.getCounter(CounterNames.REDUCER_CALLS).getValue());
            assertEquals(inputItems, counters.getCounter(CounterNames.SORT_CALLS).getValue());
            assertEquals(inputItems * 3, counters.getCounter(CounterNames.MERGE_CALLS).getValue());

            List<List<Long>> actualOutput = result.getOutputResult();
            assertEquals(1, actualOutput.size());
            assertEquals(inputItems, actualOutput.get(0).size());
          });
  }

  @Test
  public void testSlicingJob() throws Exception {
    final int NUM_SHARDS = 4;
    final int NUM_REDUCERS = 3;

    MapReduceSpecification.Builder<Long, String, Long, String, List<List<String>>> builder =
        new MapReduceSpecification.Builder<>();
    builder.setJobName("Test MR");
    builder.setInput(new ConsecutiveLongInput(-100, 100, NUM_SHARDS));
    builder.setMapper(new Mod37Mapper());
    builder.setKeyMarshaller(Marshallers.getStringMarshaller());
    builder.setValueMarshaller(Marshallers.getLongMarshaller());
    builder.setReducer(KeyProjectionReducer.<String, Long>create());
    builder.setOutput(new InMemoryOutput<>());
    builder.setNumReducers(NUM_REDUCERS);
    runTest(testSettings.toBuilder().millisPerSlice(0).build(), builder.build(),
        new Verifier<List<List<String>>>() {
          @Override
          public void verify(MapReduceResult<List<List<String>>> result) throws Exception {
            Counters counters = result.getCounters();
            assertEquals(200, counters.getCounter(CounterNames.MAPPER_CALLS).getValue());
            assertEquals(Mod37Mapper.MOD_VALUE, counters.getCounter(CounterNames.REDUCER_CALLS).getValue());

            List<List<String>> actualOutput = result.getOutputResult();
            assertEquals(NUM_REDUCERS, actualOutput.size());
            List<String> allKeys = new ArrayList<>();
            for (int reducerShard = 0; reducerShard < NUM_REDUCERS; reducerShard++) {
              allKeys.addAll(actualOutput.get(reducerShard));
            }
            assertEquals(Mod37Mapper.MOD_VALUE, allKeys.size());
            for (int i = 0; i < Mod37Mapper.MOD_VALUE; i++) {
              assertTrue(String.valueOf(i), allKeys.contains(String.valueOf(i)));
            }
          }
        });
  }

  @RequiredArgsConstructor
  @SuppressWarnings("serial")
  static class SideOutputMapper extends Mapper<Long, GcsFilename, Void> {
    transient GoogleCloudStorageFileOutputWriter sideOutput;

    final String bucket;
    final GoogleCloudStorageFileOutput.Options options;

    @Override
    public void beginSlice() {
      GcsFilename filename = new GcsFilename(bucket, UUID.randomUUID().toString());
      sideOutput = new GoogleCloudStorageFileOutputWriter(filename, "application/octet-stream", options);
      try {
        sideOutput.beginShard();
        sideOutput.beginSlice();
      } catch (IOException ex) {
        throw new RuntimeException(ex);
      }
    }

    @Override
    public void map(Long input) {
      log.info("map(" + input + ") in shard " + getContext().getShardNumber());
      try {
        sideOutput.write(Marshallers.getLongMarshaller().toBytes(input));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void endSlice() {
      log.info("endShard() in shard " + getContext().getShardNumber());
      try {
        sideOutput.endSlice();
        sideOutput.endShard();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      emit(sideOutput.getFile(), null);
    }
  }

  @Test
  public void testSideOutput() throws Exception {

    final int SHARD_COUNT = 3;

    runTest(new MapReduceSpecification.Builder<>(new ConsecutiveLongInput(0, SHARD_COUNT, SHARD_COUNT),
        new SideOutputMapper(getStorageTestHelper().getBucket(), cloudStorageFileOutputOptions), KeyProjectionReducer.<GcsFilename, Void>create(),
        new InMemoryOutput<>())
        .setKeyMarshaller(Marshallers.getSerializationMarshaller())
        .setValueMarshaller(Marshallers.getVoidMarshaller())
        .setJobName("Test MR")
        .build(),
        new Verifier<List<List<GcsFilename>>>() {
          @Override
          public void verify(MapReduceResult<List<List<GcsFilename>>> result) throws Exception {
            List<List<GcsFilename>> outputResult = result.getOutputResult();
            Set<Long> expected = new HashSet<>();
            for (long i = 0; i < SHARD_COUNT; i++) {
              expected.add(i);
            }
            assertEquals(1, outputResult.size());

            for (List<GcsFilename> files : outputResult) {
              assertEquals(SHARD_COUNT, files.size());
              for (GcsFilename file : files) {
                ByteBuffer buf = ByteBuffer.allocate(8);
                try (ReadChannel ch = getStorageTestHelper().getStorage().reader(file.asBlobId())) {
                  assertEquals(8, ch.read(buf));
                  assertEquals(-1, ch.read(ByteBuffer.allocate(1)));
                }
                buf.flip();
                assertTrue(expected.remove(Marshallers.getLongMarshaller().fromBytes(buf)));
              }
            }
            assertTrue(expected.isEmpty());
          }
        });
  }

  @SuppressWarnings("serial")
  static class TestMapper extends Mapper<Long, String, Long> {
    @Override
    public void map(Long i) {
      getContext().incrementCounter("map");
      try {
        Thread.sleep(1);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
      log.info("map(" + i + ")");
      if (i % 2 == 0) {
        emit("even", i);
      }
      if (i % 10 == 0) {
        emit("multiple-of-ten", i);
      }
    }

    @Override
    public void beginShard() {
      getContext().incrementCounter("beginShard");
    }

    @Override
    public void endShard() {
      getContext().incrementCounter("endShard");
    }

    @Override
    public void beginSlice() {
      getContext().incrementCounter("beginSlice");
    }

    @Override
    public void endSlice() {
      getContext().incrementCounter("endSlice");
    }
  }

  @SuppressWarnings("serial")
  static class TestReducer extends Reducer<String, Long, KeyValue<String, List<Long>>> {
    @Override
    public void reduce(String property, ReducerInput<Long> matchingValues) {
      ImmutableList.Builder<Long> out = ImmutableList.builder();
      while (matchingValues.hasNext()) {
        long value = matchingValues.next();
        getContext().incrementCounter(property);
        out.add(value);
      }
      List<Long> values = out.build();
      emit(KeyValue.of(property, values));
    }
  }
}
