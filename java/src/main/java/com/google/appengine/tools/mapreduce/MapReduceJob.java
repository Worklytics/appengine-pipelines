// Copyright 2011 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce;

import com.google.appengine.tools.mapreduce.impl.BaseContext;
import com.google.appengine.tools.mapreduce.impl.CountersImpl;
import com.google.appengine.tools.mapreduce.impl.FilesByShard;
import com.google.appengine.tools.mapreduce.impl.GoogleCloudStorageMapOutput;
import com.google.appengine.tools.mapreduce.impl.GoogleCloudStorageMergeInput;
import com.google.appengine.tools.mapreduce.impl.GoogleCloudStorageMergeOutput;
import com.google.appengine.tools.mapreduce.impl.GoogleCloudStorageReduceInput;
import com.google.appengine.tools.mapreduce.impl.GoogleCloudStorageSortInput;
import com.google.appengine.tools.mapreduce.impl.GoogleCloudStorageSortOutput;
import com.google.appengine.tools.mapreduce.impl.HashingSharder;
import com.google.appengine.tools.mapreduce.impl.MapShardTask;
import com.google.appengine.tools.mapreduce.impl.ReduceShardTask;
import com.google.appengine.tools.mapreduce.impl.WorkerController;
import com.google.appengine.tools.mapreduce.impl.WorkerShardTask;
import com.google.appengine.tools.mapreduce.impl.pipeline.CleanupPipelineJob;
import com.google.appengine.tools.mapreduce.impl.pipeline.ExamineStatusAndReturnResult;
import com.google.appengine.tools.mapreduce.impl.pipeline.ResultAndStatus;
import com.google.appengine.tools.mapreduce.impl.pipeline.ShardedJob;
import com.google.appengine.tools.mapreduce.impl.shardedjob.ShardedJobId;
import com.google.appengine.tools.mapreduce.impl.shardedjob.ShardedJobSettings;
import com.google.appengine.tools.mapreduce.impl.sort.MergeContext;
import com.google.appengine.tools.mapreduce.impl.sort.MergeShardTask;
import com.google.appengine.tools.mapreduce.impl.sort.SortContext;
import com.google.appengine.tools.mapreduce.impl.sort.SortShardTask;
import com.google.appengine.tools.mapreduce.impl.sort.SortWorker;
import com.google.appengine.tools.mapreduce.inputs.GoogleCloudStorageLineInput;
import com.google.appengine.tools.mapreduce.outputs.GoogleCloudStorageFileOutput;
import com.google.appengine.tools.pipeline.*;
import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.DatastoreOptions;
import com.google.cloud.datastore.Key;
import com.google.cloud.storage.*;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Setter;

import javax.inject.Inject;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.CancellationException;
import java.util.logging.Level;
import java.util.logging.Logger;


/**
 * A Pipeline job that runs a MapReduce.
 *
 * @author ohler@google.com (Christian Ohler)
 *
 * @param <I> type of input values
 * @param <K> type of intermediate keys
 * @param <V> type of intermediate values
 * @param <O> type of output values
 * @param <R> type of final result
 */
@RequiredArgsConstructor
public class MapReduceJob<I, K, V, O, R> extends Job0<MapReduceResult<R>> {

  private static final long serialVersionUID = 723635736794527552L;
  private static final Logger log = Logger.getLogger(MapReduceJob.class.getName());

  @NonNull private final MapReduceSpecification<I, K, V, O, R> specification;
  @NonNull private final MapReduceSettings settings;

  @Setter(onMethod_ = @VisibleForTesting)
  @Inject
  transient Datastore datastore;

  protected Datastore getDatastore() {
    if (datastore == null) {
      datastore = DatastoreOptions.getDefaultInstance().toBuilder()
        .setNamespace(settings.getNamespace())
        .build().getService();
    }
    return datastore;
  }

  interface MRStage {

    enum Stage {
      MAP,
      SORT,
      MERGE,
      REDUCE
    }

    /**
     * @return key of this stage
     */
    Stage getStage();

    /**
     * @return key of MR job, of which this is one stage
     */
    Key getMRJobKey();

    default ShardedJobId getShardedJobId() {
      return ShardedJobId.of(getMRJobKey().getProjectId(), getMRJobKey().getNamespace(), getStageId());
    }

    /**
     * @return unique id for stage instance, given project + namespace
     */
    default String getStageId() {
      return getStage().name() + "-" + getMRJobKey().getName();
    }

    /**
     * @return MRJobKey, as a string-encoded ID
     */
    default String getMRJobId() {
      return getMRJobKey().toUrlSafe();
    }
  }

  /**
   * Starts a {@link MapReduceJob} with the given parameters in a new Pipeline.
   * Returns the pipeline id.
   * @deprecated Use {@link PipelineOrchestrator#start} instead.
   */
  @Deprecated
  public static <I, K, V, O, R> String start(
      @NonNull MapReduceSpecification<I, K, V, O, R> specification, @NonNull MapReduceSettings settings) {
    throw new UnsupportedOperationException("Use PipelineOrchestrator:start");
  }

  private static void verifyBucketIsWritable(MapReduceSettings settings) {
   Storage client = GcpCredentialOptions.getStorageClient(settings);
    BlobId blobId = BlobId.of(settings.getBucketName(), UUID.randomUUID() + ".tmp");
    if (client.get(blobId) != null) {
      log.warning("File '" + blobId.getName() + "' exists. Skipping bucket write test.");
      return;
    }
    try {
      client.create(BlobInfo.newBuilder(blobId).build(), "Delete me!".getBytes(StandardCharsets.UTF_8));
    } catch (StorageException e) {
      throw new IllegalArgumentException("Bucket " + settings.getBucketName() + " is not writeable; MR job needs to write it for sort/shuffle phase of job", e);
    } finally {
      client.delete(blobId);
    }
  }

  /**
   * The pipeline job to execute the Map stage of the MapReduce. (For all shards)
   */
  @RequiredArgsConstructor
  static class MapJob<I, K, V> extends Job0<MapReduceResult<FilesByShard>> implements MRStage {

    private static final long serialVersionUID = 1L;

    @Getter
    private final Stage stage = Stage.MAP;
    @Getter
    @NonNull private final Key MRJobKey;
    @NonNull private final MapReduceSpecification<I, K, V, ?, ?> mrSpec;
    @NonNull private final MapReduceSettings settings;


    @Setter(onMethod = @__(@VisibleForTesting))
    private transient Datastore datastore;





    protected Datastore getDatastore() {
      if (datastore == null) {
        datastore = settings.getDatastoreOptions().getService();
      }
      return datastore;
    }

    @Override
    public String toString() {
      return getClass().getSimpleName() + "(" + getShardedJobId() + ")";
    }

    /**
     * Starts a shardedJob for each map worker. The format of the files and output is defined by
     * {@link GoogleCloudStorageMapOutput}.
     *
     * @returns A future containing the FilesByShard for the sortJob
     */
    @Override
    public Value<MapReduceResult<FilesByShard>> run() {
      Context context = new BaseContext(getShardedJobId());
      Input<I> input = mrSpec.getInput();
      input.setContext(context);
      List<? extends InputReader<I>> readers;
      try {
        readers = input.createReaders();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      Output<KeyValue<K, V>, FilesByShard> output = new GoogleCloudStorageMapOutput<>(
              settings.getBucketName(),
              getShardedJobId(),
              mrSpec.getKeyMarshaller(),
              mrSpec.getValueMarshaller(),
              new HashingSharder(getNumOutputFiles(readers.size())),
              GoogleCloudStorageFileOutput.BaseOptions.builder()
                .serviceAccountKey(settings.getServiceAccountKey())
                .build()
      );
      output.setContext(context);

      List<? extends OutputWriter<KeyValue<K, V>>> writers = output.createWriters(readers.size());
      Preconditions.checkState(readers.size() == writers.size(), "%s: %s readers, %s writers",
        getShardedJobId(), readers.size(), writers.size());
      ImmutableList.Builder<WorkerShardTask<I, KeyValue<K, V>, MapperContext<K, V>>> mapTasks =
          ImmutableList.builder();
      for (int i = 0; i < readers.size(); i++) {
        mapTasks.add(new MapShardTask<>(getShardedJobId(), i, readers.size(), readers.get(i),
            mrSpec.getMapper(), writers.get(i), settings.getMillisPerSlice()));
      }
      ShardedJobSettings shardedJobSettings =
          settings.toShardedJobSettings(getMRJobKey(), getPipelineKey());

      PromisedValue<ResultAndStatus<FilesByShard>> resultAndStatus = newPromise();
      WorkerController<I, KeyValue<K, V>, FilesByShard, MapperContext<K, V>> workerController =
          new WorkerController<>(getShardedJobId(), new CountersImpl(), output, resultAndStatus.getHandle());

      DatastoreOptions datastoreOptions = settings.getDatastoreOptions();
      ShardedJob<?> shardedJob =
          new ShardedJob<>(getShardedJobId(), mapTasks.build(), workerController, shardedJobSettings);
      FutureValue<Void> shardedJobResult = futureCall(shardedJob, settings.toJobSettings());
      return futureCall(new ExamineStatusAndReturnResult<>(getShardedJobId()),
          resultAndStatus, settings.toJobSettings(waitFor(shardedJobResult),
              statusConsoleUrl(shardedJobSettings.getMapReduceStatusUrl())));
    }

    private int getNumOutputFiles(int mapShards) {
      return Math.min(settings.getMapFanout(), Math.max(mapShards, mrSpec.getNumReducers()));
    }

    @SuppressWarnings("unused")
    public Value<MapReduceResult<FilesByShard>> handleException(CancellationException ex) {
      getPipelineOrchestrator().abortJob(getShardedJobId());
      return null;
    }
  }

  /**
   * The pipeline job to execute the Sort stage of the MapReduce. (For all shards)
   */
  @RequiredArgsConstructor
  static class SortJob extends Job1<
      MapReduceResult<FilesByShard>,
      MapReduceResult<FilesByShard>> implements MRStage {
    private static final long serialVersionUID = 1L;
    // We don't need the CountersImpl part of the MapResult input here but we
    // accept it to avoid needing an adapter job to connect this job to MapJob's result.

    @Getter
    private final Stage stage = Stage.SORT;
    @Getter
    @NonNull private final Key MRJobKey;
    @NonNull private final MapReduceSpecification<?, ?, ?, ?, ?> mrSpec;
    @NonNull private final MapReduceSettings settings;

    @Setter(onMethod = @__(@VisibleForTesting))
    private transient Datastore datastore;

    protected Datastore getDatastore() {
      if (datastore == null) {
        datastore = DatastoreOptions.getDefaultInstance().toBuilder()
          .setNamespace(settings.getNamespace())
          .build().getService();
      }
      return datastore;
    }

    @Override
    public String toString() {
      return getClass().getSimpleName() + "(" + getShardedJobId().asEncodedString() + ")";
    }

    /**
     * Takes in the the result of the map stage. (FilesByShard indexed by sortShard) These files are
     * then read, and written out in sorted order. The result is a set of files for each reducer.
     * The format for how the data is written out is defined by {@link GoogleCloudStorageSortOutput}
     */
    @Override
    public Value<MapReduceResult<FilesByShard>> run(MapReduceResult<FilesByShard> mapResult) {

      Context context = new BaseContext(getShardedJobId());
      int mapShards = findMaxFilesPerShard(mapResult.getOutputResult());
      int reduceShards = mrSpec.getNumReducers();
      FilesByShard filesByShard = mapResult.getOutputResult();
      filesByShard.splitShards(Math.max(mapShards, reduceShards));
      GoogleCloudStorageLineInput.BaseOptions inputOptions = GoogleCloudStorageLineInput.BaseOptions.defaults();
      GoogleCloudStorageFileOutput.BaseOptions outputOptions = GoogleCloudStorageFileOutput.BaseOptions.defaults();
      if (settings.getServiceAccountKey() != null) {
        inputOptions = inputOptions.withServiceAccountKey(settings.getServiceAccountKey());
        outputOptions = outputOptions.withServiceAccountKey(settings.getServiceAccountKey());
      }
      GoogleCloudStorageSortInput input = new GoogleCloudStorageSortInput(filesByShard, inputOptions);
      ((Input<?>) input).setContext(context);
      List<? extends InputReader<KeyValue<ByteBuffer, ByteBuffer>>> readers = input.createReaders();
      Output<KeyValue<ByteBuffer, List<ByteBuffer>>, FilesByShard> output =
          new GoogleCloudStorageSortOutput(settings.getBucketName(), getShardedJobId(),
              new HashingSharder(reduceShards), outputOptions);
      output.setContext(context);

      List<? extends OutputWriter<KeyValue<ByteBuffer, List<ByteBuffer>>>> writers =
          output.createWriters(readers.size());
      Preconditions.checkState(readers.size() == writers.size(), "%s: %s readers, %s writers",
        getShardedJobId(), readers.size(), writers.size());
      ImmutableList.Builder<WorkerShardTask<KeyValue<ByteBuffer, ByteBuffer>,
          KeyValue<ByteBuffer, List<ByteBuffer>>, SortContext>> sortTasks =
              ImmutableList.builder();
      for (int i = 0; i < readers.size(); i++) {
        sortTasks.add(new SortShardTask(getShardedJobId(),
            i,
            readers.size(),
            readers.get(i),
            new SortWorker(settings.getMaxSortMemory(), settings.getSortBatchPerEmitBytes()),
            writers.get(i),
            settings.getSortReadTimeMillis()));
      }
      ShardedJobSettings shardedJobSettings =
          settings.toShardedJobSettings(getMRJobKey(), getPipelineKey());

      PromisedValue<ResultAndStatus<FilesByShard>> resultAndStatus = newPromise();
      WorkerController<KeyValue<ByteBuffer, ByteBuffer>, KeyValue<ByteBuffer, List<ByteBuffer>>,
          FilesByShard, SortContext> workerController = new WorkerController<>(getShardedJobId(),
          mapResult.getCounters(), output, resultAndStatus.getHandle());

      ShardedJob<?> shardedJob =
          new ShardedJob<>(getShardedJobId(), sortTasks.build(), workerController, shardedJobSettings);
      FutureValue<Void> shardedJobResult = futureCall(shardedJob, settings.toJobSettings());

      return futureCall(new ExamineStatusAndReturnResult<>(getShardedJobId()),
          resultAndStatus, settings.toJobSettings(waitFor(shardedJobResult),
              statusConsoleUrl(shardedJobSettings.getMapReduceStatusUrl())));
    }

    @SuppressWarnings("unused")
    public Value<FilesByShard> handleException(CancellationException ex) {
      getPipelineOrchestrator().abortJob(getShardedJobId());
      return null;
    }
  }

  /**
   * The pipeline job to execute the optional Merge stage of the MapReduce. (For all shards)
   */
  @RequiredArgsConstructor
  static class MergeJob extends
      Job1<MapReduceResult<FilesByShard>, MapReduceResult<FilesByShard>> implements MRStage {

    private static final long serialVersionUID = 2L;

    // We don't need the CountersImpl part of the MapResult input here but we
    // accept it to avoid needing an adapter job to connect this job to MapJob's result.
    @Getter
    private final Stage stage = Stage.MERGE;
    @Getter
    @NonNull private final Key MRJobKey;
    @NonNull private final MapReduceSpecification<?, ?, ?, ?, ?> mrSpec;
    @NonNull private final MapReduceSettings settings;
    @NonNull private final Integer tier;

    @Setter(onMethod = @__(@VisibleForTesting))
    private transient Datastore datastore;

    protected Datastore getDatastore() {
      if (datastore == null) {
        datastore = DatastoreOptions.getDefaultInstance().toBuilder()
          .setNamespace(settings.getNamespace())
          .build().getService();
      }
      return datastore;
    }

    @Override
    public String toString() {
      return getClass().getSimpleName() + "(" + getShardedJobId().asEncodedString() + ")";
    }

    /**
     * Takes in the the result of the sort stage, the files are read and re-written, merging
     * multiple files into one in the process. In the event that multiple phases are required this
     * method will invoke itself recursively.
     */
    @Override
    public Value<MapReduceResult<FilesByShard>> run(MapReduceResult<FilesByShard> priorResult) {

      //preempt NPE for priorResult; not sure how this happens exactly, but seems like we probably have a bug rolling
      // up empty collections of FilesByShard somehow
      if (priorResult == null) {
        throw new Error("Prior result passed to " + getShardedJobId() + " was null");
      }

      Context context = new BaseContext(getShardedJobId());
      FilesByShard sortFiles = priorResult.getOutputResult();
      int maxFilesPerShard = findMaxFilesPerShard(sortFiles);
      if (maxFilesPerShard <= settings.getMergeFanin()) {
        //no merge needed
        return immediate(priorResult);
      }

      GoogleCloudStorageLineInput.BaseOptions inputOptions = GoogleCloudStorageLineInput.BaseOptions.defaults();
      GoogleCloudStorageFileOutput.BaseOptions outputOptions = GoogleCloudStorageFileOutput.BaseOptions.defaults();
      if (settings.getServiceAccountKey() != null) {
        inputOptions = inputOptions.withServiceAccountKey(settings.getServiceAccountKey());
        outputOptions = outputOptions.withServiceAccountKey(settings.getServiceAccountKey());
      }

      GoogleCloudStorageMergeInput input =
          new GoogleCloudStorageMergeInput(sortFiles, settings.getMergeFanin(), inputOptions);
      ((Input<?>) input).setContext(context);
      List<? extends InputReader<KeyValue<ByteBuffer, Iterator<ByteBuffer>>>> readers =
          input.createReaders();

      Output<KeyValue<ByteBuffer, List<ByteBuffer>>, FilesByShard> output =
          new GoogleCloudStorageMergeOutput(settings.getBucketName(), getMRJobId(), tier, outputOptions);
      output.setContext(context);

      List<? extends OutputWriter<KeyValue<ByteBuffer, List<ByteBuffer>>>> writers =
          output.createWriters(readers.size());
      Preconditions.checkState(readers.size() == writers.size(), "%s: %s readers, %s writers",
          getShardedJobId(), readers.size(), writers.size());
      ImmutableList.Builder<WorkerShardTask<KeyValue<ByteBuffer, Iterator<ByteBuffer>>,
          KeyValue<ByteBuffer, List<ByteBuffer>>, MergeContext>> mergeTasks =
          ImmutableList.builder();
      for (int i = 0; i < readers.size(); i++) {
        mergeTasks.add(new MergeShardTask(getShardedJobId(),
            i,
            readers.size(),
            readers.get(i),
            writers.get(i),
            settings.getSortReadTimeMillis()));
      }
      ShardedJobSettings shardedJobSettings =
          settings.toShardedJobSettings(getJobKey(), getPipelineKey());

      PromisedValue<ResultAndStatus<FilesByShard>> resultAndStatus = newPromise();
      WorkerController<KeyValue<ByteBuffer, Iterator<ByteBuffer>>,
          KeyValue<ByteBuffer, List<ByteBuffer>>, FilesByShard, MergeContext> workerController =
          new WorkerController<>(getShardedJobId(), priorResult.getCounters(), output, resultAndStatus.getHandle());

      ShardedJob<?> shardedJob =
          new ShardedJob<>(getShardedJobId(), mergeTasks.build(), workerController, shardedJobSettings);
      FutureValue<Void> shardedJobResult = futureCall(shardedJob, settings.toJobSettings());

      FutureValue<MapReduceResult<FilesByShard>> finished = futureCall(
          new ExamineStatusAndReturnResult<>(getShardedJobId()),
          resultAndStatus, settings.toJobSettings(waitFor(shardedJobResult),
              statusConsoleUrl(shardedJobSettings.getMapReduceStatusUrl())));
      futureCall(new MapReduceJob.Cleanup(settings), immediate(priorResult), waitFor(finished));
      return futureCall(new MergeJob(getMRJobKey(), mrSpec, settings, tier + 1), finished,
          settings.toJobSettings(maxAttempts(1)));
    }

    @SuppressWarnings("unused")
    public Value<FilesByShard> handleException(CancellationException ex) {
      getPipelineOrchestrator().abortJob(getShardedJobId());
      return null;
    }
  }

  private static int findMaxFilesPerShard(FilesByShard byShard) {
    int max = 0;
    for (int shard = 0; shard < byShard.getShardCount(); shard++) {
      max = Math.max(max, byShard.getFilesForShard(shard).getNumFiles());
    }
    return max;
  }

  /**
   * The pipeline job to execute the Reduce stage of the MapReduce. (For all shards)
   */
  @RequiredArgsConstructor
  static class ReduceJob<K, V, O, R> extends Job1<MapReduceResult<R>,
      MapReduceResult<FilesByShard>> implements MRStage {

    private static final long serialVersionUID = 1L;

    @Getter
    private final Stage stage = Stage.REDUCE;
    @Getter
    @NonNull private final Key MRJobKey;
    @NonNull private final MapReduceSpecification<?, K, V, O, R> mrSpec;
    @NonNull private final MapReduceSettings settings;

    @Setter(onMethod = @__(@VisibleForTesting))
    private transient Datastore datastore;

    protected Datastore getDatastore() {
      if (datastore == null) {
        datastore = DatastoreOptions.getDefaultInstance().toBuilder()
          .setNamespace(settings.getNamespace())
          .build().getService();
      }
      return datastore;
    }

    @Override
    public String toString() {
      return getClass().getSimpleName() + "(" + getShardedJobId().asEncodedString() + ")";
    }

    /**
     * Takes in the output from merge, and creates a sharded task to call the reducer with the
     * ordered input.
     * The way the data is read in is defined by {@link GoogleCloudStorageReduceInput}
     */
    @Override
    public Value<MapReduceResult<R>> run(MapReduceResult<FilesByShard> mergeResult) {
      Context context = new BaseContext(getShardedJobId());
      Output<O, R> output = mrSpec.getOutput();
      output.setContext(context);
      GoogleCloudStorageReduceInput<K, V> input = new GoogleCloudStorageReduceInput<>(
          mergeResult.getOutputResult(), mrSpec.getKeyMarshaller(), mrSpec.getValueMarshaller(), GoogleCloudStorageLineInput.BaseOptions.defaults().withServiceAccountKey(settings.getServiceAccountKey()));
      ((Input<?>) input).setContext(context);
      List<? extends InputReader<KeyValue<K, Iterator<V>>>> readers = input.createReaders();

      List<? extends OutputWriter<O>> writers = output.createWriters(mrSpec.getNumReducers());
      Preconditions.checkArgument(readers.size() == writers.size(), "%s: %s readers, %s writers",
        getShardedJobId(), readers.size(), writers.size());
      ImmutableList.Builder<WorkerShardTask<KeyValue<K, Iterator<V>>, O, ReducerContext<O>>>
          reduceTasks = ImmutableList.builder();
      for (int i = 0; i < readers.size(); i++) {
        reduceTasks.add(new ReduceShardTask<>(getShardedJobId(), i, readers.size(), readers.get(i),
            mrSpec.getReducer(), writers.get(i), settings.getMillisPerSlice()));
      }
      ShardedJobSettings shardedJobSettings =
          settings.toShardedJobSettings(getJobKey(), getPipelineKey());
      PromisedValue<ResultAndStatus<R>> resultAndStatus = newPromise();
      WorkerController<KeyValue<K, Iterator<V>>, O, R, ReducerContext<O>> workerController =
          new WorkerController<>(getShardedJobId(), mergeResult.getCounters(), output,
              resultAndStatus.getHandle());
      ShardedJob<?> shardedJob =
          new ShardedJob<>(getShardedJobId(), reduceTasks.build(), workerController, shardedJobSettings);
      FutureValue<Void> shardedJobResult = futureCall(shardedJob, settings.toJobSettings());
      return futureCall(new ExamineStatusAndReturnResult<>(getShardedJobId()), resultAndStatus,
          settings.toJobSettings(waitFor(shardedJobResult),
              statusConsoleUrl(shardedJobSettings.getMapReduceStatusUrl())));
    }

    @SuppressWarnings("unused")
    public Value<MapReduceResult<R>> handleException(CancellationException ex) {
      getPipelineOrchestrator().abortJob(getShardedJobId());
      return null;
    }
  }

  @RequiredArgsConstructor
  static class Cleanup extends Job1<Void, MapReduceResult<FilesByShard>> {

    private static final long serialVersionUID = 4559443543355672948L;

    @NonNull
    private final MapReduceSettings settings;

    @Override
    public Value<Void> run(MapReduceResult<FilesByShard> result) {
      Set<GcsFilename> toDelete = new HashSet<>();

      FilesByShard filesByShard = result.getOutputResult();
      for (int i = 0; i < filesByShard.getShardCount(); i++) {
        toDelete.addAll(filesByShard.getFilesForShard(i).getFiles());
      }

      // this calls a pipeline job async, in practice a fire-and-forget operation; if fails, files above as well as
      // records of cleanup job will be left behind; alternatively, we could depend on expiration of these files somehow,
      // in which case none of this would be needed
      CleanupPipelineJob.cleanup(getPipelineService(), settings, new ArrayList<>(toDelete), settings.toJobSettings());


      return null;
    }
  }

  @Override
  public Value<MapReduceResult<R>> run() {
    MapReduceSettings settings = this.settings;
    if (settings.getWorkerQueueName() == null) {
      String queue = getOnQueue();
      if (queue == null) {
        log.warning("workerQueueName is null and current queue is not available in the pipeline"
            + " job, using 'default'");
        queue = "default";
      }
      settings = new MapReduceSettings.Builder(settings).setWorkerQueueName(queue).build();
    }
    FutureValue<MapReduceResult<FilesByShard>> mapResult = futureCall(
        new MapJob<>(getJobKey(), specification, settings), settings.toJobSettings(maxAttempts(1)));
    FutureValue<MapReduceResult<FilesByShard>> sortResult = futureCall(
        new SortJob(getJobKey(), specification, settings), mapResult, settings.toJobSettings(maxAttempts(1)));
    FutureValue<MapReduceResult<FilesByShard>> mergeResult = futureCall(
        new MergeJob(getJobKey(), specification, settings, 1), sortResult, settings.toJobSettings(maxAttempts(1)));
    FutureValue<MapReduceResult<R>> reduceResult = futureCall(
        new ReduceJob<>(getJobKey(), specification, settings), mergeResult, settings.toJobSettings(maxAttempts(1)));
    futureCall(new Cleanup(settings), mapResult, waitFor(sortResult));
    futureCall(new Cleanup(settings), mergeResult, waitFor(reduceResult));
    return reduceResult;
  }

  public Value<MapReduceResult<R>> handleException(Throwable t) throws Throwable {
    log.log(Level.SEVERE, "MapReduce job " + getJobKey().getName() + " failed because of: ", t);
    throw t;
  }

  @Override
  public String getJobDisplayName() {
    return Optional.fromNullable(specification.getJobName()).or(super.getJobDisplayName());
  }
}
