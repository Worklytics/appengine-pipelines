// Copyright 2014 Google Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.google.appengine.tools.mapreduce.servlets;

import com.github.rholder.retry.RetryerBuilder;
import com.github.rholder.retry.StopStrategies;
import com.google.appengine.tools.mapreduce.*;
import com.google.appengine.tools.mapreduce.impl.MapReduceConstants;
import com.google.appengine.tools.mapreduce.impl.shardedjob.ShardedJobRunner;
import com.google.appengine.tools.mapreduce.impl.util.RequestUtils;
import com.google.appengine.tools.mapreduce.inputs.GoogleCloudStorageLevelDbInput;
import com.google.appengine.tools.mapreduce.inputs.GoogleCloudStorageLineInput;
import com.google.appengine.tools.mapreduce.inputs.UnmarshallingInput;
import com.google.appengine.tools.mapreduce.mappers.IdentityMapper;
import com.google.appengine.tools.mapreduce.outputs.GoogleCloudStorageFileOutput;
import com.google.appengine.tools.mapreduce.outputs.GoogleCloudStorageLevelDbOutput;
import com.google.appengine.tools.mapreduce.outputs.MarshallingOutput;
import com.google.appengine.tools.mapreduce.reducers.IdentityReducer;
import com.google.appengine.tools.pipeline.*;
import com.google.appengine.tools.pipeline.di.DaggerJobRunServiceComponent;
import com.google.appengine.tools.pipeline.di.JobRunServiceComponent;
import com.google.appengine.tools.pipeline.di.StepExecutionComponent;
import com.google.appengine.tools.pipeline.di.StepExecutionModule;
import com.google.cloud.WriteChannel;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.io.ByteStreams;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.extern.java.Log;
import org.apache.commons.codec.digest.DigestUtils;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serial;
import java.net.URLEncoder;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.logging.Level;

import static java.util.concurrent.Executors.callable;

/**
 * This servlet provides a way for Python MapReduce Jobs to use the Java MapReduce as a shuffle. It
 * takes in a list of files to shuffle and a task queue to send the completion notification to. When
 * the job finishes a message will be sent to that queue which indicates the status and where to
 * find the results.
 */
@Log
public class ShufflerServlet extends HttpServlet {

  @Setter(onMethod_ = { @VisibleForTesting })
  JobRunServiceComponent component;
  RequestUtils requestUtils;

  @Serial
  private static final long serialVersionUID = 2L;

  private static final String MIME_TYPE = "application/octet-stream";

  private static final int MAX_VALUES_COUNT = 10000;

  private static  RetryerBuilder getRetryerBuilder() {
    return RetryerBuilder.newBuilder()
      .retryIfException((e) ->
        e instanceof Exception
          && !(e instanceof IllegalArgumentException)
      )
      .withWaitStrategy(RetryUtils.defaultWaitStrategy())
      .withStopStrategy(StopStrategies.stopAfterAttempt(10))
      .withRetryListener(RetryUtils.logRetry(log, ShufflerServlet.class.getName()));
  }


  @Override
  public void init(ServletConfig config) throws ServletException {
    super.init(config);
    if (component == null) {
      component = DaggerJobRunServiceComponent.create();
    }
    requestUtils = component.requestUtils();
  }

  @RequiredArgsConstructor
  @VisibleForTesting
  static final class ShuffleMapReduce extends Job0<Void> {

    @Serial
    private static final long serialVersionUID = 2L;

    private final Marshaller<ByteBuffer> identityMarshaller = Marshallers.getByteBufferMarshaller();

    private final ShufflerParams shufflerParams;

    @Override
    public Value<Void> run() throws Exception {
      MapReduceJob<KeyValue<ByteBuffer, ByteBuffer>, ByteBuffer, ByteBuffer,
          KeyValue<ByteBuffer, ? extends Iterable<ByteBuffer>>, GoogleCloudStorageFileSet> shuffleStageJob =
          new MapReduceJob<>(createSpec(), createSettings());

      FutureValue<MapReduceResult<GoogleCloudStorageFileSet>> result = futureCall(shuffleStageJob);


      // Take action once the shuffle stage is complete.
      return futureCall(new Complete(shufflerParams, this.getJobRunId()), result, maxAttempts(10));
    }

    private MapReduceSettings createSettings() {
      return MapReduceSettings.builder()
          .bucketName(shufflerParams.getGcsBucket())
          .workerQueueName(shufflerParams.getShufflerQueue())
          .serviceAccountKey(shufflerParams.getServiceAccountKey())
          .namespace(shufflerParams.getNamespace())
          .build();
    }

    private MapReduceSpecification<KeyValue<ByteBuffer, ByteBuffer>, ByteBuffer, ByteBuffer,
      KeyValue<ByteBuffer, ? extends Iterable<ByteBuffer>>, GoogleCloudStorageFileSet>
        createSpec() {
      return new MapReduceSpecification.Builder<KeyValue<ByteBuffer, ByteBuffer>, ByteBuffer,
          ByteBuffer, KeyValue<ByteBuffer, ? extends Iterable<ByteBuffer>>,
          GoogleCloudStorageFileSet>()

          .setInput(createInput())
          .setMapper(new IdentityMapper<>())
          .setReducer(new IdentityReducer<>(MAX_VALUES_COUNT))
          .setOutput(createOutput())
          .setJobName("Shuffle")
          .setKeyMarshaller(identityMarshaller)
          .setValueMarshaller(identityMarshaller)
          .setNumReducers(shufflerParams.getOutputShards())
          .build();
    }

    private MarshallingOutput<KeyValue<ByteBuffer, ? extends Iterable<ByteBuffer>>,
        GoogleCloudStorageFileSet> createOutput() {
      String jobId = getJobRunId().asEncodedString();

      GoogleCloudStorageFileOutput.Options gcsOutputOptions = GoogleCloudStorageFileOutput.BaseOptions.defaults()
        .withServiceAccountKey(shufflerParams.getServiceAccountKey());

      return new MarshallingOutput<>(
        new GoogleCloudStorageLevelDbOutput(shufflerParams.getGcsBucket(), getOutputNamePattern(jobId), MIME_TYPE, gcsOutputOptions),
          Marshallers.getKeyValuesMarshaller(identityMarshaller, identityMarshaller)
      );
    }

    private String getOutputNamePattern(String jobId) {
      //as JobId might be URL-encoded string, make it safe for a format
      String safeJobId = DigestUtils.sha256Hex(jobId.getBytes());
      return shufflerParams.getOutputDir() + "/sortedData-" + safeJobId + "/shard-%04d";
    }

    /**
     * reference to manifest file for shuffle-phase of a mapReduceJob
     *
     * @param shuffleMapReduceJobId identifies the shuffle stage this manifest file is for
     * @param shufflerParams shuffler parameters
     * @return
     */
    @VisibleForTesting
    static GcsFilename getManifestFile(JobRunId shuffleMapReduceJobId,
                                       ShufflerParams shufflerParams) {
      String fileName = Optional.ofNullable(shufflerParams.getManifestFileNameOverride())
        .orElseGet(() -> DigestUtils.sha256Hex(shuffleMapReduceJobId.asEncodedString()));
      return new GcsFilename(shufflerParams.getGcsBucket(), shufflerParams.getOutputDir() + "/Manifest-" + fileName + ".txt");
    }

    private UnmarshallingInput<KeyValue<ByteBuffer, ByteBuffer>> createInput() {
      List<String> fileNames = Arrays.asList(shufflerParams.getInputFileNames());
      return new UnmarshallingInput<>(new GoogleCloudStorageLevelDbInput(
          new GoogleCloudStorageFileSet(shufflerParams.getGcsBucket(), fileNames), GoogleCloudStorageLineInput.BaseOptions.defaults().withServiceAccountKey(shufflerParams.getServiceAccountKey())),
          Marshallers.getKeyValueMarshaller(identityMarshaller, identityMarshaller));
    }

    /**
     * Logs the error and notifies the requester.
     */
    public Value<Void> handleException(Throwable t) {
      log.log(Level.SEVERE, "Shuffle job failed: jobId=" + getJobRunId().asEncodedString(), t);
      getShardedJobRunner().enqueueCallbackTask(shufflerParams, "job=" + getJobRunId().asEncodedString() + "&status=failed", "Shuffled-" + getJobRunId().getJobId().replace(JobRunId.DELIMITER, "_"));
      return immediate(null);
    }
  }

  /**
   * Save the output filenames in GCS with one filename per line. Then invokes
   * {@link ShardedJobRunner#enqueueCallbackTask}
   */
  @RequiredArgsConstructor
  private static final class Complete extends
      Job1<Void, MapReduceResult<GoogleCloudStorageFileSet>> {

    @Serial
    private static final long serialVersionUID = 2L;
    private final ShufflerParams shufflerParams;
    private final JobRunId shuffleMapReduceJobId;

    @Override
    public Value<Void> run(MapReduceResult<GoogleCloudStorageFileSet> result) throws Exception {

      GcsFilename manifestFile = ShuffleMapReduce.getManifestFile(this.shuffleMapReduceJobId , shufflerParams);

      log.info("Shuffle job done: jobId=" + this.getJobRunId() + ", results located in " + manifestFile + "]");

      Storage client = GcpCredentialOptions.getStorageClient(this.shufflerParams);

      Blob blob = client.create(BlobInfo.newBuilder(manifestFile.asBlobId()).setContentType("text/plain").build());

      WriteChannel output = blob.writer();

      for (com.google.appengine.tools.mapreduce.GcsFilename fileName : result.getOutputResult().getFiles()) {
        output.write(StandardCharsets.UTF_8.encode(fileName.getObjectName()));
        output.write(StandardCharsets.UTF_8.encode("\n"));
      }
      output.close();

      getShardedJobRunner().enqueueCallbackTask(shufflerParams,
          "job=" + this.getJobRunId().asEncodedString() + "&status=done&output=" + URLEncoder.encode(manifestFile.getObjectName(), "UTF-8"),
          "Shuffled-" + this.getJobRunId() .asEncodedString().replace(JobRunId.DELIMITER, "-"));
      return immediate(null);
    }
  }


  @VisibleForTesting
  ShufflerParams readShufflerParams(InputStream in) throws IOException {
    Marshaller<ShufflerParams> marshaller =
        Marshallers.getGenericJsonMarshaller(ShufflerParams.class);
    ShufflerParams params = marshaller.fromBytes(ByteBuffer.wrap(ByteStreams.toByteArray(in)));
    if (params.getOutputShards() <= 0
        || params.getOutputShards() > MapReduceConstants.MAX_REDUCE_SHARDS) {
      throw new IllegalArgumentException(
          "Invalid requested number of shards: " + params.getOutputShards());
    }
    if (params.getOutputDir().length() > 850) {
      throw new IllegalArgumentException(
          "OutputDir is too long: " + params.getOutputDir().length());
    }
    if (params.getOutputDir().contains("\n")) {
      throw new IllegalArgumentException("OutputDir may not contain a newline");
    }
    if (params.getGcsBucket() == null) {
      throw new IllegalArgumentException("GcsBucket parameter is mandatory");
    }
    if (params.getCallbackService() == null || params.getCallbackVersion() == null) {
      throw new IllegalArgumentException(
          "CallbackModule and CallbackVersion parameters are mandatory");
    }
    return params;
  }

  @Override
  public void doPost(HttpServletRequest req, HttpServletResponse resp) throws IOException {
    ShufflerParams shufflerParams = readShufflerParams(req.getInputStream());

    StepExecutionComponent stepExecutionComponent =
      component.stepExecutionComponent(new StepExecutionModule(req));

    PipelineService pipelineService = stepExecutionComponent.pipelineService();

    JobRunId pipelineId = pipelineService.startNewPipeline(
        new ShuffleMapReduce(shufflerParams),
        new JobSetting.OnQueue(shufflerParams.getShufflerQueue()),
        new JobSetting.DatastoreNamespace(shufflerParams.getNamespace()));
    log.info("Started shuffler: jobId=" + pipelineId + ", params=" + shufflerParams);

    resp.setStatus(HttpServletResponse.SC_OK);
    resp.getWriter().append(pipelineId.asEncodedString());
  }

}
