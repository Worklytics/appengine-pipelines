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


import com.google.appengine.api.blobstore.dev.LocalBlobstoreService;
import com.google.appengine.api.taskqueue.QueueFactory;
import com.google.appengine.tools.cloudtasktest.JakartaServletInvokingTaskCallback;
import com.google.appengine.tools.development.ApiProxyLocal;
import com.google.appengine.tools.development.testing.LocalMemcacheServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalModulesServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalServiceTestHelper;
import com.google.appengine.tools.development.testing.LocalTaskQueueTestConfig;
import com.google.appengine.tools.mapreduce.*;
import com.google.appengine.tools.mapreduce.impl.sort.LexicographicalComparator;
import com.google.appengine.tools.mapreduce.impl.util.RequestUtils;
import com.google.appengine.tools.mapreduce.inputs.GoogleCloudStorageLevelDbInputReader;
import com.google.appengine.tools.mapreduce.inputs.GoogleCloudStorageLineInput;
import com.google.appengine.tools.mapreduce.outputs.GoogleCloudStorageFileOutput;
import com.google.appengine.tools.mapreduce.outputs.GoogleCloudStorageFileOutputWriter;
import com.google.appengine.tools.mapreduce.outputs.LevelDbOutputWriter;
import com.google.appengine.tools.mapreduce.servlets.ShufflerServlet.ShuffleMapReduce;
import com.google.appengine.tools.pipeline.PipelineService;
import com.google.appengine.tools.pipeline.di.JobRunServiceComponent;
import com.google.appengine.tools.pipeline.impl.servlets.PipelineServlet;
import com.google.apphosting.api.ApiProxy;
import com.google.cloud.ReadChannel;
import com.google.cloud.datastore.Datastore;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.TreeMultimap;

import lombok.Getter;
import lombok.Setter;
import lombok.SneakyThrows;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for {@link ShufflerServlet}
 */
@PipelineSetupExtensions
public class ShufflerServletTest {

  private static final Logger log = Logger.getLogger(ShufflerServletTest.class.getName());

  private static final String CALLBACK_PATH = "/callback";

  private static final int MAX_RECORD_SIZE = 500;

  private static final Marshaller<KeyValue<ByteBuffer, ByteBuffer>> KEY_VALUE_MARSHALLER =
      Marshallers.getKeyValueMarshaller(Marshallers.getByteBufferMarshaller(),
          Marshallers.getByteBufferMarshaller());

  private static final
      Marshaller<KeyValue<ByteBuffer, ? extends Iterable<ByteBuffer>>> KEY_VALUES_MARSHALLER =
          Marshallers.getKeyValuesMarshaller(Marshallers.getByteBufferMarshaller(),
              Marshallers.getByteBufferMarshaller());

  private final static int RECORDS_PER_FILE = 10;

  private static final Semaphore WAIT_ON = new Semaphore(0);

  private final LocalServiceTestHelper helper = new LocalServiceTestHelper(
      new LocalTaskQueueTestConfig().setDisableAutoTaskExecution(false).setCallbackClass(TaskRunner.class),
      new LocalMemcacheServiceTestConfig(),
      new LocalModulesServiceTestConfig()
  );


  public static class TaskRunner extends JakartaServletInvokingTaskCallback {

    static Map<String, HttpServlet> servletMap;

    @Override
    protected Map<String, HttpServlet> getServletMap() {
      return servletMap;
    }

    @SuppressWarnings("serial")
    @Override
    protected HttpServlet getDefaultServlet() {
      return new HttpServlet() {};
    }

    static Map<String, String> extraParamValues;

    @Override
    public Map<String, String> getExtraParamValues() {
      return this.extraParamValues;
    }
  }

  private static class CallbackServlet extends HttpServlet {
    private static final long serialVersionUID = 1L;

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException,
        IOException {
      WAIT_ON.release();
    }
  }

  @Getter
  CloudStorageIntegrationTestHelper storageIntegrationTestHelper;

  @Setter(onMethod_ = @BeforeEach)
  PipelineService pipelineService;

  @BeforeEach
  public void setUp(JobRunServiceComponent component,
                    Datastore datastore) throws Exception {
    helper.setUp();
    ApiProxyLocal proxy = (ApiProxyLocal) ApiProxy.getDelegate();
    // Creating files is not allowed in some test execution environments, so don't.
    proxy.setProperty(LocalBlobstoreService.NO_STORAGE_PROPERTY, "true");
    WAIT_ON.drainPermits();
    storageIntegrationTestHelper = new CloudStorageIntegrationTestHelper();
    storageIntegrationTestHelper.setUp();

    TaskRunner.extraParamValues = Map.of(RequestUtils.Params.DATASTORE_HOST,
      datastore.getOptions().getHost());

    PipelineServlet pipelineServlet = new PipelineServlet();
    pipelineServlet.init();
    MapReduceServlet mapReduceServlet = new MapReduceServlet();
    mapReduceServlet.init();

    TaskRunner.servletMap = new ImmutableMap.Builder<String, HttpServlet>()
      .put("/mapreduce", mapReduceServlet)
      .put("/_ah/pipeline", pipelineServlet)
      .put(CALLBACK_PATH, new CallbackServlet())
      .build();
  }

  @AfterEach
  public void tearDown() throws Exception {
    for (int count = 0; getQueueDepth() > 0; count++) {
      if (count > 10) {
        log.severe("Messages did not drain from queue.");
        break;
      }
      Thread.sleep(1000);
    }
    helper.tearDown();
  }



  static final int SHUFFLE_VERIFICATION_RETRIES = 3;

  @SneakyThrows
  @Test
  public void testDataIsOrdered() throws InterruptedException, IOException {
    final int INPUT_FILES_FOR_TEST = 3;
    final int OUTPUT_SHARDS = 2;
    ShufflerParams shufflerParams =
      createParams(storageIntegrationTestHelper.getBase64EncodedServiceAccountKey(), storageIntegrationTestHelper.getBucket(), INPUT_FILES_FOR_TEST, OUTPUT_SHARDS);

    // for test purposes, give a manifest file name that's unique, yet known outside of the shuffle stage of the map reduce job
    // (in usual case, derived from the shuffle stage's job id, which isn't known outside)

    shufflerParams.setManifestFileNameOverride(UUID.randomUUID().toString());

    TreeMultimap<ByteBuffer, ByteBuffer> input = writeInputFiles(shufflerParams, new Random(0));
    assertEquals(INPUT_FILES_FOR_TEST * RECORDS_PER_FILE, input.keySet().size());


    ShuffleMapReduce mr = new ShuffleMapReduce(shufflerParams);
    pipelineService.startNewPipeline(mr);

    assertTrue(WAIT_ON.tryAcquire(100, TimeUnit.SECONDS));

    TreeMultimap<ByteBuffer, ByteBuffer> notSeen;
    int retriesRemaining = SHUFFLE_VERIFICATION_RETRIES;
    do {
      Thread.sleep((retriesRemaining - SHUFFLE_VERIFICATION_RETRIES) * 1000L);

      List<KeyValue<ByteBuffer, List<ByteBuffer>>> output = validateOrdered(shufflerParams);
      notSeen = assertExpectedOutput(input, output);
    } while (!notSeen.isEmpty() && retriesRemaining-- > 0);

    assertTrue(notSeen.isEmpty());
    assertEquals(SHUFFLE_VERIFICATION_RETRIES, retriesRemaining, "Passed, but only after retries");
  }

  @Test
  public void testJson() throws IOException {
    ShufflerParams shufflerParams = createParams(storageIntegrationTestHelper.getBase64EncodedServiceAccountKey(), storageIntegrationTestHelper.getBucket(), 3, 2);
    Marshaller<ShufflerParams> marshaller =
        Marshallers.getGenericJsonMarshaller(ShufflerParams.class);
    ByteBuffer bytes = marshaller.toBytes(shufflerParams);
    ByteArrayInputStream bin = new ByteArrayInputStream(bytes.array());
    ShufflerParams readShufflerParams = ShufflerServlet.readShufflerParams(bin);
    assertEquals(shufflerParams.getShufflerQueue(), readShufflerParams.getShufflerQueue());
    assertEquals(shufflerParams.getGcsBucket(), readShufflerParams.getGcsBucket());
    assertArrayEquals(shufflerParams.getInputFileNames(), readShufflerParams.getInputFileNames());
    assertEquals(shufflerParams.getOutputDir(), readShufflerParams.getOutputDir());
    assertEquals(shufflerParams.getOutputShards(), readShufflerParams.getOutputShards());
    assertEquals(shufflerParams.getCallbackQueue(), readShufflerParams.getCallbackQueue());
    assertEquals(shufflerParams.getCallbackService(), readShufflerParams.getCallbackService());
    assertEquals(shufflerParams.getCallbackVersion(), readShufflerParams.getCallbackVersion());
    assertEquals(shufflerParams.getCallbackPath(), readShufflerParams.getCallbackPath());
    assertEquals(shufflerParams.getServiceAccountKey(), readShufflerParams.getServiceAccountKey());
  }

  private TreeMultimap<ByteBuffer, ByteBuffer> assertExpectedOutput(TreeMultimap<ByteBuffer, ByteBuffer> expected,
                                                                    List<KeyValue<ByteBuffer, List<ByteBuffer>>> actual) {
    TreeMultimap<ByteBuffer, ByteBuffer> notYetSeen = TreeMultimap.create();
    notYetSeen.putAll(expected);
    for (KeyValue<ByteBuffer, List<ByteBuffer>> kv : actual) {
      SortedSet<ByteBuffer> expectedValues = notYetSeen.removeAll(kv.getKey());
      assertTrue(expectedValues.containsAll(kv.getValue()));
      assertTrue(kv.getValue().containsAll(expectedValues));
    }
    return notYetSeen;
  }

  List<KeyValue<ByteBuffer, List<ByteBuffer>>> validateOrdered(ShufflerParams shufflerParams) throws IOException {
    List<KeyValue<ByteBuffer, List<ByteBuffer>>> result = new ArrayList<>();

    GcsFilename manifest = ShuffleMapReduce.getManifestFile(null, shufflerParams);

    List<GcsFilename> outputFiles;
    try (ReadChannel readChannel = storageIntegrationTestHelper.getStorage().get(manifest.asBlobId()).reader()) {
      byte[] manifestBytes = new byte[4000];
      int read = readChannel.read(ByteBuffer.wrap(manifestBytes));
      String manifestContent = new String(manifestBytes, 0, read, "UTF-8");
      outputFiles = Arrays.stream(manifestContent.split("\n"))
        .map(s -> new GcsFilename(shufflerParams.getGcsBucket(), s))
          .collect(Collectors.toList());
    }

    assertEquals(shufflerParams.getOutputShards(), outputFiles.size());

    for (GcsFilename file : outputFiles) {
      GoogleCloudStorageLevelDbInputReader reader = new GoogleCloudStorageLevelDbInputReader(file,
        GoogleCloudStorageLineInput.BaseOptions.defaults().withServiceAccountKey(shufflerParams.getServiceAccountKey()));
      reader.beginShard();
      reader.beginSlice();
      try {
        ByteBuffer previous = null;
        while (true) {
          KeyValue<ByteBuffer, ? extends Iterable<ByteBuffer>> keyValue =
              KEY_VALUES_MARSHALLER.fromBytes(reader.next());
          assertTrue(previous == null
              || LexicographicalComparator.compareBuffers(previous, keyValue.getKey()) < 0);
          ArrayList<ByteBuffer> list = new ArrayList<>();
          for (ByteBuffer item : keyValue.getValue()) {
            list.add(item);
          }
          result.add(new KeyValue<>(keyValue.getKey(), list));
        }
      } catch (NoSuchElementException e) {
        // reader has no more values
      }
      reader.endSlice();
      reader.endShard();
    }
    return result;
  }
  GoogleCloudStorageFileOutput.Options outputOptions() {
    return GoogleCloudStorageFileOutput.BaseOptions.defaults()
      .withServiceAccountKey(storageIntegrationTestHelper.getBase64EncodedServiceAccountKey());
  }

  private TreeMultimap<ByteBuffer, ByteBuffer> writeInputFiles(ShufflerParams shufflerParams,
      Random rand) throws IOException {
    LexicographicalComparator comparator = new LexicographicalComparator();
    TreeMultimap<ByteBuffer, ByteBuffer> result = TreeMultimap.create(comparator, comparator);
    for (String fileName : shufflerParams.getInputFileNames()) {
      LevelDbOutputWriter writer = new LevelDbOutputWriter(new GoogleCloudStorageFileOutputWriter(
          new GcsFilename(shufflerParams.getGcsBucket(), fileName), "text/plain; charset=UTF-8", outputOptions()));
      writer.beginShard();
      writer.beginSlice();
      for (int i = 0; i < RECORDS_PER_FILE; i++) {
        KeyValue<ByteBuffer, ByteBuffer> kv = writeRandomKVByteBuffer(rand, writer);
        result.put(kv.getKey(), kv.getValue());
      }
      writer.endSlice();
      writer.endShard();
    }
    return result;
  }

  private KeyValue<ByteBuffer, ByteBuffer> writeRandomKVByteBuffer(Random rand,
      LevelDbOutputWriter writer) throws IOException {
    byte[] key = new byte[rand.nextInt(5) + 1];
    byte[] value = new byte[rand.nextInt(MAX_RECORD_SIZE)];
    rand.nextBytes(key);
    rand.nextBytes(value);
    KeyValue<ByteBuffer, ByteBuffer> keyValue =
        new KeyValue<>(ByteBuffer.wrap(key), ByteBuffer.wrap(value));
    writer.write(KEY_VALUE_MARSHALLER.toBytes(keyValue));
    return keyValue;
  }

  static ShufflerParams createParams(String serviceAccountKey, String bucket, int inputFiles, int outputShards) {
    ShufflerParams shufflerParams = new ShufflerParams();
    shufflerParams.setCallbackService("default");
    shufflerParams.setCallbackVersion("callbackVersion");
    shufflerParams.setCallbackPath(CALLBACK_PATH);
    shufflerParams.setCallbackQueue("default");
    ArrayList<String> list = new ArrayList<>();
    for (int i = 0; i < inputFiles; i++) {
      list.add("input" + i);
    }
    shufflerParams.setInputFileNames(list.toArray(new String[inputFiles]));
    shufflerParams.setOutputShards(outputShards);
    shufflerParams.setShufflerQueue("default");
    shufflerParams.setGcsBucket(bucket);
    shufflerParams.setOutputDir("storageDir");
    shufflerParams.setServiceAccountKey(serviceAccountKey);
    return shufflerParams;
  }


  private int getQueueDepth() {
    return LocalTaskQueueTestConfig
      .getLocalTaskQueue()
      .getQueueStateInfo()
      .get(QueueFactory.getDefaultQueue().getQueueName())
      .getTaskInfo()
      .size();
  }

}
