package com.google.appengine.tools.mapreduce.outputs;

import com.google.appengine.tools.development.testing.LocalServiceTestHelper;
import com.google.appengine.tools.mapreduce.GoogleCloudStorageFileSet;
import com.google.appengine.tools.mapreduce.OutputWriter;
import com.google.appengine.tools.mapreduce.impl.util.SerializationUtil;

import com.google.appengine.tools.test.CloudStorageExtension;
import com.google.appengine.tools.test.CloudStorageExtensions;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import lombok.Getter;
import lombok.Setter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@CloudStorageExtensions
public class SizeSegmentedGoogleCloudStorageFileOutputTest {

  private final LocalServiceTestHelper helper = new LocalServiceTestHelper();

  private static final String MIME_TYPE = "application/json";

  public static final String GCS_FILE_NAME_FORMAT =
    "FilesToLoad/Job-%s/Shard-%%04d/file-%%04d";


  GoogleCloudStorageFileOutput.Options options;

  @Getter
  String bucket;
  @Getter @Setter(onMethod_ = @BeforeEach)
  Storage storage;

  @BeforeEach
  protected void setUp() throws Exception {
    helper.setUp();
    options = GoogleCloudStorageFileOutput.BaseOptions.defaults().withServiceAccountKey(CloudStorageExtension.getBase64EncodedServiceAccountKey()).withProjectId(CloudStorageExtension.getProjectId());
  }

  @AfterEach
  protected void tearDown() throws Exception {
    helper.tearDown();
  }

  @Test
  public void testFilesWritten() throws IOException {
    int segmentSizeLimit = 10;
    String fileNamePattern = String.format(GCS_FILE_NAME_FORMAT, "testJob");
    SizeSegmentedGoogleCloudStorageFileOutput segmenter =
        new SizeSegmentedGoogleCloudStorageFileOutput(bucket, segmentSizeLimit, fileNamePattern,
            MIME_TYPE, options);
    List<? extends OutputWriter<ByteBuffer>> writers = segmenter.createWriters(5);
    List<OutputWriter<ByteBuffer>> finished = new ArrayList<>();
    assertEquals(5, writers.size());
    for (OutputWriter<ByteBuffer> w : writers) {
      w.beginShard();
      w.beginSlice();
      w.write(ByteBuffer.wrap(new byte[9]));
      w.endSlice();
      w = SerializationUtil.clone(w);
      w.beginSlice();
      w.write(ByteBuffer.wrap(new byte[9]));
      w.endSlice();
      w = SerializationUtil.clone(w);
      w.beginSlice();
      w.write(ByteBuffer.wrap(new byte[9]));
      w.endSlice();
      w.endShard();
      finished.add(w);
    }
    GoogleCloudStorageFileSet filesWritten = segmenter.finish(finished);
    assertEquals(15, filesWritten.getNumFiles());
    for (int i = 0; i < filesWritten.getNumFiles(); i++) {
      Blob blob = getStorage().get(filesWritten.getFile(i).asBlobId());
      assertNotNull(blob);
      assertEquals(MIME_TYPE, blob.getContentType());
    }
  }

  @Test
  public void testSegmentation() throws IOException {
    int segmentSizeLimit = 10;
    SizeSegmentedGoogleCloudStorageFileOutput segmenter =
        new SizeSegmentedGoogleCloudStorageFileOutput(getBucket(), segmentSizeLimit, "testJob/%%04d_%d",
           MIME_TYPE, options);
    List<? extends OutputWriter<ByteBuffer>> writers = segmenter.createWriters(5);
    int countFiles = 0;
    for (OutputWriter<ByteBuffer> w : writers) {
      writeMultipleValues(w, 3, 9);
      countFiles += 3;
    }
    GoogleCloudStorageFileSet filesWritten = segmenter.finish(writers);
    assertEquals(countFiles, filesWritten.getNumFiles());
    for (int i = 0; i < filesWritten.getNumFiles(); i++) {
      Blob blob = getStorage().get(filesWritten.getFile(i).asBlobId());
      assertNotNull(blob);
      assertEquals(MIME_TYPE, blob.getContentType());
    }
  }

  /**
   * @param writer
   * @throws IOException
   */
  private void writeMultipleValues(OutputWriter<ByteBuffer> writer, int count, int size)
      throws IOException {
    writer.beginShard();
    writer.beginSlice();
    Random r = new Random(0);
    for (int i = 0; i < count; i++) {
      byte[] data = new byte[size];
      r.nextBytes(data);
      writer.write(ByteBuffer.wrap(data));
    }
    writer.endSlice();
    writer.endShard();
  }
}
