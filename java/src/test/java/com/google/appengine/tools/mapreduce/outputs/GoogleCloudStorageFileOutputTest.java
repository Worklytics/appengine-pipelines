package com.google.appengine.tools.mapreduce.outputs;


import com.google.appengine.tools.mapreduce.CloudStorageIntegrationTestHelper;
import com.google.appengine.tools.mapreduce.GoogleCloudStorageFileSet;
import com.google.appengine.tools.mapreduce.OutputWriter;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import lombok.Getter;
import org.junit.BeforeClass;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.util.List;
import java.util.Random;

import static com.google.cloud.MetadataConfig.getProjectId;
import static org.junit.jupiter.api.Assertions.*;

public class GoogleCloudStorageFileOutputTest {

  private static final String FILE_NAME_PATTERN = "shard-%02x";
  private static final String MIME_TYPE = "text/ascii";
  private static final int NUM_SHARDS = 3; //let's not go crazy
  private static final byte[] SMALL_CONTENT = "content".getBytes();

  // This size chosen so that it is larger than the buffer and on the first and second buffer fills
  // there will be some left over.
  private static final byte[] LARGE_CONTENT = new byte[(int) (1024 * 1024 * 2.5)];

  @Getter
  CloudStorageIntegrationTestHelper storageIntegrationTestHelper;

  @BeforeClass
  public static void setupStorage() {

  }
  @BeforeEach
  protected void setUp() throws Exception {
    storageIntegrationTestHelper = new CloudStorageIntegrationTestHelper();
    storageIntegrationTestHelper.setUp();

    // Filling the large_content buffer with a non-repeating but consistent pattern.
    Random r = new Random(0);
    r.nextBytes(LARGE_CONTENT);
  }


  @AfterEach
  protected void tearDown() throws Exception {
    storageIntegrationTestHelper.tearDown();
  }

  @Test
  public void testFilesAreWritten() throws IOException {
    GoogleCloudStorageFileOutput creator =
        new GoogleCloudStorageFileOutput(storageIntegrationTestHelper.getBucket(), FILE_NAME_PATTERN, MIME_TYPE, GoogleCloudStorageFileOutput.BaseOptions.defaults().withServiceAccountKey(storageIntegrationTestHelper.getBase64EncodedServiceAccountKey()).withProjectId(getProjectId()));
    List<? extends OutputWriter<ByteBuffer>> writers = creator.createWriters(NUM_SHARDS);
    assertEquals(NUM_SHARDS, writers.size());
    beginShard(writers);
    for (int i = 0; i < NUM_SHARDS; i++) {
      OutputWriter<ByteBuffer> out = writers.get(i);
      out.beginSlice();
      out.write(ByteBuffer.wrap(SMALL_CONTENT));
      out.endSlice();
      out.endShard();
    }
    GoogleCloudStorageFileSet files = creator.finish(writers);
    assertEquals(NUM_SHARDS, files.getNumFiles());
    for (int i = 0; i < NUM_SHARDS; i++) {
      Blob blob = storageIntegrationTestHelper.getStorage().get(BlobId.of(files.getFile(i).getBucketName(), files.getFile(i).getObjectName()));
      assertNotNull(blob);
      assertEquals(SMALL_CONTENT.length, (long) blob.getSize());
      assertEquals(MIME_TYPE, blob.getContentType());
    }
  }

  private void beginShard(List<? extends OutputWriter<ByteBuffer>> writers) throws IOException {
    for (OutputWriter<ByteBuffer> writer : writers) {
      writer.beginShard();
    }
  }

  @Test
  public void testSmallSlicing() throws IOException, ClassNotFoundException {
    testSlicing(SMALL_CONTENT);
  }

  @Test
  public void testLargeSlicing() throws IOException, ClassNotFoundException {
    testSlicing(LARGE_CONTENT);
  }

  private void testSlicing(byte[] content) throws IOException, ClassNotFoundException {
    GoogleCloudStorageFileOutput creator =
      new GoogleCloudStorageFileOutput(storageIntegrationTestHelper.getBucket(), FILE_NAME_PATTERN, MIME_TYPE, GoogleCloudStorageFileOutput.BaseOptions.defaults().withServiceAccountKey(storageIntegrationTestHelper.getBase64EncodedServiceAccountKey()).withProjectId(getProjectId()));
    List<? extends OutputWriter<ByteBuffer>> writers = creator.createWriters(NUM_SHARDS);
    assertEquals(NUM_SHARDS, writers.size());
    beginShard(writers);
    for (int i = 0; i < NUM_SHARDS; i++) {
      OutputWriter<ByteBuffer> out = writers.get(i);
      out.beginSlice();
      out.write(ByteBuffer.wrap(content));
      out.endSlice();
      out = reconstruct(out);
      out.beginSlice();
      out.write(ByteBuffer.wrap(content));
      out.endSlice();
      out.endShard();
    }
    GoogleCloudStorageFileSet files = creator.finish(writers);
    assertEquals(NUM_SHARDS, files.getNumFiles());
    ByteBuffer expectedContent = ByteBuffer.allocate(content.length * 2);
    expectedContent.put(content);
    expectedContent.put(content);
    for (int i = 0; i < NUM_SHARDS; i++) {
      expectedContent.rewind();
      ByteBuffer actualContent = ByteBuffer.allocate(content.length * 2 + 1);
      BlobId blobId = BlobId.of(files.getFile(i).getBucketName(), files.getFile(i).getObjectName());
      Blob blob = storageIntegrationTestHelper.getStorage().get(BlobId.of(files.getFile(i).getBucketName(), files.getFile(i).getObjectName()));
      assertNotNull(blob);
      assertEquals(expectedContent.capacity(), (long) blob.getSize());
      assertEquals(MIME_TYPE, blob.getContentType());
      try (ReadableByteChannel readChannel = storageIntegrationTestHelper.getStorage().reader(blobId)) {
        int read = readChannel.read(actualContent);
        assertEquals(read, content.length * 2);
        actualContent.limit(actualContent.position());
        actualContent.rewind();
        assertEquals(expectedContent, actualContent);
      }
    }
  }

  private OutputWriter<ByteBuffer> reconstruct(OutputWriter<ByteBuffer> writer) throws IOException,
      ClassNotFoundException {
    ByteArrayOutputStream bout = new ByteArrayOutputStream();
    try (ObjectOutputStream oout = new ObjectOutputStream(bout)) {
      oout.writeObject(writer);
    }
    assertTrue(bout.size() < 1000 * 1000); // Should fit in datastore.
    ByteArrayInputStream bin = new ByteArrayInputStream(bout.toByteArray());
    ObjectInputStream oin = new ObjectInputStream(bin);
    return (OutputWriter<ByteBuffer>) oin.readObject();
  }
}
