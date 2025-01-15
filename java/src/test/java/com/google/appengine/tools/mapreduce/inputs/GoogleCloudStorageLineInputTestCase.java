package com.google.appengine.tools.mapreduce.inputs;

import com.google.appengine.tools.development.testing.LocalDatastoreServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalServiceTestHelper;

import com.google.appengine.tools.test.CloudStorageExtensions;
import com.google.cloud.WriteChannel;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import lombok.Getter;
import lombok.Setter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.io.IOException;
import java.nio.ByteBuffer;


@CloudStorageExtensions
abstract class GoogleCloudStorageLineInputTestCase {

  private final LocalServiceTestHelper helper = new LocalServiceTestHelper(
      new LocalDatastoreServiceTestConfig());

  @BeforeEach
  public void setUp() throws Exception {
    helper.setUp();
  }

  @Getter
  @Setter(onMethod_ = @BeforeEach)
  Storage storage;

  @Getter
  String bucket;

  long createFile(String filename, String record, int recordsCount) throws IOException {
    try (WriteChannel writeChannel = storage.writer(BlobInfo.newBuilder(bucket, filename).setContentType("application/bin").build())) {
      for (int i = 0; i < recordsCount; i++) {
        writeChannel.write(ByteBuffer.wrap(record.getBytes()));
      }
    }
    return storage.get(BlobId.of(bucket, filename)).getSize();
  }

  @AfterEach
  public void tearDown() throws Exception {
    helper.tearDown();
  }
}
