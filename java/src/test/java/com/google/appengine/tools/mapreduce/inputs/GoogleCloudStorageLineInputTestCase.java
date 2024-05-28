package com.google.appengine.tools.mapreduce.inputs;

import com.google.appengine.tools.development.testing.LocalDatastoreServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalServiceTestHelper;

import com.google.appengine.tools.mapreduce.CloudStorageIntegrationTestHelper;
import com.google.cloud.WriteChannel;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.io.IOException;
import java.nio.ByteBuffer;


abstract class GoogleCloudStorageLineInputTestCase {

  CloudStorageIntegrationTestHelper cloudStorageIntegrationTestHelper;

  private final LocalServiceTestHelper helper = new LocalServiceTestHelper(
      new LocalDatastoreServiceTestConfig());

  @BeforeEach
  public void setUp() throws Exception {
    helper.setUp();
    cloudStorageIntegrationTestHelper = new CloudStorageIntegrationTestHelper();
    cloudStorageIntegrationTestHelper.setUp();
  }

  long createFile(String filename, String record, int recordsCount) throws IOException {
    Storage storage = cloudStorageIntegrationTestHelper.getStorage();
    try (WriteChannel writeChannel = storage.writer(BlobInfo.newBuilder(cloudStorageIntegrationTestHelper.getBucket(), filename).setContentType("application/bin").build())) {
      for (int i = 0; i < recordsCount; i++) {
        writeChannel.write(ByteBuffer.wrap(record.getBytes()));
      }
    }
    return cloudStorageIntegrationTestHelper.getStorage().get(BlobId.of(cloudStorageIntegrationTestHelper.getBucket(), filename)).getSize();
  }

  @AfterEach
  public void tearDown() throws Exception {
    helper.tearDown();
    cloudStorageIntegrationTestHelper.tearDown();
  }
}
