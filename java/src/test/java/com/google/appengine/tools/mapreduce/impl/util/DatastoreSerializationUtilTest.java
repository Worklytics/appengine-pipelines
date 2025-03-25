// Copyright 2012 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce.impl.util;

import com.google.appengine.tools.development.testing.LocalServiceTestHelper;
import com.google.appengine.tools.mapreduce.DatastoreExtension;

import com.google.appengine.tools.txn.PipelineBackendTransaction;
import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.Key;
import com.google.cloud.datastore.Transaction;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Optional;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author ohler@google.com (Christian Ohler)
 */
@ExtendWith({
  DatastoreExtension.class,
  //AppEngineEnvironmentExtension.class,
  DatastoreExtension.ParameterResolver.class,
})
public class DatastoreSerializationUtilTest {

  private final LocalServiceTestHelper helper = new LocalServiceTestHelper();

  Datastore datastore;

  @BeforeEach
  public void injectDatastore(Datastore datastore) {
    this.datastore = datastore;
  }

  @BeforeEach
  protected void setUp() throws Exception {
    helper.setUp();
  }

  @AfterEach
  protected void tearDown() throws Exception {
    helper.tearDown();
  }


  private static class Value implements Serializable {

    private static final long serialVersionUID = -2908491492725087639L;
    private final byte[] bytes;

    Value(int kb) {
      bytes = new byte[kb * 1024];
      new Random().nextBytes(bytes);
    }

    @Override
    public int hashCode() {
      return ByteBuffer.wrap(bytes).getInt();
    }

    @Override
    public boolean equals(Object obj) {
      if (obj instanceof Value) {
        Value other = (Value) obj;
        return Arrays.equals(bytes, other.bytes);
      }
      return false;
    }
  }

  @ParameterizedTest
  @CsvSource(value = {
    "0,0",
    "500,0",
    "1000,2", // sufficient to force sharding
    "2000,3",
    "4000,5",
    // >4000 fails with emulator error: [datastore] io.grpc.StatusRuntimeException: INTERNAL: Frame size 5123097 exceeds maximum: 4194304. If this is normal, increase the maxMessageSize in the channel/server builder
    // 5000, 10000
  })
  public void testSerializeToDatastore(int size, int expectedShardCount) throws Exception {
    Value original = new Value(size);

    PipelineBackendTransaction tx = PipelineBackendTransaction.newInstance(this.datastore);
    Key key = tx.getDatastore().newKeyFactory().setKind("mr-entity").newKey(1+size);
    Entity.Builder entity = Entity.newBuilder(key);
    int shards = DatastoreSerializationUtil.serializeToDatastoreProperty(tx, entity, "foo", original, Optional.of(0));
    assertEquals(expectedShardCount, shards);
    tx.put(entity.build());
    tx.commit();

    //read back in new txn
    Entity fromDb = datastore.get(key);
    assertFalse(tx.isActive());
    tx = PipelineBackendTransaction.newInstance(this.datastore);
    Serializable restored = DatastoreSerializationUtil.deserializeFromDatastoreProperty(tx, fromDb, "foo");
    assertEquals(original, restored);
  }
}
