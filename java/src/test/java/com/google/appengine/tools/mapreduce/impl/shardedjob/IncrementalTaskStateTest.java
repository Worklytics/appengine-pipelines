package com.google.appengine.tools.mapreduce.impl.shardedjob;

import com.google.appengine.tools.pipeline.DatastoreExtension;
import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.DatastoreOptions;
import com.google.cloud.datastore.Key;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import static org.junit.jupiter.api.Assertions.assertNull;

@ExtendWith(DatastoreExtension.class)
class IncrementalTaskStateTest {

  @Test
  void hasNoParent() {

    Datastore datastore = DatastoreOptions.getDefaultInstance().getService();

    Key exampleKey = IncrementalTaskState.makeKey(datastore, IncrementalTaskId.of(ShardedJobRunId.builder().project("test-project").jobId("job").build(), 1));

    assertNull(exampleKey.getParent());

  }
}