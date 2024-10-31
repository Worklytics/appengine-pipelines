package com.google.appengine.tools.mapreduce.impl.shardedjob;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class IncrementalTaskIdTest {

  @Test
  void parse() {

    IncrementalTaskId id = IncrementalTaskId.of(ShardedJobRunId.of("test-project", "db", "ns", "123124"), 0);


    assertEquals("test-project/db/ns/123124-task-0", id.asEncodedString());
    assertEquals(id, IncrementalTaskId.parse(id.asEncodedString()));

  }
}