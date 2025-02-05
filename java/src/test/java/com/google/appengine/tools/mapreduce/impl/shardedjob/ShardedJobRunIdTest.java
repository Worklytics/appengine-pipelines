package com.google.appengine.tools.mapreduce.impl.shardedjob;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class ShardedJobRunIdTest {

  @Test
  void testToString() {
    assertEquals("ShardedJobRunId(project:databaseId:namespace:jobId)",
      ShardedJobRunId.of("project", "databaseId", "namespace", "jobId").toString());
  }
}