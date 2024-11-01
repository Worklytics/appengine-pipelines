package com.google.appengine.tools.pipeline;

import com.google.appengine.tools.pipeline.impl.model.JobRecord;
import com.google.cloud.datastore.KeyFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.junit.jupiter.api.Assertions.*;

class JobRunIdTest {

  @ParameterizedTest
  @CsvSource({
    "project, databaseId,namespace, jobId",
    "project,,,jobId",
    "project,,namespace,jobId",
    "project,databaseId,,jobId"
  })
  void testEquals(String project, String databaseId, String namespace, String jobId) {
    JobRunId jobRunId = JobRunId.of(project, databaseId, namespace, jobId);

    //round trip to string and back
    assertEquals(jobRunId, JobRunId.fromEncodedString(jobRunId.asEncodedString()));

    //round trip to key and back
    assertEquals(jobRunId, JobRunId.of(JobRecord.keyFromPipelineHandle(jobRunId)));
  }

  @Test
  void testEqualsNulls() {
    JobRunId jobRunId = JobRunId.of("project", null, null, "jobId");

    //round trip to string and back
    assertEquals(jobRunId, JobRunId.fromEncodedString(jobRunId.asEncodedString()));

    //round trip to key and back
    assertEquals(jobRunId, JobRunId.of(JobRecord.keyFromPipelineHandle(jobRunId)));
  }
}