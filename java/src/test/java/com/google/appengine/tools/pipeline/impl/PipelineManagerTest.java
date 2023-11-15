package com.google.appengine.tools.pipeline.impl;

import com.google.appengine.tools.pipeline.*;
import com.google.appengine.tools.pipeline.impl.backend.AppEngineBackEnd;
import com.google.appengine.tools.pipeline.impl.model.JobRecord;
import com.google.cloud.datastore.Key;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;

class PipelineManagerTest extends PipelineTest {

  static class NoopJob extends Job1<String, String> {

    @Override
    public Value<String> run(String value) throws Exception {
      return immediate(value);
    }
  }

  @Test
  void restore(AppEngineBackEnd appEngineBackend,
               PipelineManager pipelineManager) {

    JobSetting[] settings = new JobSetting[0];
    Job<String> jobInstance = new NoopJob();
    JobRecord jobRecord = JobRecord.createRootJobRecord("test-project", jobInstance, appEngineBackend.getSerializationStrategy(), settings);

    Job<?> restored = pipelineManager.restore(jobRecord);

    //valid state of Job restored with all values we expect
    assertNotNull(restored);
    assertNotNull(restored.getPipelineRunner());
    assertNotNull(restored.getPipelineKey());
    assertNotNull(restored.getJobKey());
    assertNotNull(restored.getCurrentRunGUID());
    assertNotNull(restored.getThisJobRecord());
    assertNotNull(restored.getUpdateSpec());
  }

  @SneakyThrows
  @Test
  void startNewPipeline(PipelineManager pipelineManager) {
    JobSetting[] settings = new JobSetting[0];
    Job<String> jobInstance = new NoopJob();
    String pipelineId = pipelineManager.startNewPipeline(settings, jobInstance);

    //returned a pipeline id, and it is a url-safe datastore key
    assertNotNull(pipelineId);
    Key key = Key.fromUrlSafe(pipelineId);
    assertNotNull(key);
    JobRecord jobRecord = pipelineManager.getJob(pipelineId);
    assertNotNull(jobRecord);
  }

  @SneakyThrows
  @Test
  void deletePipelineRecords(PipelineManager pipelineManager) {
    JobSetting[] settings = new JobSetting[0];
    Job<String> jobInstance = new NoopJob();
    String pipelineId = pipelineManager.startNewPipeline(settings, jobInstance);

    pipelineManager.deletePipelineRecords(pipelineId, true, false);

    try {
      pipelineManager.getJob(pipelineId);
      fail("expected NoSuchObjectException, as job records should have been deleted");
    } catch (NoSuchObjectException e) {
      //expected
    }
  }
}