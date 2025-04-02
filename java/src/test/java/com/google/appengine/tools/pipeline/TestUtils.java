package com.google.appengine.tools.pipeline;

import com.google.appengine.api.taskqueue.dev.LocalTaskQueue;
import com.google.appengine.api.taskqueue.dev.QueueStateInfo;
import com.google.appengine.tools.mapreduce.impl.util.RequestUtils;
import com.google.appengine.tools.pipeline.impl.model.*;
import com.google.cloud.datastore.*;
import com.google.common.collect.Lists;
import javax.servlet.http.HttpServletRequest;
import lombok.extern.java.Log;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@Log
public class TestUtils {

  public static void waitUntilTaskQueueIsEmpty(LocalTaskQueue taskQueue) throws InterruptedException {

    boolean hasMoreTasks = true;
    while (hasMoreTasks) {
      Map<String, QueueStateInfo> taskInfoMap = taskQueue.getQueueStateInfo();
      hasMoreTasks = false;
      for (QueueStateInfo taskQueueInfo : taskInfoMap.values()) {
        if (taskQueueInfo.getCountTasks() > 0) {
          hasMoreTasks = true;
          break;
        }
      }
      if (hasMoreTasks) {
        Thread.sleep(100);
      }
    }
  }


  public static JobInfo waitUntilJobComplete(PipelineService pipelineService, JobRunId pipelineId) throws Exception {
    while (true) {
      Thread.sleep(2000);
      JobInfo jobInfo = pipelineService.getJobInfo(pipelineId);
      switch (jobInfo.getJobState()) {
        case RUNNING:
        case WAITING_TO_RETRY:
          break;
        default:
          return jobInfo;
      }
    }
  }

  @SuppressWarnings("unchecked")
  public static <T> T waitForJobToComplete(PipelineService pipelineService, JobRunId pipelineId) throws Exception {
    JobInfo jobInfo = waitUntilJobComplete(pipelineService, pipelineId);
    switch (jobInfo.getJobState()) {
      case COMPLETED_SUCCESSFULLY:
        return (T) jobInfo.getOutput();
      case STOPPED_BY_ERROR:
        throw new RuntimeException("Job stopped " + jobInfo.getError());
      case STOPPED_BY_REQUEST:
        throw new RuntimeException("Job stopped by request.");
      case CANCELED_BY_REQUEST:
        throw new RuntimeException("Job was canceled by request.");
      default:
        throw new RuntimeException("Unexpected job state: " + jobInfo.getJobState());
    }
  }

  /**
   * assert actual string matches expected, ignoring whitespace
   * @param expected
   * @param actual
   */
  public static void assertEqualsIgnoreWhitespace(String expected, String actual) {
    assertEquals(stripWhitespace(expected), stripWhitespace(actual));
  }

  private static String stripWhitespace(String s) {
    return s.replaceAll("\\s+","");
  }

  /**
   * add datastore headers to request mocked with Mockito
   *
   * @param request must be a Mockito mock
   * @param datastoreOptions to add as headers
   */
  public static void addDatastoreHeadersToRequest(HttpServletRequest request, DatastoreOptions datastoreOptions) {
    when(request.getParameter(eq(RequestUtils.Params.DATASTORE_HOST)))
      .thenReturn(datastoreOptions.getHost());
    when(request.getParameter(eq(RequestUtils.Params.DATASTORE_NAMESPACE)))
      .thenReturn(datastoreOptions.getNamespace());
    when(request.getParameter(eq(RequestUtils.Params.DATASTORE_PROJECT_ID)))
      .thenReturn(datastoreOptions.getProjectId());
    when(request.getParameter(eq(RequestUtils.Params.DATASTORE_DATABASE_ID))
    ).thenReturn(datastoreOptions.getDatabaseId());
  }


  /**
   * count datastore entities of kinds used by pipelines fw
   *
   * (original tests leveraged GAE Datastore SDK count() method that counted everything in datastore)
   *
   * @param datastore
   * @return
   */
  public static int countDatastoreEntities(Datastore datastore) {
    List<String> kinds = Arrays.asList(
      "MR-ShardedJob", //ShardedJobStateImpl.ShardedJobSerializer.ENTITY_KIND
      "MR-IncrementalTask", //IncrementalTaskState.Serializer.ENTITY_KIND
      "MR-IncrementalTask-ShardInfo", //IncrementalTaskState.Serializer.SHARD_INFO_ENTITY_KIND
      "MR-ShardRetryState", // ShardRetryState.Serializer.ENTITY_KIND

      //6 pipeline- kinds
      Barrier.DATA_STORE_KIND,
      JobRecord.DATA_STORE_KIND,
      JobInstanceRecord.DATA_STORE_KIND,
      ShardedValue.DATA_STORE_KIND, // as of 2024-06-25, none of these in staging
      Slot.DATA_STORE_KIND,
      ExceptionRecord.DATA_STORE_KIND
    );
    int count = 0;
    for (String kind : kinds) {
      Query<Entity> query = Query.newEntityQueryBuilder().setKind(kind).build();
      QueryResults<Entity> entities = datastore.run(query);
      int entityCount = Lists.newArrayList(entities).size();
      count += entityCount;
      if (entityCount > 0) {
        log.info("kind: " + kind + " count: " + entityCount);
      }
    }
    return count;
  }
}
