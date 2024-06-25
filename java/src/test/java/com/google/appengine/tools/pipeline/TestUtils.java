package com.google.appengine.tools.pipeline;

import com.google.appengine.api.taskqueue.dev.LocalTaskQueue;
import com.google.appengine.api.taskqueue.dev.QueueStateInfo;
import com.google.appengine.tools.mapreduce.impl.util.RequestUtils;
import com.google.cloud.datastore.DatastoreOptions;
import jakarta.servlet.http.HttpServletRequest;

import java.util.Map;

import static org.easymock.EasyMock.expect;
import static org.junit.jupiter.api.Assertions.assertEquals;

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


  public static JobInfo waitUntilJobComplete(PipelineService pipelineService, String pipelineId) throws Exception {
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
  public static <T> T waitForJobToComplete(PipelineService pipelineService, String pipelineId) throws Exception {
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

  public static void addDatastoreHeadersToRequest(HttpServletRequest request, DatastoreOptions datastoreOptions) {
    expect(request.getParameter(RequestUtils.Params.DATASTORE_HOST))
      .andReturn(datastoreOptions.getHost()).anyTimes();

    expect(request.getParameter(RequestUtils.Params.DATASTORE_NAMESPACE))
      .andReturn(datastoreOptions.getNamespace()).anyTimes();

    expect(request.getParameter(RequestUtils.Params.DATASTORE_PROJECT_ID))
      .andReturn(datastoreOptions.getProjectId()).anyTimes();

    expect(request.getParameter(RequestUtils.Params.DATASTORE_DATABASE_ID))
      .andReturn(datastoreOptions.getDatabaseId()).anyTimes();
  }
}
