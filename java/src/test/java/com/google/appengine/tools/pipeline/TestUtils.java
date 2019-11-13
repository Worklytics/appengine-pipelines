package com.google.appengine.tools.pipeline;

import com.google.appengine.api.taskqueue.dev.LocalTaskQueue;
import com.google.appengine.api.taskqueue.dev.QueueStateInfo;

import java.util.Map;

class TestUtils {

  static void waitUntilTaskQueueIsEmpty(LocalTaskQueue taskQueue) throws InterruptedException {
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


  static JobInfo waitUntilJobComplete(String pipelineId) throws Exception {
    PipelineService service = PipelineServiceFactory.newPipelineService();
    while (true) {
      Thread.sleep(2000);
      JobInfo jobInfo = service.getJobInfo(pipelineId);
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
  static <T> T waitForJobToComplete(String pipelineId) throws Exception {
    JobInfo jobInfo = waitUntilJobComplete(pipelineId);
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
}
