// Copyright 2011 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce.impl.handlers;

import com.google.appengine.tools.mapreduce.Counter;
import com.google.appengine.tools.mapreduce.Counters;
import com.google.appengine.tools.mapreduce.impl.CountersImpl;
import com.google.appengine.tools.mapreduce.impl.IncrementalTaskContext;
import com.google.appengine.tools.mapreduce.impl.IncrementalTaskWithContext;
import com.google.appengine.tools.mapreduce.impl.shardedjob.IncrementalTask;
import com.google.appengine.tools.mapreduce.impl.shardedjob.IncrementalTaskState;
import com.google.appengine.tools.mapreduce.impl.shardedjob.ShardedJobRunId;
import com.google.appengine.tools.mapreduce.impl.shardedjob.ShardedJobState;
import com.google.appengine.tools.mapreduce.impl.util.RequestUtils;
import com.google.appengine.tools.pipeline.PipelineOrchestrator;
import com.google.appengine.tools.pipeline.PipelineRunner;
import com.google.appengine.tools.pipeline.di.JobRunServiceComponent;
import com.google.appengine.tools.pipeline.di.StepExecutionComponent;
import com.google.appengine.tools.pipeline.di.StepExecutionModule;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.primitives.Longs;

import com.googlecode.charts4j.AxisLabelsFactory;
import com.googlecode.charts4j.BarChart;
import com.googlecode.charts4j.Data;
import com.googlecode.charts4j.DataUtil;
import com.googlecode.charts4j.GCharts;
import com.googlecode.charts4j.Plot;
import com.googlecode.charts4j.Plots;

import lombok.AllArgsConstructor;
import lombok.extern.java.Log;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import javax.inject.Inject;

/**
 * UI Status view logic handler.
 *
 */
@Log
@AllArgsConstructor(onConstructor_ = @Inject)
final class StatusHandler {

  public static final int DEFAULT_JOBS_PER_PAGE_COUNT = 50;

  final JobRunServiceComponent component;
  final RequestUtils requestUtils;


  // Command paths
  public static final String LIST_JOBS_PATH = "list_jobs";
  public static final String CLEANUP_JOB_PATH = "cleanup_job";
  public static final String ABORT_JOB_PATH = "abort_job";
  public static final String GET_JOB_DETAIL_PATH = "get_job_detail";

  private JSONObject handleCleanupJob(PipelineOrchestrator pipelineOrchestrator, ShardedJobRunId jobId) throws JSONException {
    JSONObject retValue = new JSONObject();
    if (pipelineOrchestrator.cleanupJob(jobId)) {
      retValue.put("status", "Successfully deleted requested job.");
    } else {
      retValue.put("status", "Can't delete an active job");
    }
    return retValue;
  }

  private JSONObject handleAbortJob(PipelineOrchestrator pipelineOrchestrator, ShardedJobRunId jobId) throws JSONException {
    JSONObject retValue = new JSONObject();
    pipelineOrchestrator.abortJob(jobId);
    retValue.put("status", "Successfully aborted requested job.");
    return retValue;
  }

  /**
   * Handles all status page commands.
   */
  void handleCommand(
      String command, HttpServletRequest request, HttpServletResponse response) {
    response.setContentType("application/json");
    boolean isPost = "POST".equals(request.getMethod());
    JSONObject retValue = null;

    StepExecutionComponent stepExecutionComponent =
      component.stepExecutionComponent(new StepExecutionModule(request));
    PipelineOrchestrator pipelineOrchestrator = stepExecutionComponent.pipelineOrchestrator();
    PipelineRunner pipelineRunner = stepExecutionComponent.pipelineRunner();

    try {
      if (command.equals(LIST_JOBS_PATH) && !isPost) {
        retValue = handleListJobs(request);
      } else if (command.equals(CLEANUP_JOB_PATH) && isPost) {
        retValue = handleCleanupJob(pipelineOrchestrator, requestUtils.getMapReduceId(request));
      } else if (command.equals(ABORT_JOB_PATH) && isPost) {
        retValue = handleAbortJob(pipelineOrchestrator, requestUtils.getMapReduceId(request));
      } else if (command.equals(GET_JOB_DETAIL_PATH) && !isPost) {
        retValue = handleGetJobDetail(pipelineRunner, requestUtils.getMapReduceId(request));
        if (retValue == null) {
          log.warning("No job found for : " + requestUtils.getMapReduceId(request));
          response.sendError(HttpServletResponse.SC_NOT_FOUND, "No job found for : " + requestUtils.getMapReduceId(request));
        }
      }
    } catch (Exception t) {
      log.log(Level.SEVERE, "Got exception while running command", t);
      try {
        retValue = new JSONObject();
        retValue.put("error_class", t.getClass().getName());
        retValue.put("error_message",
            "Full stack trace is available in the server logs. Message: "
                + t.getMessage());
      } catch (JSONException e) {
        throw new RuntimeException("Couldn't create error JSON object", e);
      }
    }
    try {
      if (retValue == null) {
        response.sendError(HttpServletResponse.SC_NOT_FOUND);
      } else {
        retValue.write(response.getWriter());
        response.getWriter().flush();
      }
    } catch (JSONException | IOException e) {
      throw new RuntimeException("Couldn't write command response", e);
    }
  }

  private static JSONObject toJson(Counters counters) throws JSONException {
    JSONObject retValue = new JSONObject();
    for (Counter counter : counters.getCounters()) {
      retValue.put(counter.getName(), counter.getValue());
    }
    return retValue;
  }

  private static int getChartWidth(int shardCount) {
    return Math.max(300, Math.min(700, 70 * (int) Math.sqrt(shardCount)));
  }

  private static String getChartUrl(long[] workerCallCounts) {
    // If max == 0, the numeric range will be from 0 to 0. This causes some
    // problems when scaling to the range, so add 1 to max, assuming that the
    // smallest value can be 0, and this ensures that the chart always shows,
    // at a minimum, a range from 0 to 1 - when all shards are just starting.
    long maxPlusOne = workerCallCounts.length == 0 ? 1 : Longs.max(workerCallCounts) + 1;

    List<String> countLabels = new ArrayList<>();
    for (int i = 0; i < workerCallCounts.length; i++) {
      countLabels.add(String.valueOf(i));
    }

    Data countData = DataUtil.scaleWithinRange(0, maxPlusOne, Longs.asList(workerCallCounts));

    // TODO(user): Rather than returning charts from both servers, let's just
    // do it on the client's end.
    Plot countPlot = Plots.newBarChartPlot(countData);
    BarChart countChart = GCharts.newBarChart(countPlot);
    countChart.addYAxisLabels(AxisLabelsFactory.newNumericRangeAxisLabels(0, maxPlusOne));
    countChart.addXAxisLabels(AxisLabelsFactory.newAxisLabels(countLabels));
    countChart.setSize(getChartWidth(workerCallCounts.length), 200);
    countChart.setBarWidth(BarChart.AUTO_RESIZE);
    countChart.setSpaceBetweenGroupsOfBars(1);
    return countChart.toURLString();
  }

  /**
   * Handle the get_job_detail AJAX command.
   */
  @VisibleForTesting
  JSONObject handleGetJobDetail(PipelineRunner pipelineRunner, ShardedJobRunId jobId) {
    ShardedJobState state = pipelineRunner.getJobState(jobId);
    if (state == null) {
      return null;
    }
    JSONObject jobObject = new JSONObject();
    try {
      jobObject.put("name", jobId.getJobId()); // For display
      jobObject.put("mapreduce_id", jobId.asEncodedString()); // This is the sharedJobId but it needs be be called
                                            // mapreduce_id for python compatibility.
      jobObject.put("start_timestamp_ms", state.getStartTime().toEpochMilli());

      if (state.getStatus().isActive()) {
        jobObject.put("active", true);
        jobObject.put("updated_timestamp_ms", System.currentTimeMillis());
      } else {
        jobObject.put("active", false);
        jobObject.put("result_status", String.valueOf(state.getStatus().getStatusCode()));
        jobObject.put("updated_timestamp_ms", state.getMostRecentUpdateTime().toEpochMilli());
      }
      jobObject.put("shards", state.getTotalTaskCount());
      jobObject.put("active_shards", state.getActiveTaskCount());

      JSONObject mapperParams = new JSONObject();
      mapperParams.put("Shards completed",
          state.getTotalTaskCount() - state.getActiveTaskCount());
      mapperParams.put("Shards active", state.getActiveTaskCount());
      mapperParams.put("Shards total", state.getTotalTaskCount());
      JSONObject mapperSpec = new JSONObject();
      mapperSpec.put("mapper_params", mapperParams);
      jobObject.put("mapper_spec", mapperSpec);

      JSONArray shardArray = new JSONArray();
      Counters totalCounters = new CountersImpl();
      int i = 0;
      long[] workerCallCounts = new long[state.getTotalTaskCount()];
      List<IncrementalTaskState<IncrementalTask>> tasks = pipelineRunner.lookupTasks(state);

      for (IncrementalTaskState<?> taskState : tasks) {
        JSONObject shardObject = new JSONObject();
        shardObject.put("shard_number", i);
        shardObject.put("shard_description", taskState.getTaskId());
        shardObject.put("updated_timestamp_ms", taskState.getMostRecentUpdateTime().toEpochMilli());

        if (taskState.getStatus().isActive()) {
          shardObject.put("active", true);
        } else {
          shardObject.put("active", false);
          shardObject.put("result_status", taskState.getStatus().getStatusCode());
        }
        IncrementalTask task = taskState.getTask();
        if (task instanceof IncrementalTaskWithContext) {
          IncrementalTaskContext context = ((IncrementalTaskWithContext) task).getContext();
          totalCounters.addAll(context.getCounters());
          workerCallCounts[i] = context.getWorkerCallCount();
          shardObject.put("last_work_item", context.getLastWorkItemString());
        }
        shardArray.put(shardObject);
        i++;
      }

      jobObject.put("counters", toJson(totalCounters));
      jobObject.put("shards", shardArray);
      jobObject.put("chart_width", getChartWidth(state.getTotalTaskCount()));
      jobObject.put("chart_url", getChartUrl(workerCallCounts));
      jobObject.put("chart_data", workerCallCounts);
    } catch (JSONException e) {
      throw new RuntimeException("Hard coded string is null", e);
    }

    return jobObject;
  }

  private static JSONObject handleListJobs(HttpServletRequest request) {
    String cursor = request.getParameter("cursor");
    String countString = request.getParameter("count");
    int count = DEFAULT_JOBS_PER_PAGE_COUNT;
    if (countString != null) {
      count = Integer.parseInt(countString);
    }

    return handleListJobs(cursor, count);
  }

  /**
   * Handle the list_jobs AJAX command.
   */
  @SuppressWarnings("unused")
  private static JSONObject handleListJobs(String cursor, int count) {
    throw new RuntimeException("Job listing is not implemented yet.");
  }
}
