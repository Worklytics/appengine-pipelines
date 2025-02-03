package com.google.appengine.tools.mapreduce.impl.util;

import com.google.appengine.tools.mapreduce.impl.shardedjob.ShardedJobRunId;
import com.google.appengine.tools.pipeline.JobRunId;
import com.google.appengine.tools.pipeline.impl.backend.AppEngineBackEnd;
import com.google.appengine.tools.pipeline.impl.backend.AppEngineTaskQueue;
import com.google.appengine.tools.pipeline.impl.backend.PipelineBackEnd;
import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.DatastoreOptions;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import lombok.AllArgsConstructor;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.net.URLEncoder;
import java.util.Optional;

/**
 * handles translation to/from request parameters and pipeline backends
 *
 * q: good pattern? kinda franken factory to build instances from requests ... but w/o this, gets repeated in a bunch
 *  of places
 */
@Singleton
@AllArgsConstructor(onConstructor_ = @Inject)
public class RequestUtils {



  public static class Params {
    public static final String DATASTORE_PROJECT_ID = "dsProjectId";
    public static final String DATASTORE_DATABASE_ID = "dsDatabaseId";
    public static final String DATASTORE_NAMESPACE = "dsNamespace";
    public static final String DATASTORE_HOST = "dsHost";

    //originally defined per-handler in the gae pipelines project
    public static final String ROOT_PIPELINE_ID = "root_pipeline_id";

    //hard-coded in many places, JS / etc
    public static final String MAPREDUCE_ID = "mapreduce_id";
  }

  public static class GCPHeaders {
    public static final String CLOUD_TASKS_EXECUTION_COUNT = "X-CloudTasks-ExecutionCount";
    public static final String CLOUD_TASKS_TASKNAME = "X-CloudTasks-TaskName";
  }

  public PipelineBackEnd buildBackendFromRequest(HttpServletRequest request) {
    return new AppEngineBackEnd(buildDatastoreFromRequest(request), new AppEngineTaskQueue());
  }


  @Deprecated // use pipeline backend
  public Datastore buildDatastoreFromRequest(HttpServletRequest request) {
    // so we need 1) host, 2) projectId, and 3) databaseId from somewhere

    // options
    // - pass as parameters on request
    // - set as env vars (system properties), via Maven to pull (wouldn't exactly let us do integration tests)
    //    --> no, host may include port, set at runtime by emulator; not easy/appropriate to fake as env var

    DatastoreOptions.Builder builder = DatastoreOptions.getDefaultInstance().toBuilder();
    getParam(request, Params.DATASTORE_HOST).ifPresent(builder::setHost);
    getParam(request, Params.DATASTORE_NAMESPACE).ifPresent(builder::setNamespace);
    getParam(request, Params.DATASTORE_PROJECT_ID).ifPresent(builder::setProjectId);
    getParam(request, Params.DATASTORE_DATABASE_ID).ifPresent(builder::setDatabaseId);

    return builder.build().getService();
  }

  public Optional<String> getHeader(HttpServletRequest request, String s) {
    return Optional.ofNullable(request.getHeader(s));
  }

  public Optional<String> getParam(HttpServletRequest request, String name) {
    return Optional.ofNullable(request.getParameter(name));
  }

  public Optional<String> getJobId(HttpServletRequest request, String paramName) {
    return getParam(request, paramName);
  }

  public JobRunId getRootPipelineId(HttpServletRequest request) throws ServletException {
    return getJobId(request, Params.ROOT_PIPELINE_ID).map(s -> JobRunId.fromEncodedString(s))
      .orElseThrow(() -> new ServletException(Params.ROOT_PIPELINE_ID + " parameter not found."));
  }

  /**
   * gets 'map_reduce_id', undo'ing serlvet fws decoding of URL-encoded param value
   * @param request
   * @return
   * @throws ServletException
   */
  public ShardedJobRunId getMapReduceId(HttpServletRequest request) throws ServletException {
    return getParam(request, Params.MAPREDUCE_ID).map(ShardedJobRunId::fromEncodedString)
      .orElseThrow(() -> new ServletException(Params.MAPREDUCE_ID + " parameter not found."));
  }
}
