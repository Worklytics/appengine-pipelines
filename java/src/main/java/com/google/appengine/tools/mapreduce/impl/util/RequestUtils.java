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

import lombok.*;
import lombok.extern.java.Log;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.Optional;

/**
 * handles translation to/from request parameters and pipeline backends
 *
 * q: good pattern? kinda franken factory to build instances from requests ... but w/o this, gets repeated in a bunch
 *  of places
 */
@Log
@Singleton
@NoArgsConstructor(onConstructor_ = @Inject)
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

  private static final String TRACEPARENT_HEADER = "traceparent";
  private static final String CLOUD_TRACE_CONTEXT_HEADER = "X-Cloud-Trace-Context";


  // value of env var GOOGLE_CLOUD_PROJECT when running locally; underscores aren't actually legal in GCP project ids,
  // so if this ever ends up being used in a real GCP API call, it blows up in validation before request is even sent by client
  private static final String LOCAL_GAE_PROJECT_ID = "no_app_id";

  private static final String DEFAULT_OVERRIDE_LOCAL_GAE_PROJECT_ID = "local-gae-project";
  /**
   * value to override local GAE project id with, when running locally; to allow this on case-by-case basis
   */
  @Getter @Setter
  private String localProjectIdOverride = DEFAULT_OVERRIDE_LOCAL_GAE_PROJECT_ID;

  public PipelineBackEnd buildBackendFromRequest(HttpServletRequest request) {
    return new AppEngineBackEnd(buildDatastoreFromRequest(request), new AppEngineTaskQueue());
  }

  @Deprecated // use pipeline backend
  Datastore buildDatastoreFromRequest(HttpServletRequest request) {
    // so we need 1) host, 2) projectId, and 3) databaseId from somewhere

    // options
    // - pass as parameters on request
    // - set as env vars (system properties), via Maven to pull (wouldn't exactly let us do integration tests)
    //    --> no, host may include port, set at runtime by emulator; not easy/appropriate to fake as env var

    DatastoreOptions defaultInstance = DatastoreOptions.getDefaultInstance();

    DatastoreOptions.Builder builder = defaultInstance.toBuilder();

    if (LOCAL_GAE_PROJECT_ID.equals(defaultInstance.getProjectId())) {
      log.info("pipelines fw detected running locally with GAE projectId set as 'no_app_id'; this isn't legal GCP project id, so changing to 'local-gae-project'");
      // 'no_app_id' isn't legal name, so change it
      builder.setProjectId(getLocalProjectIdOverride());
      // try to get emulator host from env var, if available
      builder.setHost(System.getProperty("DATASTORE_EMULATOR_HOST", System.getenv("DATASTORE_EMULATOR_HOST")));
    }

    // whatever values are, they can be overridden by request params
    getParam(request, Params.DATASTORE_HOST).ifPresent(builder::setHost);
    getParam(request, Params.DATASTORE_PROJECT_ID).ifPresent(builder::setProjectId);
    getParam(request, Params.DATASTORE_DATABASE_ID).ifPresent(builder::setDatabaseId);
    getParam(request, Params.DATASTORE_NAMESPACE).ifPresent(builder::setNamespace);

    return builder.build().getService();
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


  public String getRequestId(HttpServletRequest request) {
    try {
      // see: https://cloud.google.com/trace/docs/trace-context
      Optional<String> traceParentHeader = Optional.ofNullable(request.getHeader(TRACEPARENT_HEADER));
      if (traceParentHeader.isPresent()) {
        return TraceParent.of(traceParentHeader.get()).getTraceId();
      }
    } catch (RuntimeException e) {
      log.warning("Error parsing traceparent header: " + e.getMessage());
    }

    try {
      Optional<String> cloudTraceContextHeader = Optional.ofNullable(request.getHeader(CLOUD_TRACE_CONTEXT_HEADER));
      if (cloudTraceContextHeader.isPresent()) {
        return cloudTraceContextHeader.get().split("/")[0];
      }
    } catch (RuntimeException e) {
      log.warning("Error parsing X-Cloud-Trace-Context header: " + e.getMessage());
    }

    return "unknown-trace-id"; // Fallback if the header is missing
  }

  @Value
  public static class TraceParent {

    String version;
    String traceId;
    String parentId;
    String traceFlags;

    private TraceParent(String traceparentHeader) {
      String[] parts = traceparentHeader.split("-");
      if (parts.length != 4) {
        throw new IllegalArgumentException("Invalid traceparent format");
      }

      this.version = parts[0];
      this.traceId = parts[1];
      this.parentId = parts[2];
      this.traceFlags = parts[3];
    }

    public static TraceParent of(String traceparentHeader) {
      return new TraceParent(traceparentHeader);
    }
  }
}
