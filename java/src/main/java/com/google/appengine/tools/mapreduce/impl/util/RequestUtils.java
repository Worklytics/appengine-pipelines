package com.google.appengine.tools.mapreduce.impl.util;

import com.google.appengine.tools.pipeline.impl.backend.AppEngineBackEnd;
import com.google.appengine.tools.pipeline.impl.backend.AppEngineTaskQueue;
import com.google.appengine.tools.pipeline.impl.backend.PipelineBackEnd;
import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.DatastoreOptions;

import javax.servlet.http.HttpServletRequest;
import lombok.AllArgsConstructor;

import javax.inject.Inject;
import javax.inject.Singleton;
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

  Optional<String> getParam(HttpServletRequest request, String name) {
    return Optional.ofNullable(request.getParameter(name));
  }
}
