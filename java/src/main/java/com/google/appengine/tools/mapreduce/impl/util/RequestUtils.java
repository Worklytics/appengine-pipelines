package com.google.appengine.tools.mapreduce.impl.util;

import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.DatastoreOptions;

import jakarta.servlet.http.HttpServletRequest;

public class RequestUtils {


  public static final String PARAM_NAMESPACE = "ns";

  public Datastore buildDatastoreFromRequest(HttpServletRequest request) {
    // pull projectId, any dat
    DatastoreOptions.Builder builder = DatastoreOptions.getDefaultInstance().toBuilder();

    String namespace = request.getParameter(PARAM_NAMESPACE);
    if (namespace != null) {
      builder.setNamespace(namespace);
    }
    return builder.build().getService();
  }
}
