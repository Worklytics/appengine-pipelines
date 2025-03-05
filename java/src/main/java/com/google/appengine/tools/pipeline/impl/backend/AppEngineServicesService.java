package com.google.appengine.tools.pipeline.impl.backend;

public interface AppEngineServicesService {

  /**
   * TODO: look at this; I think it's more correctly the *current* service, which is what pipelines default to using if none is specified
   * so I think this method should be renamed to getCurrentService (or removed in favor of AppEngineEnvironment.getService)
   *
   * @return the default service for the GAE app (project)
   */
  String getDefaultService();

  /**
   * @param service the service name
   * @return default version of the service (the one with most traffic allocated)
   */
  String getDefaultVersion(String service);

  String getWorkerServiceHostName(String service, String version);
}
