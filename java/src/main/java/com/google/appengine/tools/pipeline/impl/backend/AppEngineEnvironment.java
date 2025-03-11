package com.google.appengine.tools.pipeline.impl.backend;


public interface AppEngineEnvironment {

  String getProjectId();

  String getLocation();

  String getService();

  String getVersion();
}
