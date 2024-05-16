package com.google.appengine.tools.pipeline.impl.backend;

/**
 * "gen2" java runtimes, eg 11 or 17 as of Nov 2023
 *
 * @see 'https://cloud.google.com/appengine/docs/standard/java-gen2/runtime'
 */
public class AppEngineStandardGen2 implements AppEngineEnvironment {

  @Override
  public String getService() {
    return System.getProperty("GAE_SERVICE");
  }

  @Override
  public String getVersion() {
    return System.getProperty("GAE_VERSION");
  }
}
