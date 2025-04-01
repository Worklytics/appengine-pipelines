package com.google.appengine.tools.pipeline.impl.backend;

import com.google.appengine.tools.pipeline.impl.util.StringUtils;
import it.unimi.dsi.fastutil.objects.ObjectAVLTreeSet;
import lombok.extern.java.Log;

import java.util.Objects;

/**
 * "gen2" java runtimes, eg 11 (deprecated), 17, or 21 as of March 2025
 *
 * @see 'https://cloud.google.com/appengine/docs/standard/java-gen2/runtime'
 */
@Log
public class AppEngineStandardGen2 implements AppEngineEnvironment {

  static final String GOOGLE_CLOUD_PROJECT = "GOOGLE_CLOUD_PROJECT";

  static final String GAE_SERVICE = "GAE_SERVICE";

  //yes, they just call this 'GAE_VERSION', but it's the version-label of your service, not GAE - so I think this is less confusing
  static final String GAE_SERVICE_VERSION = "GAE_VERSION";

  //NOTE: is this ALWAYS the same as the applicationId???
  @Override
  public String getProjectId() {
    // this approach allows us to override at the JVM level in test scenarios, failing back to env vars (which we can't mutate from java process)
    return System.getProperty(GOOGLE_CLOUD_PROJECT, System.getenv(GOOGLE_CLOUD_PROJECT));
  }

  @Override
  public String getService() {
    String service = System.getProperty("DEFAULT_PIPELINES_SERVICE", System.getenv("DEFAULT_PIPELINES_SERVICE"));
    if (service == null || service.trim().isEmpty()) {
      return System.getProperty(GAE_SERVICE, System.getenv(GAE_SERVICE));
    } else {
      return service;
    }
  }

  @Override
  public String getVersion() {
    return System.getProperty(GAE_SERVICE_VERSION,System.getenv(GAE_SERVICE_VERSION));
  }
}
