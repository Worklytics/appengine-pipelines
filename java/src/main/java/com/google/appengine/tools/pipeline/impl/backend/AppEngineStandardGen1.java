package com.google.appengine.tools.pipeline.impl.backend;

import com.google.appengine.api.modules.ModulesService;
import com.google.appengine.api.modules.ModulesServiceFactory;

/**
 * see https://cloud.google.com/appengine/docs/standard/services/modules?tab=java#top
 *
 * possible to use this in gen2, via following mechanism:
 * https://cloud.google.com/appengine/docs/standard/java-gen2/services/access
 *
 */
public class AppEngineStandardGen1 implements AppEngineEnvironment {


  ModulesService modulesService = ModulesServiceFactory.getModulesService();
  @Override
  public String getService() {
    return modulesService.getCurrentModule();
  }

  @Override
  public String getVersion() {
    return modulesService.getCurrentVersion();
  }
}
