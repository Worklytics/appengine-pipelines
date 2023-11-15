package com.google.appengine.tools.pipeline;

import com.google.appengine.tools.pipeline.impl.backend.AppEngineEnvironment;
import com.google.appengine.tools.pipeline.impl.backend.AppEngineStandardGen1;
import com.google.appengine.tools.pipeline.impl.model.JobRecord;
import org.junit.jupiter.api.extension.*;

public class AppEngineEnvironmentExtension implements BeforeAllCallback {


  @Override
  public void beforeAll(ExtensionContext extensionContext) throws Exception {
    // this should be legit in prod, if we do the legacy support stuff
    JobRecord.environment = new AppEngineStandardGen1();

//      new AppEngineEnvironment() {
//      @Override
//      public String getService() {
//        return "test-service";
//      }
//
//      @Override
//      public String getVersion() {
//        return "v1";
//      }
//    };
  }
}
