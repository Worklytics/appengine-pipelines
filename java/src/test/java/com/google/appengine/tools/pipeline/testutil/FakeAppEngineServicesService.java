package com.google.appengine.tools.pipeline.testutil;

import com.google.appengine.tools.pipeline.impl.backend.AppEngineServicesService;
import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class FakeAppEngineServicesService implements AppEngineServicesService {

  @Builder.Default
  String project = "fake-project";

  @Builder.Default
  String defaultService = "default";

  @Builder.Default
  String version = "v1";

  @Builder.Default
  String domain = "fakeappspot.fake";


  @Builder.Default
  String location = "us-central1";

  @Override
  public String getDefaultVersion(String service) {
    return version;
  }

  @Override
  public String getWorkerServiceHostName(String service, String version) {
    return version + "-dot-" + service + "-dot-" + project + "." + domain;
  }
}
