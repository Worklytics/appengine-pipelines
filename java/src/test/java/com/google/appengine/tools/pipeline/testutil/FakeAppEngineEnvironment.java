package com.google.appengine.tools.pipeline.testutil;

import com.google.appengine.tools.pipeline.impl.backend.AppEngineEnvironment;

import lombok.Builder;
import lombok.Value;

@Builder
@Value
public class FakeAppEngineEnvironment implements AppEngineEnvironment {
  String projectId;
  String service;
  String version;
}
