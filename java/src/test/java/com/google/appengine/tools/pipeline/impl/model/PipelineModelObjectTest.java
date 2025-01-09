package com.google.appengine.tools.pipeline.impl.model;

import com.google.cloud.datastore.Key;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class PipelineModelObjectTest {

  @Test
  void generateKey() {

    Key key = PipelineModelObject.generateKey("project", "ns", "Kind");

    assertEquals("project", key.getProjectId());
    assertEquals("ns", key.getNamespace());
    assertEquals("Kind", key.getKind());

    //validate key.getName() is legal for GCP cloud datastore
    assertTrue(key.getName().matches("^[a-zA-Z0-9\\-_.~]+$"));
  }
}