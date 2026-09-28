package com.google.appengine.tools.pipeline.impl.model;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import com.google.cloud.datastore.Key;

class PipelineModelObjectTest {

  @Test
  void generateKey() {

    Key key = PipelineObjectKey.builder()
        .projectId("project")
        .namespace("ns")
        .kind("Kind")
        .name(PipelineObjectKey.newName())
        .build()
        .toDatastoreKey();

    assertEquals("project", key.getProjectId());
    assertEquals("ns", key.getNamespace());
    assertEquals("Kind", key.getKind());
    assertTrue(key.getDatabaseId() == null || key.getDatabaseId().isEmpty());

    // validate key.getName() is legal for GCP cloud datastore
    assertTrue(key.getName().matches("^[a-zA-Z0-9\\-_.~]+$"));
  }
}