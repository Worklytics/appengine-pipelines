package com.google.appengine.tools.pipeline.impl.util;

import com.google.common.collect.ImmutableMap;
import org.junit.Test;

import java.util.Optional;

import static org.junit.Assert.*;

public class JsonUtilsTest {


  public static class Jdk8Bean {

    public Optional<String> getFull() {
      return Optional.of("string");
    }

    public Optional<String> getEmpty() {
      return Optional.ofNullable(null);
    }

  }

  @Test
  public void jdk8() {

    String JSON_WITH_UNWRAPPED_OPTIONALS = "{\n" +
      "  \"bean\" : {\n" +
      "    \"empty\" : null,\n" +
      "    \"full\" : \"string\"\n" +
      "  }\n" +
      "}";

    String json = JsonUtils.mapToJson(ImmutableMap.of("bean", new Jdk8Bean()));

    assertEquals(JSON_WITH_UNWRAPPED_OPTIONALS, json);
  }

}
