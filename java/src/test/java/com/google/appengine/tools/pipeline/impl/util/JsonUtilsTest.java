package com.google.appengine.tools.pipeline.impl.util;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.google.common.collect.ImmutableMap;
import lombok.Getter;
import org.junit.Test;

import java.time.Instant;
import java.util.Optional;

import static org.junit.Assert.*;

public class JsonUtilsTest {


  @Getter
  @JsonPropertyOrder({"instant", "full", "empty"})  //make test deterministic
  public static class Jdk8Bean {

    private Instant instant = Instant.parse("2020-01-01T12:00:00Z");

    private Optional<String> full = Optional.of("string");

    private Optional<String> empty = Optional.ofNullable(null);

  }

  @Test
  public void jdk8() {

    String JSON_WITH_UNWRAPPED_OPTIONALS = "{\n" +
      "  \"bean\" : {\n" +
      "    \"instant\" : \"2020-01-01T12:00:00Z\",\n" +
      "    \"full\" : \"string\",\n" +
      "    \"empty\" : null\n" +
      "  }\n" +
      "}";

    String json = JsonUtils.mapToJson(ImmutableMap.of("bean", new Jdk8Bean()));

    assertEquals(JSON_WITH_UNWRAPPED_OPTIONALS, json);
  }

}
