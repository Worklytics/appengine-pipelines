package com.google.appengine.tools.pipeline.impl.util;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonView;
import com.google.common.collect.ImmutableMap;
import lombok.Getter;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

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

  @Getter
  public static class SecretBean {

    private String notASecret = "public";

    @JsonView(Secret.class)
    private String secret = "something secret";

  }

  public static class Secret {

  }

  @Test
  public void secret() {

    String JSON_WITH_SECRET = "{\n" +
      "  \"bean\" : {\n" +
      "    \"notASecret\" : \"public\"\n" +
      "  }\n" +
      "}";

    String json = JsonUtils.mapToJson(ImmutableMap.of("bean", new SecretBean()));

    assertEquals(JSON_WITH_SECRET, json);
  }

}
