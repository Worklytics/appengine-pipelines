// Copyright 2011 Google Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy of
// the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations under
// the License.

package com.google.appengine.tools.pipeline.impl.util;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import java.io.IOException;
import java.util.Map;

/**
 * @author rudominer@google.com (Mitch Rudominer)
 *
 */
public class JsonUtils {

  //q: expose this so its behavior can be configured??
  static ObjectMapper objectToJsonMapper;

  //initialize ObjectMapper
  static  {
    objectToJsonMapper = new ObjectMapper();
    objectToJsonMapper.registerModule(new Jdk8Module());  //java.util.Optional, Streams
    objectToJsonMapper.registerModule(new JavaTimeModule());

    objectToJsonMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    objectToJsonMapper.disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);
    //set explicit view for object mapper, so anything annotated with an explicit @JsonView won't be shown by default
    objectToJsonMapper.setConfig(objectToJsonMapper.getSerializationConfig().withView(Object.class));
  }

  public static String mapToJson(Map<?, ?> map) {
    try {
      return objectToJsonMapper
        .setSerializationInclusion(JsonInclude.Include.NON_NULL)
        .writer()
        .with(SerializationFeature.INDENT_OUTPUT)
        .writeValueAsString(map);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  // All code below is only for manually testing this class.

  /**
   * Convert a JSON representation into an object
   */
  public static Map<String, ?> fromJson(String json) {
    try {
      return objectToJsonMapper.readValue(json, Map.class);
    } catch (IOException e) {
      throw new RuntimeException("json=" + json, e);
    }
  }
}
