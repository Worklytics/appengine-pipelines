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

import java.util.Map;

/**
 * @author rudominer@google.com (Mitch Rudominer)
 *
 */
public class JsonUtils {

  static ObjectMapper objectToJsonMapper = new ObjectMapper();

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
  public static Object fromJson(String json) {
    try {
      return objectToJsonMapper.reader().readValue(json);
    } catch (JsonProcessingException e) {
      throw new RuntimeException("json=" + json, e);
    }
  }
}
