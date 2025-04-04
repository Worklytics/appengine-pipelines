package com.google.appengine.tools.pipeline.util;

import java.util.Optional;

/**
 * encapsulates a config property that can be provided by system property or environment variable.
 *
 * usage
 * <pre>
   enum ConfigProperty implements ConfigProperty {
     GAE_SERVICE_HOST_SUFFIX,
     ;
   }
 * </pre>
 */
public interface ConfigProperty {

  String getPropertyName();

  default Optional<String> getValue() {
    return Optional.ofNullable(System.getProperty(getPropertyName(), System.getenv(getPropertyName())));
  }

}
