// Copyright 2014 Google Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.google.appengine.tools.mapreduce.servlets;

import com.google.appengine.tools.mapreduce.GcpCredentialOptions;
import com.google.auth.oauth2.ServiceAccountCredentials;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.NonNull;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.Serial;
import java.io.Serializable;
import java.util.Base64;
import java.util.Optional;

@AllArgsConstructor
@NoArgsConstructor
@Data
public class ShufflerParams implements Serializable, GcpCredentialOptions {

  @Serial
  private static final long serialVersionUID = 3L;

  private String shufflerQueue;
  @NonNull
  private String gcsBucket;
  private String namespace;
  private String[] inputFileNames;
  private String outputDir;
  private String serviceAccountKey;

  /**
   * if provided, this value will be used as the manifest file name instead of generated manifest file name from job id
   * NOTE: will cause conflicts/overwrites if multiple jobs are writing to the same bucket/directory, with same manifestFileNameOverride value
   *
   * use case for this is testing, where we want to inspect manifest file contents
   * @see {@link ShufflerServletTest}
   */
  private String manifestFileNameOverride;

  private int outputShards;
  private String callbackQueue;
  private String callbackService;
  private String callbackVersion;
  private String callbackPath;

  /**
   * @return the callbackPath
   */
  public String getCallbackPath() {
    if (outputDir == null) {
      return "";
    }
    return callbackPath;
  }

}
