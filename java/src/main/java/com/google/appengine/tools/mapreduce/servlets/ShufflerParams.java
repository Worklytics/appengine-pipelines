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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.Base64;
import java.util.Optional;

@AllArgsConstructor
@NoArgsConstructor
@Data
class ShufflerParams implements Serializable, GcpCredentialOptions {

  private static final long serialVersionUID = 2L;

  private String shufflerQueue;
  private String gcsBucket;
  private String namespace;
  private String[] inputFileNames;
  private String outputDir;
  private String serviceAccountKey;

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
