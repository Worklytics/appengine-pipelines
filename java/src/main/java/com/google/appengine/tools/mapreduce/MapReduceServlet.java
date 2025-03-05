/*
 * Copyright 2010 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.appengine.tools.mapreduce;

import com.google.appengine.tools.mapreduce.impl.shardedjob.RejectRequestException;
import com.google.appengine.tools.pipeline.di.DaggerJobRunServiceComponent;
import com.google.appengine.tools.pipeline.di.JobRunServiceComponent;

import java.io.IOException;
import java.io.Serial;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.common.annotations.VisibleForTesting;
import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import lombok.Setter;
import lombok.extern.java.Log;

import javax.inject.Inject;

/**
 * Servlet for all MapReduce API related functions.
 *
 * This should be specified as the handler for MapReduce URLs in web.xml.
 * For instance:
 * <pre>
 * {@code
 * <servlet>
 *   <servlet-name>mapreduce</servlet-name>
 *   <servlet-class>com.google.appengine.tools.mapreduce.MapReduceServlet</servlet-class>
 * </servlet>
 * <servlet-mapping>
 *   <servlet-name>mapreduce</servlet-name>
 *   <url-pattern>/mapreduce/*</url-pattern>
 * </servlet-mapping>
 * }
 *
 * Generally you'll want this handler to be protected by an admin security constraint
 * (see <a
 * href="http://cloud.google.com/appengine/docs/java/config/webxml.html#Security_and_Authentication">
 * Security and Authentication</a>)
 * for more details.
 * </pre>
 */
@Log
public class MapReduceServlet extends HttpServlet {

  @Serial
  private static final long serialVersionUID = 1L;

  private static final int REJECT_REQUEST_STATUSCODE = 429; // See rfc6585

  @Setter(onMethod_ = @VisibleForTesting)
  JobRunServiceComponent component;

  @Override
  public void init() throws ServletException {
    super.init();
    component = DaggerJobRunServiceComponent.create();
  }

  @Override
  public void doPost(HttpServletRequest req, HttpServletResponse resp) throws IOException {

    try {
      component.mapReduceServletImpl().doPost(req, resp);
    } catch (RejectRequestException e) {
      handleRejectedRequest(resp, e);
    }
  }

  @Override
  public void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
    try {
      component.mapReduceServletImpl().doGet(req, resp);
    } catch (RejectRequestException e) {
      handleRejectedRequest(resp, e);
    }
  }

  private static void handleRejectedRequest(HttpServletResponse resp, RejectRequestException e) {
    resp.addIntHeader("Retry-After", 0);
    resp.setStatus(REJECT_REQUEST_STATUSCODE);
    log.log(Level.INFO, "Rejecting request: " + e.getLocalizedMessage());
  }
}
