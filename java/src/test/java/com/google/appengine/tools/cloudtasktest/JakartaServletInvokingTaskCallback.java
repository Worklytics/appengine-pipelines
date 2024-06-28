package com.google.appengine.tools.cloudtasktest;

import com.google.appengine.api.urlfetch.URLFetchServicePb;
import com.google.appengine.tools.development.testing.LocalTaskQueueTestConfig;
import com.google.common.collect.Maps;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletResponse;
import lombok.SneakyThrows;
import lombok.extern.java.Log;

import java.io.UnsupportedEncodingException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLDecoder;
import java.util.Map;
import java.util.logging.Level;

@Log
public abstract class JakartaServletInvokingTaskCallback extends LocalTaskQueueTestConfig.DeferredTaskCallback {

    /**
     * @return A mapping from url path to HttpServlet. Where url path is a string that looks like
     *         "/foo/bar" (It must start with a '/' and should not contain characters that are not
     *         allowed in the path portion of a url.)
     */
    protected abstract Map<String, ? extends HttpServlet> getServletMap();

    protected Map<String, String> getExtraParamValues() {
      return Map.of();
    }

    /**
     * @return A servlet that will be used if none of the ones from {@link #getServletMap()} match.
     */
    protected abstract HttpServlet getDefaultServlet();

    private static Map<String, String> extractParamValues(final String body) {
      Map<String, String> params = Maps.newHashMap();
      if (body.length() > 0) {
        for (String keyValue : body.split("&")) {
          String[] split = keyValue.split("=");
          try {
            params.put(split[0], URLDecoder.decode(split[1], "utf-8"));
          } catch (UnsupportedEncodingException e) {
            throw new RuntimeException("Could not decode param " + split[1]);
          }
        }
      }
      return params;
    }

    @Override
    protected int executeNonDeferredRequest(URLFetchServicePb.URLFetchRequest req) {
      try {
        FakeHttpServletResponse response = new FakeHttpServletResponse();
        response.setCharacterEncoding("utf-8");

        URL url = new URL(req.getUrl());
        FakeHttpServletRequest request = new FakeHttpServletRequest();
        request.setMethod(req.getMethod().name());
        request.setHostName(url.getHost());
        request.setPort(url.getPort());
        request.setParametersFromQueryString(url.getQuery());

        for (URLFetchServicePb.URLFetchRequest.Header header : req.getHeaderList()) {
          request.setHeader(header.getKey(), header.getValue());
        }

        String payload = req.getPayload().toStringUtf8();
        for (Map.Entry<String, String> entry : extractParamValues(payload).entrySet()) {
          request.addParameter(entry.getKey(), entry.getValue());
        }

        for (Map.Entry<String, String> entry : getExtraParamValues().entrySet()) {
          request.addParameter(entry.getKey(), entry.getValue());
        }

        String servletPath = null;
        HttpServlet servlet = null;
        for (Map.Entry<String, ? extends HttpServlet> entry : getServletMap().entrySet()) {
          if (url.getPath().startsWith(entry.getKey())) {
            servletPath = entry.getKey();
            servlet = entry.getValue();
          }
        }
        if (servlet == null) {
          servlet = getDefaultServlet();
          request.setPathInfo(url.getPath());
        } else {
          int servletPathStart = servletPath.lastIndexOf('/');
          if (servletPathStart == -1) {
            throw new IllegalArgumentException("The servlet path was configured as: "
              + servletPath + " which does not contan a '/'");
          }
          request.setContextPath(servletPath.substring(0, servletPathStart));
          request.setSerletPath(servletPath.substring(servletPathStart));
          request.setPathInfo(url.getPath().substring(servletPath.length()));
        }
        servlet.init();
        servlet.service(request, response);
        int result = response.getStatus();
        return result;
      } catch (Exception ex) {
        log.log(Level.WARNING, ex.getMessage(), ex);
        return HttpServletResponse.SC_INTERNAL_SERVER_ERROR;
      }
    }
  }