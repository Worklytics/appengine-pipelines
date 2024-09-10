package com.google.appengine.tools.mapreduce.impl.util;

import com.google.cloud.datastore.Key;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;

import javax.servlet.http.HttpServletRequest;

import java.net.URLDecoder;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class RequestUtilsTest {

  public final String ENCODED_EXAMPLE = "partition_id+%7B%0A++project_id%3A+%22worklytics-dev%22%0A%7D%0Apath+%7B%0A++kind%3A+%22pipeline-job%22%0A++name%3A+%22c6fa877b-81a6-4e17-a8f7-62268036db97%22%0A%7D%0A";

  @Test
  void getJobId() {

    Key example =
      Key.fromUrlSafe(ENCODED_EXAMPLE);

    RequestUtils requestUtils = new RequestUtils();
    HttpServletRequest request = mock(HttpServletRequest.class);

    // HttpServletRequest *decodes* url params
    when(request.getParameter("root_pipeline_id"))
      .thenReturn(URLDecoder.decode(example.toUrlSafe()));

    //in effect, ensure round-trip encode and decode works
    assertEquals(ENCODED_EXAMPLE,
      requestUtils.getJobId(request, "root_pipeline_id").get());


    String s = URLDecoder.decode(ENCODED_EXAMPLE);


    assertEquals("partition_id {\n" +
      "  project_id: \"worklytics-dev\"\n" +
      "}\n" +
      "path {\n" +
      "  kind: \"pipeline-job\"\n" +
      "  name: \"c6fa877b-81a6-4e17-a8f7-62268036db97\"\n" +
      "}\n", s);
  }

  @SneakyThrows
  @Test
  public void getMapReduceId() {
    RequestUtils requestUtils = new RequestUtils();
    HttpServletRequest request = mock(HttpServletRequest.class);

    when(request.getParameter("mapreduce_id"))
      .thenReturn(URLDecoder.decode(ENCODED_EXAMPLE));


    assertEquals(Key.fromUrlSafe(ENCODED_EXAMPLE).toUrlSafe(),
      requestUtils.getMapReduceId(request));

  }
}

