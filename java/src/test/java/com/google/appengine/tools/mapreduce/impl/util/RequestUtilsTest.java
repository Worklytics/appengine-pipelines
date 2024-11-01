package com.google.appengine.tools.mapreduce.impl.util;

import com.google.appengine.tools.mapreduce.impl.shardedjob.ShardedJobRunId;
import com.google.cloud.datastore.Key;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;

import javax.servlet.http.HttpServletRequest;

import java.net.URLDecoder;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class RequestUtilsTest {

  public final String ENCODED_MR_EXAMPLE = "test-project:::c6fa877b-81a6-4e17-a8f7-62268036db97";

  //jobId, mapreduceId basically the same when encoded. type implied by context
  @SneakyThrows
  @Test
  public void getMapReduceId() {
    RequestUtils requestUtils = new RequestUtils();
    HttpServletRequest request = mock(HttpServletRequest.class);

    when(request.getParameter("mapreduce_id"))
      .thenReturn(URLDecoder.decode(ENCODED_MR_EXAMPLE));


    assertEquals(ShardedJobRunId.of("test-project", null,  null,"c6fa877b-81a6-4e17-a8f7-62268036db97").asEncodedString(),
      requestUtils.getMapReduceId(request).asEncodedString());

  }
}

