package com.google.appengine.tools.mapreduce.impl.util;

import com.google.appengine.tools.mapreduce.impl.shardedjob.ShardedJobRunId;
import com.google.appengine.tools.pipeline.JobRunId;
import com.google.appengine.tools.pipeline.impl.backend.AppEngineBackEnd;
import com.google.appengine.tools.pipeline.impl.backend.PipelineBackEnd;
import com.google.appengine.tools.pipeline.impl.model.JobRecord;
import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.DatastoreOptions;
import com.google.cloud.datastore.Key;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;

import javax.servlet.http.HttpServletRequest;

import java.net.URLDecoder;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

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
      .thenReturn(URLDecoder.decode(ENCODED_MR_EXAMPLE, StandardCharsets.UTF_8));


    assertEquals(ShardedJobRunId.of("test-project", null,  null,"c6fa877b-81a6-4e17-a8f7-62268036db97").asEncodedString(),
      requestUtils.getMapReduceId(request).asEncodedString());

  }

  @Test
  public void localBootstrap() {
    //make this like local situation
    System.setProperty("GOOGLE_CLOUD_PROJECT", "no_app_id");
    System.setProperty("DATASTORE_EMULATOR_HOST", "http://localhost:8081");

    RequestUtils requestUtils = new RequestUtils();
    HttpServletRequest request = mock(HttpServletRequest.class);

    DatastoreOptions datastore = requestUtils.buildDatastoreFromRequest(request);

    assertEquals(datastore.getHost(), "http://localhost:8081");
  }

  @Test
  public void traceparentHeader() {
    RequestUtils.TraceParent parsed = RequestUtils.TraceParent.of("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01");
    assertEquals("0af7651916cd43dd8448eb211c80319c", parsed.getTraceId());
  }

  @Test
  public void traceparentHeader() {
    RequestUtils.TraceParent parsed = RequestUtils.TraceParent.of("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01");
    assertEquals("0af7651916cd43dd8448eb211c80319c", parsed.getTraceId());
  }
}

