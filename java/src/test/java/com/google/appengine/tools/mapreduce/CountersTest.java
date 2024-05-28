package com.google.appengine.tools.mapreduce;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.appengine.tools.mapreduce.impl.CountersImpl;
import org.junit.jupiter.api.Test;

import static org.junit.Assert.assertEquals;

public class CountersTest {

  @Test
  public void writeCounters() throws JsonProcessingException {

    Counters counters = new CountersImpl();

    counters.getCounter("a").increment(1);
    ObjectMapper objectMapper = new ObjectMapper();
    assertEquals("{\"a\":1}", objectMapper.writeValueAsString(counters));

  }
}
