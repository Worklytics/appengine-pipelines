package com.google.appengine.tools.mapreduce.impl.sort;

import static java.nio.charset.StandardCharsets.US_ASCII;
import static org.junit.jupiter.api.Assertions.*;

import com.google.appengine.tools.mapreduce.KeyValue;
import com.google.appengine.tools.mapreduce.OutputWriter;
import com.google.appengine.tools.mapreduce.impl.IncrementalTaskContext;
import com.google.appengine.tools.mapreduce.impl.shardedjob.ShardedJobRunId;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.UUID;

/**
 * Tests for {@link SortWorker}
 */
public class SortTest {
  private static class StringStringGenerator implements Iterator<KeyValue<ByteBuffer, ByteBuffer>> {

    private static final int KEY_SIZE = 36;
    private static final int VALUE_SIZE = 100;
    private static final byte[] MAX_VALUE = new byte[KEY_SIZE];
    private static final byte[] MIN_VALUE = new byte[KEY_SIZE];
    static {
      Arrays.fill(MAX_VALUE, (byte) 0xFF);
      Arrays.fill(MIN_VALUE, (byte) 0x00);
    }
    private int remaining;
    private final Random sequence = new Random(0);

    StringStringGenerator(int num) {
      this.remaining = num;
    }

    @Override
    public boolean hasNext() {
      return remaining != 0;
    }

    @Override
    public KeyValue<ByteBuffer, ByteBuffer> next() {
      if (remaining <= 0) {
        throw new NoSuchElementException();
      }
      String string = new UUID(sequence.nextLong(), sequence.nextLong()).toString();
      ByteBuffer key = ByteBuffer.allocate(KEY_SIZE).put(string.getBytes(US_ASCII));
      key.limit(key.position());
      key.rewind();
      ByteBuffer value = ByteBuffer.allocate(VALUE_SIZE);
      remaining--;
      return new KeyValue<>(key, value);
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }
  }

  private static final class MapSortContext extends SortContext {

    @SuppressWarnings("serial")
    public MapSortContext() {
      super(new IncrementalTaskContext(ShardedJobRunId.of("test-project", null, null,"TestJob"), 1, 1, "calls", "time"),
        new OutputWriter<KeyValue<ByteBuffer, List<ByteBuffer>>>() {

        @Override
        public void write(KeyValue<ByteBuffer, List<ByteBuffer>> value) {
          throw new UnsupportedOperationException();
        }

        @Override
        public void endShard() {
          throw new UnsupportedOperationException();
        }
      });
    }

    LinkedHashMap<ByteBuffer, List<ByteBuffer>> map = new LinkedHashMap<>();
    int sameKeyCount = 0;

    @Override
    public void emit(KeyValue<ByteBuffer, List<ByteBuffer>> keyValue) {
      ByteBuffer key = keyValue.getKey();
      List<ByteBuffer> list = map.get(key);
      if (list == null) {
        map.put(key, Lists.newArrayList(keyValue.getValue()));
      } else {
        list.addAll(keyValue.getValue());
        sameKeyCount++;
      }
    }
  }

  @Test
  public void testDoesNotRunOutOfMemory() {
    SortWorker s = new SortWorker(null, Integer.MAX_VALUE);
    s.prepare();
    Map<ByteBuffer, List<ByteBuffer>> map =
        sortUntilFull(s, new StringStringGenerator(Integer.MAX_VALUE), null);
    assertTrue(map.size() > 50000, "Map size was: " + map.size()); // Works down to 32mb vm.
  }

  @Test
  public void testThrowsOnOverfill() {
    final int numberToWrite = 4;
    SortWorker s = createWorker(numberToWrite);
    // Assumes no collisions.
    try {
      sortUntilFull(s, new StringStringGenerator(numberToWrite),
          new KeyValue<>(ByteBuffer.allocate(1), ByteBuffer.allocate(1)));
      fail();
    } catch (IllegalArgumentException e) {
      // expected
    }
  }

  @Test
  public void testCorrectOrder() {
    final int numberToWrite = 100000;
    SortWorker s = createWorker(numberToWrite);
    // Assumes no collisions.
    Map<ByteBuffer, List<ByteBuffer>> map =
        sortUntilFull(s, new StringStringGenerator(numberToWrite), null);
    assertTrue(s.isFull()); // Confirms the bufferCapacity setting above.
    assertEquals(numberToWrite, map.size());
    String last = "\0";
    for (ByteBuffer key : map.keySet()) {
      String current = US_ASCII.decode(key).toString();
      assertTrue(last.compareTo(current) < 0, "Last: " + last + " vs " + current);
      last = current;
    }
  }

  @Test
  public void testZeroByteKey() {
    final int numberToWrite = 40;
    SortWorker s = createWorker(numberToWrite);
    // Assumes no collisions.
    Map<ByteBuffer, List<ByteBuffer>> map = sortUntilFull(s,
        new StringStringGenerator(numberToWrite - 1), new KeyValue<>(
            ByteBuffer.allocate(0),
            ByteBuffer.allocate(StringStringGenerator.VALUE_SIZE)));
    assertEquals(numberToWrite, map.size());
    String last = null;
    for (ByteBuffer key : map.keySet()) {
      String string = US_ASCII.decode(key).toString();
      if (last != null) {
        assertTrue(last.compareTo(string) < 0, "Last: " + last + " vs " + string);
      }
      last = string;
    }
  }

  @Test

  public void testLeftOverFirst() {
    final int numberToWrite = 4000;
    SortWorker s = createWorker(numberToWrite);
    // Assumes no collisions.
    Map<ByteBuffer, List<ByteBuffer>> map = sortUntilFull(s,
        new StringStringGenerator(numberToWrite - 1), new KeyValue<>(
            ByteBuffer.wrap(StringStringGenerator.MIN_VALUE),
            ByteBuffer.allocate(StringStringGenerator.VALUE_SIZE)));
    assertTrue(s.isFull()); // Confirms the bufferCapacity setting above.
    assertEquals(numberToWrite, map.size());
    String last = "\0";
    for (ByteBuffer key : map.keySet()) {
      String string = US_ASCII.decode(key).toString();
      assertTrue( last.compareTo(string) < 0, "Last: " + last + " vs " + string);
      last = string;
    }
  }

  @Test
  public void testLeftOverLast() {
    final int numberToWrite = 4000;
    SortWorker s = createWorker(numberToWrite);
    // Assumes no collisions.
    Map<ByteBuffer, List<ByteBuffer>> map = sortUntilFull(s,
        new StringStringGenerator(numberToWrite - 1), new KeyValue<>(
            ByteBuffer.wrap(StringStringGenerator.MAX_VALUE),
            ByteBuffer.allocate(StringStringGenerator.VALUE_SIZE)));
    assertTrue(s.isFull()); // Confirms the bufferCapacity setting above.
    assertEquals(numberToWrite, map.size());
    String last = "\0";
    for (ByteBuffer key : map.keySet()) {
      String string = US_ASCII.decode(key).toString();
      assertTrue(last.compareTo(string) < 0, "Last: " + last + " vs " + string);
      last = string;
    }
  }


  @Test
  public void testLeftOverEqualToMax() {
    final int numberToWrite = 4000;
    SortWorker s = createWorker(numberToWrite);
    // Assumes no collisions.
    LinkedHashMap<ByteBuffer, List<ByteBuffer>> map =
        sortUntilFull(s, new StringStringGenerator(numberToWrite - 1), null);
    s = createWorker(numberToWrite);
    ByteBuffer last = map.keySet().toArray(new ByteBuffer[] {})[numberToWrite - 2];
    map = sortUntilFull(s, new StringStringGenerator(numberToWrite - 1),
        new KeyValue<>(last.slice(), ByteBuffer.allocate(StringStringGenerator.VALUE_SIZE)));
    assertTrue(s.isFull()); // Confirms the bufferCapacity setting above.
    assertEquals(numberToWrite - 1, map.size());
    String previous = "\0";
    for (ByteBuffer key : map.keySet()) {
      String string = US_ASCII.decode(key).toString();
      assertTrue(previous.compareTo(string) <= 0, "Last: " + previous + " vs " + string);
      previous = string;
    }
  }

  private SortWorker createWorker(final int numberToWrite) {
    SortWorker worker = new SortWorker((long) (numberToWrite * (
        StringStringGenerator.KEY_SIZE + StringStringGenerator.VALUE_SIZE
        + SortWorker.POINTER_SIZE_BYTES) - 1), // Set to force the last item to be leftover
        Integer.MAX_VALUE);
    worker.prepare();
    return worker;
  }

  @Test

  public void testValuesSegmentation() {
    int uniqueItems = 10;
    int batchSize = 1000;
    List<Iterator<KeyValue<ByteBuffer, ByteBuffer>>> iters = new ArrayList<>();
    int numDups = 2 * (batchSize / StringStringGenerator.VALUE_SIZE) + 1;
    for (int i = 0; i < numDups; i++) {
      iters.add(new StringStringGenerator(uniqueItems));
    }
    Iterator<KeyValue<ByteBuffer, ByteBuffer>> datax = Iterators.concat(iters.iterator());
    SortWorker sorter = new SortWorker(64 * 1024 * 1024L, batchSize);
    sorter.prepare();
    sorter.beginSlice();
    while (!sorter.isFull() && datax.hasNext()) {
      KeyValue<ByteBuffer, ByteBuffer> next = datax.next();
      sorter.addValue(next.getKey(), next.getValue());
    }
    MapSortContext context = new MapSortContext();
    sorter.setContext(context);
    sorter.endSlice();
    assertEquals(2 * uniqueItems, context.sameKeyCount);

    assertEquals(uniqueItems, context.map.size());
    for (List<ByteBuffer> values : context.map.values()) {
      assertEquals(numDups, values.size());
      ByteBuffer previous = null;
      for (ByteBuffer value : values) {
        if (previous != null) {
          assertEquals(previous, value);
        }
        previous = value;
      }
    }
  }

  @Test
  public void testMultipleValues() {
    Iterator<KeyValue<ByteBuffer, ByteBuffer>> datax4 = Iterators.concat(
        new StringStringGenerator(1000), new StringStringGenerator(1000),
        new StringStringGenerator(1000), new StringStringGenerator(1000));
    SortWorker s = new SortWorker(1024 * 1024L, 4 * 136);
    s.prepare();
    Map<ByteBuffer, List<ByteBuffer>> map = sortUntilFull(s, datax4, null);
    assertEquals(1000, map.size());
    for (List<ByteBuffer> values : map.values()) {
      assertEquals(4, values.size());
      ByteBuffer previous = null;
      for (ByteBuffer value : values) {
        if (previous != null) {
          assertEquals(previous, value);
        }
        previous = value;
      }
    }
  }

  @Test
  public void testPointersFormat() {
    SortWorker worker = createWorker(1000);
    worker.addPointer(1, 10, 11, 100);
    worker.addPointer(111, 10, 121, 200);
    ByteBuffer pointer = worker.readPointer(0);
    assertEquals(1, pointer.getInt(0));
    assertEquals(11, pointer.getInt(4));
    assertEquals(100, pointer.getInt(8));
    pointer = worker.readPointer(1);
    assertEquals(111, pointer.getInt(0));
    assertEquals(121, pointer.getInt(4));
    assertEquals(200, pointer.getInt(8));
  }


  @Test
  public void testKeyValuesRoundTrip() {
    SortWorker worker = createWorker(1000);
    ByteBuffer key = ByteBuffer.wrap(new byte[] {1, 2, 3, 4, 5, 6, 7});
    ByteBuffer value = ByteBuffer.wrap(new byte[] {0, 9, 7, 5, 2, 4});
    worker.addValue(key, value);
    KeyValue<ByteBuffer, ByteBuffer> keyValue = worker.getKeyValueFromPointer(0);
    assertEquals(key, keyValue.getKey());
    assertEquals(value, keyValue.getValue());
    ByteBuffer buffer = worker.getKeyFromPointer(0);
    assertEquals(key, buffer);
  }

  @Test
  public void testSwap() {
    SortWorker worker = createWorker(1000);
    ByteBuffer key1 = ByteBuffer.wrap(new byte[] {1, 2, 3});
    ByteBuffer value1 = ByteBuffer.wrap(new byte[] {4, 5, 6, 7});
    worker.addValue(key1, value1);
    ByteBuffer key2 = ByteBuffer.wrap(new byte[] {8, 9});
    ByteBuffer value2 = ByteBuffer.wrap(new byte[] {10, 11, 12, 13, 14});
    worker.addValue(key2, value2);
    worker.swapPointers(0, 1);
    KeyValue<ByteBuffer, ByteBuffer> keyValue = worker.getKeyValueFromPointer(0);
    assertEquals(key2, keyValue.getKey());
    assertEquals(value2, keyValue.getValue());
    assertEquals(key2, worker.getKeyFromPointer(0));
    keyValue = worker.getKeyValueFromPointer(1);
    assertEquals(key1, keyValue.getKey());
    assertEquals(value1, keyValue.getValue());
    assertEquals(key1, worker.getKeyFromPointer(1));
  }

  @Test
  public void testStoredData() {
    int size = 1000;
    SortWorker worker = new SortWorker(256 * 1024L, 0);
    worker.prepare();
    worker.beginSlice();
    StringStringGenerator gen = new StringStringGenerator(size);
    List<KeyValue<ByteBuffer, ByteBuffer>> input = new ArrayList<>(size);
    for (int i = 0; i < 1000; i++) {
      KeyValue<ByteBuffer, ByteBuffer> next = gen.next();
      worker.addValue(next.getKey(), next.getValue());
      input.add(next);
    }
    assertEquals(size, worker.getValuesHeld());
    for (int i = 0; i < size; i++) {
      assertEquals(input.get(i), worker.getKeyValueFromPointer(i));
    }
  }

  @Test
  public void testDetectsFull() {
    SortWorker worker = new SortWorker(1000L, Integer.MAX_VALUE);
    worker.prepare();
    worker.beginSlice();
    ByteBuffer key = ByteBuffer.allocate(100);
    ByteBuffer value = ByteBuffer.allocate(1000 - 100 - SortWorker.POINTER_SIZE_BYTES);
    worker.addValue(key, value);
    assertFalse(worker.isFull());
    key = ByteBuffer.allocate(1);
    value = ByteBuffer.allocate(0);
    worker.addValue(key, value);
    assertTrue(worker.isFull());
    worker.beginSlice();
    assertFalse(worker.isFull());
    key = ByteBuffer.allocate(100);
    value = ByteBuffer.allocate(1000 - 100 - SortWorker.POINTER_SIZE_BYTES + 1);
    worker.addValue(key, value);
    assertTrue(worker.isFull());
  }

  private LinkedHashMap<ByteBuffer, List<ByteBuffer>> sortUntilFull(SortWorker sorter,
      Iterator<KeyValue<ByteBuffer, ByteBuffer>> input, KeyValue<ByteBuffer, ByteBuffer> extra) {
    sorter.beginSlice();
    while (!sorter.isFull() && input.hasNext()) {
      KeyValue<ByteBuffer, ByteBuffer> next = input.next();
      sorter.addValue(next.getKey(), next.getValue());
    }
    if (extra != null) {
      sorter.addValue(extra.getKey(), extra.getValue());
    }
    SortContext originalContext = sorter.getContext();
    MapSortContext context = new MapSortContext();
    sorter.setContext(context);
    sorter.endSlice();
    sorter.setContext(originalContext);
    return context.map;
  }
}
