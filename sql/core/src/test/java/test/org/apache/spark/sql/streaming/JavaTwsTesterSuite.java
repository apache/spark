/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package test.org.apache.spark.sql.streaming;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.*;

import org.junit.jupiter.api.Test;
import scala.Option;
import scala.Tuple2;
import scala.collection.JavaConverters;
import scala.jdk.javaapi.CollectionConverters;
import scala.reflect.ClassTag$;

import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.streaming.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Java test suite for TwsTester utility. Demonstrates how to use TwsTester from Java code
 * to test StatefulProcessor implementations with value state, list state, map state,
 * timers, and initial state.
 */
public class JavaTwsTesterSuite {

  /**
   * Simple processor with timer support for testing timer functionality.
   * Registers a timer on first input and emits output when timer expires.
   */
  static class SimpleTimerProcessor extends StatefulProcessor<String, String, Tuple2<String, String>> {
    private transient ValueState<Long> timerState;

    @Override
    public void init(OutputMode outputMode, TimeMode timeMode) {
      timerState = this.getHandle().getValueState("timerState",
        Encoders.LONG(), TTLConfig.NONE());
    }

    @Override
    public scala.collection.Iterator<Tuple2<String, String>> handleInputRows(
        String key,
        scala.collection.Iterator<String> rows,
        TimerValues timerValues) {
      
      // Consume rows
      List<String> rowList = new ArrayList<>();
      while (rows.hasNext()) {
        rowList.add(rows.next());
      }

      // Register timer if not already registered
      if (!timerState.exists()) {
        long timerMs = timerValues.getCurrentProcessingTimeInMs() + 1000L;
        getHandle().registerTimer(timerMs);
        timerState.update(timerMs);
      }

      List<Tuple2<String, String>> result = new ArrayList<>();
      result.add(new Tuple2<>(key, "INPUT"));
      return CollectionConverters.asScala(result).iterator();
    }

    @Override
    public scala.collection.Iterator<Tuple2<String, String>> handleExpiredTimer(
        String key,
        TimerValues timerValues,
        ExpiredTimerInfo expiredTimerInfo) {
      timerState.clear();
      List<Tuple2<String, String>> result = new ArrayList<>();
      result.add(new Tuple2<>(key, "TIMER_EXPIRED"));
      return CollectionConverters.asScala(result).iterator();
    }
  }

  /**
   * Processor with multiple state types for comprehensive state testing.
   */
  static class MultiStateProcessor extends StatefulProcessor<Integer, String, Tuple2<Integer, Long>> {
    private transient ValueState<Long> countState;
    private transient ListState<String> itemsList;
    private transient MapState<String, Long> itemCountMap;

    @Override
    public void init(OutputMode outputMode, TimeMode timeMode) {
      countState = this.getHandle().getValueState("countState",
        Encoders.LONG(), TTLConfig.NONE());
      itemsList = this.getHandle().getListState("itemsList",
        Encoders.STRING(), TTLConfig.NONE());
      itemCountMap = this.getHandle().getMapState("itemCountMap",
        Encoders.STRING(), Encoders.LONG(), TTLConfig.NONE());
    }

    @Override
    public scala.collection.Iterator<Tuple2<Integer, Long>> handleInputRows(
        Integer key,
        scala.collection.Iterator<String> rows,
        TimerValues timerValues) {
      
      long count = countState.exists() ? countState.get() : 0L;
      
      while (rows.hasNext()) {
        String item = rows.next();
        count++;
        
        // Update list state
        itemsList.appendValue(item);
        
        // Update map state
        if (itemCountMap.containsKey(item)) {
          itemCountMap.updateValue(item, itemCountMap.getValue(item) + 1);
        } else {
          itemCountMap.updateValue(item, 1L);
        }
      }
      
      countState.update(count);
      
      List<Tuple2<Integer, Long>> result = new ArrayList<>();
      result.add(new Tuple2<>(key, count));
      return CollectionConverters.asScala(result).iterator();
    }
  }

  /**
   * Processor with initial state support.
   */
  static class ProcessorWithInitialState 
      extends StatefulProcessorWithInitialState<String, String, Tuple2<String, Long>, Long> {
    private transient ValueState<Long> countState;

    @Override
    public void init(OutputMode outputMode, TimeMode timeMode) {
      countState = this.getHandle().getValueState("countState",
        Encoders.LONG(), TTLConfig.NONE());
    }

    @Override
    public void handleInitialState(String key, Long initialState, TimerValues timerValues) {
      countState.update(initialState);
    }

    @Override
    public scala.collection.Iterator<Tuple2<String, Long>> handleInputRows(
        String key,
        scala.collection.Iterator<String> rows,
        TimerValues timerValues) {
      
      long count = countState.exists() ? countState.get() : 0L;
      
      while (rows.hasNext()) {
        rows.next();
        count++;
      }
      
      countState.update(count);
      
      List<Tuple2<String, Long>> result = new ArrayList<>();
      result.add(new Tuple2<>(key, count));
      return CollectionConverters.asScala(result).iterator();
    }
  }

  @Test
  public void testBasicProcessing() {
    MultiStateProcessor processor = new MultiStateProcessor();
    List<Tuple2<Integer, Object>> emptyInitialState = new ArrayList<>();
    TwsTester<Integer, String, Tuple2<Integer, Long>> tester = 
      new TwsTester<>(processor, java.time.Clock.systemUTC(), TimeMode.None(), OutputMode.Append(), 
        CollectionConverters.asScala(emptyInitialState).toList());

    // Test with basic input
    List<Tuple2<Integer, String>> input = Arrays.asList(
      new Tuple2<>(1, "a"),
      new Tuple2<>(1, "b"),
      new Tuple2<>(2, "x"),
      new Tuple2<>(1, "c")
    );

    scala.collection.immutable.List<Tuple2<Integer, Long>> output = 
      tester.test(CollectionConverters.asScala(input).toList());
    
    List<Tuple2<Integer, Long>> outputJava = 
      CollectionConverters.asJava(output).stream().toList();

    // Verify output
    assertEquals(2, outputJava.size());
    assertTrue(outputJava.stream().anyMatch(t -> t._1() == 1 && t._2() == 3L));
    assertTrue(outputJava.stream().anyMatch(t -> t._1() == 2 && t._2() == 1L));
  }

  @Test
  public void testValueState() {
    MultiStateProcessor processor = new MultiStateProcessor();
    List<Tuple2<Integer, Object>> emptyInitialState = new ArrayList<>();
    TwsTester<Integer, String, Tuple2<Integer, Long>> tester = 
      new TwsTester<>(processor, java.time.Clock.systemUTC(), TimeMode.None(), OutputMode.Append(), 
        CollectionConverters.asScala(emptyInitialState).toList());

    // Set initial value state
    tester.setValueState("countState", 1, 10L);

    // Process input
    scala.collection.immutable.List<Tuple2<Integer, Long>> output = 
      tester.testOneRow(1, "a");

    // Verify state was updated
    Option<Object> stateOption = tester.peekValueState("countState", 1);
    assertTrue(stateOption.isDefined());
    assertEquals(11L, stateOption.get());
  }

  @Test
  public void testListState() {
    MultiStateProcessor processor = new MultiStateProcessor();
    List<Tuple2<Integer, Object>> emptyInitialState = new ArrayList<>();
    TwsTester<Integer, String, Tuple2<Integer, Long>> tester = 
      new TwsTester<>(processor, java.time.Clock.systemUTC(), TimeMode.None(), OutputMode.Append(), 
        CollectionConverters.asScala(emptyInitialState).toList());

    // Set initial list state
    List<String> initialList = Arrays.asList("x", "y");
    tester.setListState("itemsList", 1, 
      CollectionConverters.asScala(initialList).toList(),
      ClassTag$.MODULE$.apply(String.class));

    // Process input
    tester.testOneRow(1, "z");

    // Verify list state
    scala.collection.immutable.List<Object> listState = 
      tester.peekListState("itemsList", 1);
    List<Object> listStateJava = CollectionConverters.asJava(listState).stream().toList();
    
    assertEquals(3, listStateJava.size());
    assertEquals("x", listStateJava.get(0));
    assertEquals("y", listStateJava.get(1));
    assertEquals("z", listStateJava.get(2));
  }

  @Test
  public void testMapState() {
    MultiStateProcessor processor = new MultiStateProcessor();
    List<Tuple2<Integer, Object>> emptyInitialState = new ArrayList<>();
    TwsTester<Integer, String, Tuple2<Integer, Long>> tester = 
      new TwsTester<>(processor, java.time.Clock.systemUTC(), TimeMode.None(), OutputMode.Append(), 
        CollectionConverters.asScala(emptyInitialState).toList());

    // Set initial map state
    Map<String, Long> initialMap = new HashMap<>();
    initialMap.put("a", 5L);
    initialMap.put("b", 3L);
    // Build Scala immutable map manually
    scala.collection.immutable.HashMap<String, Long> scalaMap = 
      new scala.collection.immutable.HashMap<>();
    for (Map.Entry<String, Long> entry : initialMap.entrySet()) {
      scalaMap = scalaMap.updated(entry.getKey(), entry.getValue());
    }
    tester.setMapState("itemCountMap", 1, scalaMap);

    // Process input
    tester.testOneRow(1, "a");
    tester.testOneRow(1, "c");

    // Verify map state
    scala.collection.immutable.Map<Object, Object> mapState = 
      tester.peekMapState("itemCountMap", 1);
    Map<Object, Object> mapStateJava = CollectionConverters.asJava(mapState);
    
    assertEquals(3, mapStateJava.size());
    assertEquals(6L, mapStateJava.get("a"));
    assertEquals(3L, mapStateJava.get("b"));
    assertEquals(1L, mapStateJava.get("c"));
  }

  @Test
  public void testTimersWithProcessingTime() {
    SimpleTimerProcessor processor = new SimpleTimerProcessor();
    TwsTester.TestClock clock = new TwsTester.TestClock(
      Instant.ofEpochMilli(1000L), java.time.ZoneId.systemDefault());
    
    List<Tuple2<String, Object>> emptyInitialState = new ArrayList<>();
    TwsTester<String, String, Tuple2<String, String>> tester = 
      new TwsTester<>(processor, clock, TimeMode.ProcessingTime(), 
        OutputMode.Append(), CollectionConverters.asScala(emptyInitialState).toList());

    // Process input - this registers a timer
    List<Tuple2<String, String>> input = Arrays.asList(
      new Tuple2<>("key1", "value1")
    );
    scala.collection.immutable.List<Tuple2<String, String>> output1 = 
      tester.test(CollectionConverters.asScala(input).toList());
    
    List<Tuple2<String, String>> output1Java = 
      CollectionConverters.asJava(output1).stream().toList();
    assertEquals(1, output1Java.size());
    assertEquals("key1", output1Java.get(0)._1());
    assertEquals("INPUT", output1Java.get(0)._2());

    // Advance clock to trigger timer
    clock.advanceBy(Duration.ofMillis(1001L));
    
    // Process empty batch to trigger expired timers
    List<Tuple2<String, String>> emptyInput = new ArrayList<>();
    scala.collection.immutable.List<Tuple2<String, String>> output2 = 
      tester.test(CollectionConverters.asScala(emptyInput).toList());
    
    List<Tuple2<String, String>> output2Java = 
      CollectionConverters.asJava(output2).stream().toList();
    assertEquals(1, output2Java.size());
    assertEquals("key1", output2Java.get(0)._1());
    assertEquals("TIMER_EXPIRED", output2Java.get(0)._2());
  }

  @Test
  public void testInitialState() {
    ProcessorWithInitialState processor = new ProcessorWithInitialState();
    
    // Set up initial state
    List<Tuple2<String, Object>> initialState = Arrays.asList(
      new Tuple2<>("key1", 100L),
      new Tuple2<>("key2", 50L)
    );
    
    TwsTester<String, String, Tuple2<String, Long>> tester = 
      new TwsTester<>(processor, java.time.Clock.systemUTC(), TimeMode.None(), 
        OutputMode.Append(), CollectionConverters.asScala(initialState).toList());

    // Verify initial state was set
    Option<Object> state1 = tester.peekValueState("countState", "key1");
    assertTrue(state1.isDefined());
    assertEquals(100L, state1.get());

    Option<Object> state2 = tester.peekValueState("countState", "key2");
    assertTrue(state2.isDefined());
    assertEquals(50L, state2.get());

    // Process input
    List<Tuple2<String, String>> input = Arrays.asList(
      new Tuple2<>("key1", "a"),
      new Tuple2<>("key1", "b"),
      new Tuple2<>("key2", "x")
    );
    scala.collection.immutable.List<Tuple2<String, Long>> output = 
      tester.test(CollectionConverters.asScala(input).toList());
    
    List<Tuple2<String, Long>> outputJava = 
      CollectionConverters.asJava(output).stream().toList();

    // Verify output includes initial state
    assertEquals(2, outputJava.size());
    assertTrue(outputJava.stream().anyMatch(t -> t._1().equals("key1") && t._2() == 102L));
    assertTrue(outputJava.stream().anyMatch(t -> t._1().equals("key2") && t._2() == 51L));
  }

  @Test
  public void testTestOneRowWithValueState() {
    MultiStateProcessor processor = new MultiStateProcessor();
    List<Tuple2<Integer, Object>> emptyInitialState = new ArrayList<>();
    TwsTester<Integer, String, Tuple2<Integer, Long>> tester = 
      new TwsTester<>(processor, java.time.Clock.systemUTC(), TimeMode.None(), OutputMode.Append(), 
        CollectionConverters.asScala(emptyInitialState).toList());

    // Test one row and verify state transition
    Tuple2<scala.collection.immutable.List<Tuple2<Integer, Long>>, Object> result = 
      tester.testOneRowWithValueState(1, "a", "countState", 5L);

    // Verify output
    scala.collection.immutable.List<Tuple2<Integer, Long>> output = result._1();
    List<Tuple2<Integer, Long>> outputJava = 
      CollectionConverters.asJava(output).stream().toList();
    assertEquals(1, outputJava.size());
    assertEquals(6L, outputJava.get(0)._2());

    // Verify state
    assertEquals(6L, result._2());
  }

  @Test
  public void testWithWatermark() {
    SimpleTimerProcessor processor = new SimpleTimerProcessor();
    TwsTester.TestClock clock = new TwsTester.TestClock(
      Instant.ofEpochMilli(10000L), java.time.ZoneId.systemDefault());
    
    List<Tuple2<String, Object>> emptyInitialState = new ArrayList<>();
    TwsTester<String, String, Tuple2<String, String>> tester = 
      new TwsTester<>(processor, clock, TimeMode.EventTime(), 
        OutputMode.Append(), CollectionConverters.asScala(emptyInitialState).toList());

    // Set watermark
    tester.withWatermark(
      (String s) -> new Timestamp(Long.parseLong(s)),
      "2 seconds"
    );

    // Process input with event times
    List<Tuple2<String, String>> input = Arrays.asList(
      new Tuple2<>("key1", "5000"),
      new Tuple2<>("key1", "8000")
    );
    
    scala.collection.immutable.List<Tuple2<String, String>> output = 
      tester.test(CollectionConverters.asScala(input).toList());
    
    // Verify processing occurred
    assertNotNull(output);
    assertTrue(CollectionConverters.asJava(output).size() > 0);
  }

  @Test
  public void testMultipleBatches() {
    MultiStateProcessor processor = new MultiStateProcessor();
    List<Tuple2<Integer, Object>> emptyInitialState = new ArrayList<>();
    TwsTester<Integer, String, Tuple2<Integer, Long>> tester = 
      new TwsTester<>(processor, java.time.Clock.systemUTC(), TimeMode.None(), OutputMode.Append(), 
        CollectionConverters.asScala(emptyInitialState).toList());

    // First batch
    List<Tuple2<Integer, String>> batch1 = Arrays.asList(
      new Tuple2<>(1, "a"),
      new Tuple2<>(1, "b")
    );
    scala.collection.immutable.List<Tuple2<Integer, Long>> output1 = 
      tester.test(CollectionConverters.asScala(batch1).toList());
    
    List<Tuple2<Integer, Long>> output1Java = 
      CollectionConverters.asJava(output1).stream().toList();
    assertEquals(1, output1Java.size());
    assertEquals(2L, output1Java.get(0)._2());

    // Second batch
    List<Tuple2<Integer, String>> batch2 = Arrays.asList(
      new Tuple2<>(1, "c"),
      new Tuple2<>(2, "x")
    );
    scala.collection.immutable.List<Tuple2<Integer, Long>> output2 = 
      tester.test(CollectionConverters.asScala(batch2).toList());
    
    List<Tuple2<Integer, Long>> output2Java = 
      CollectionConverters.asJava(output2).stream().toList();
    assertEquals(2, output2Java.size());
    
    // Verify state persisted between batches
    Option<Object> state = tester.peekValueState("countState", 1);
    assertTrue(state.isDefined());
    assertEquals(3L, state.get());
  }

  @Test
  public void testExistingJavaStatefulProcessor() {
    // Test with the existing TestStatefulProcessor
    test.org.apache.spark.sql.TestStatefulProcessor processor = 
      new test.org.apache.spark.sql.TestStatefulProcessor();
    
    List<Tuple2<Integer, Object>> emptyInitialState = new ArrayList<>();
    TwsTester<Integer, String, String> tester = 
      new TwsTester<>(processor, java.time.Clock.systemUTC(), TimeMode.None(), OutputMode.Append(), 
        CollectionConverters.asScala(emptyInitialState).toList());

    List<Tuple2<Integer, String>> input = Arrays.asList(
      new Tuple2<>(1, "a"),
      new Tuple2<>(1, "b"),
      new Tuple2<>(3, "foo"),
      new Tuple2<>(3, "bar")
    );

    scala.collection.immutable.List<String> output = 
      tester.test(CollectionConverters.asScala(input).toList());
    
    List<String> outputJava = CollectionConverters.asJava(output).stream().toList();
    
    // Verify output
    assertEquals(2, outputJava.size());
    assertTrue(outputJava.stream().anyMatch(s -> s.equals("1ab")));
    assertTrue(outputJava.stream().anyMatch(s -> s.equals("3foobar")));
    
    // Verify value state
    Option<Object> valueState1 = tester.peekValueState("countState", 1);
    assertTrue(valueState1.isDefined());
    assertEquals(2L, valueState1.get());

    // Verify list state
    scala.collection.immutable.List<Object> listState = 
      tester.peekListState("keyList", 1);
    List<Object> listStateJava = CollectionConverters.asJava(listState).stream().toList();
    assertEquals(2, listStateJava.size());

    // Verify map state
    scala.collection.immutable.Map<Object, Object> mapState = 
      tester.peekMapState("keyCountMap", 1);
    Map<Object, Object> mapStateJava = CollectionConverters.asJava(mapState);
    assertTrue(mapStateJava.containsKey("a"));
    assertTrue(mapStateJava.containsKey("b"));
  }

  @Test
  public void testExistingProcessorWithInitialState() {
    // Test with the existing TestStatefulProcessorWithInitialState
    test.org.apache.spark.sql.TestStatefulProcessorWithInitialState processor = 
      new test.org.apache.spark.sql.TestStatefulProcessorWithInitialState();
    
    List<Tuple2<Integer, Object>> initialState = Arrays.asList(
      new Tuple2<>(1, "init1"),
      new Tuple2<>(2, "init2")
    );
    
    TwsTester<Integer, String, String> tester = 
      new TwsTester<>(processor, java.time.Clock.systemUTC(), TimeMode.None(), OutputMode.Append(), 
        CollectionConverters.asScala(initialState).toList());

    // Verify initial state was handled
    Option<Object> state1 = tester.peekValueState("testState", 1);
    assertTrue(state1.isDefined());
    assertEquals("init1", state1.get());

    Option<Object> state2 = tester.peekValueState("testState", 2);
    assertTrue(state2.isDefined());
    assertEquals("init2", state2.get());

    // Process input
    List<Tuple2<Integer, String>> input = Arrays.asList(
      new Tuple2<>(1, "a"),
      new Tuple2<>(1, "b")
    );
    
    scala.collection.immutable.List<String> output = 
      tester.test(CollectionConverters.asScala(input).toList());
    
    List<String> outputJava = CollectionConverters.asJava(output).stream().toList();
    assertEquals(1, outputJava.size());
    assertEquals("1init1ab", outputJava.get(0));
  }
}

