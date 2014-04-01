/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.mapred;

import junit.framework.TestCase;
import java.io.IOException;
import java.text.ParseException;
import java.util.Random;

import org.apache.hadoop.mapred.Counters.Counter;

/**
 * TestCounters checks the sanity and recoverability of {@code Counters}
 */
public class TestCounters extends TestCase {
  enum myCounters {TEST1, TEST2};
  private static final long MAX_VALUE = 10;
  
  // Generates enum based counters
  private Counters getEnumCounters(Enum[] keys) {
    Counters counters = new Counters();
    for (Enum key : keys) {
      for (long i = 0; i < MAX_VALUE; ++i) {
        counters.incrCounter(key, i);
      }
    }
    return counters;
  }
  
  // Generate string based counters
  private Counters getEnumCounters(String[] gNames, String[] cNames) {
    Counters counters = new Counters();
    for (String gName : gNames) {
      for (String cName : cNames) {
        for (long i = 0; i < MAX_VALUE; ++i) {
          counters.incrCounter(gName, cName, i);
        }
      }
    }
    return counters;
  }
  
  /**
   * Test counter recovery
   */
  private void testCounter(Counters counter) throws ParseException {
    String compactEscapedString = counter.makeEscapedCompactString();
    
    Counters recoveredCounter = 
      Counters.fromEscapedCompactString(compactEscapedString);
    // Check for recovery from string
    assertEquals("Recovered counter does not match on content", 
                 counter, recoveredCounter);
    assertEquals("recovered counter has wrong hash code",
                 counter.hashCode(), recoveredCounter.hashCode());
  }
  
  public void testCounters() throws IOException {
    Enum[] keysWithResource = {Task.Counter.MAP_INPUT_BYTES, 
                               Task.Counter.MAP_OUTPUT_BYTES};
    
    Enum[] keysWithoutResource = {myCounters.TEST1, myCounters.TEST2};
    
    String[] groups = {"group1", "group2", "group{}()[]"};
    String[] counters = {"counter1", "counter2", "counter{}()[]"};
    
    try {
      // I. Check enum counters that have resource bundler
      testCounter(getEnumCounters(keysWithResource));

      // II. Check enum counters that dont have resource bundler
      testCounter(getEnumCounters(keysWithoutResource));

      // III. Check string counters
      testCounter(getEnumCounters(groups, counters));
    } catch (ParseException pe) {
      throw new IOException(pe);
    }
  }
  
  /**
   * Verify counter value works
   */
  public void testCounterValue() {
    final int NUMBER_TESTS = 100;
    final int NUMBER_INC = 10;
    final Random rand = new Random();
    for (int i = 0; i < NUMBER_TESTS; i++) {
      long initValue = rand.nextInt();
      long expectedValue = initValue;
      Counter counter = new Counter("foo", "bar", expectedValue);
      assertEquals("Counter value is not initialized correctly",
                   expectedValue, counter.getValue());
      for (int j = 0; j < NUMBER_INC; j++) {
        int incValue = rand.nextInt();
        counter.increment(incValue);
        expectedValue += incValue;
        assertEquals("Counter value is not incremented correctly",
                     expectedValue, counter.getValue());
      }
      expectedValue = rand.nextInt();
      counter.setValue(expectedValue);
      assertEquals("Counter value is not set correctly",
                   expectedValue, counter.getValue());
    }
  }
  
  public static void main(String[] args) throws IOException {
    new TestCounters().testCounters();
  }
}
