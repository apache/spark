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

package org.apache.hadoop.mrunit.mapreduce;

import junit.framework.TestCase;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.junit.Before;
import org.junit.Test;

/**
 * Test counters usage in various drivers.
 */
public class TestCounters extends TestCase {

  private static final String GROUP = "GROUP";
  private static final String ELEM = "ELEM";

  private class CounterMapper extends Mapper<Text, Text, Text, Text> {
    public void map(Text k, Text v, Context context)
        throws IOException, InterruptedException {

      context.getCounter(GROUP, ELEM).increment(1);

      // Emit the same (k, v) pair twice.
      context.write(k, v);
      context.write(k, v);
    }
  }

  private class CounterReducer extends Reducer<Text, Text, Text, Text> {
    public void reduce(Text k, Iterable<Text> vals, Context context)
        throws IOException, InterruptedException {

      for(Text val : vals) {
        context.getCounter(GROUP, ELEM).increment(1);
        context.write(k, val);
      }
    }
  }

  @Test
  public void testMapper() throws IOException {
    Mapper<Text, Text, Text, Text> mapper = new CounterMapper();
    MapDriver<Text, Text, Text, Text> driver = new MapDriver<Text, Text, Text, Text>(mapper);
    driver.withInput(new Text("foo"), new Text("bar")).run();
    assertEquals("Expected 1 counter increment", 1,
        driver.getCounters().findCounter(GROUP, ELEM).getValue());
  }

  @Test
  public void testReducer() throws IOException {
    Reducer<Text, Text, Text, Text> reducer = new CounterReducer();
    ReduceDriver<Text, Text, Text, Text> driver = new ReduceDriver<Text, Text, Text, Text>(reducer);
    driver.withInputKey(new Text("foo"))
          .withInputValue(new Text("bar"))
          .run();
    assertEquals("Expected 1 counter increment", 1,
        driver.getCounters().findCounter(GROUP, ELEM).getValue());
  }

  @Test
  public void testMapReduce() throws IOException {
    Mapper<Text, Text, Text, Text> mapper = new CounterMapper();
    Reducer<Text, Text, Text, Text> reducer = new CounterReducer();
    MapReduceDriver<Text, Text, Text, Text, Text, Text> driver =
        new MapReduceDriver<Text, Text, Text, Text, Text, Text>(mapper, reducer);

    driver.withInput(new Text("foo"), new Text("bar"))
          .run();

    assertEquals("Expected counter=3", 3,
        driver.getCounters().findCounter(GROUP, ELEM).getValue());
  }
}

