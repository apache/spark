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
package org.apache.hadoop.mrunit;

import static org.apache.hadoop.mrunit.testutil.ExtendedAssert.assertListEquals;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import junit.framework.TestCase;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.mapred.lib.LongSumReducer;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

public class TestMapReduceDriver extends TestCase {

  private static final int FOO_IN_A = 42;
  private static final int FOO_IN_B = 10;
  private static final int BAR_IN = 12;
  private static final int FOO_OUT = 52;

  private Mapper<Text, LongWritable, Text, LongWritable> mapper;
  private Reducer<Text, LongWritable, Text, LongWritable> reducer;
  private MapReduceDriver<Text, LongWritable,
                  Text, LongWritable,
                  Text, LongWritable> driver;

  private MapReduceDriver<Text, Text, Text, Text, Text, Text> driver2;

  @Before
  public void setUp() throws Exception {
    mapper = new IdentityMapper<Text, LongWritable>();
    reducer = new LongSumReducer<Text>();
    driver = new MapReduceDriver<Text, LongWritable,
                                 Text, LongWritable,
                                 Text, LongWritable>(
                        mapper, reducer);
    // for shuffle tests
    driver2 = new MapReduceDriver<Text, Text, Text, Text, Text, Text>();
  }

  @Test
  public void testRun() {
    List<Pair<Text, LongWritable>> out = null;
    try {
      out = driver
              .withInput(new Text("foo"), new LongWritable(FOO_IN_A))
              .withInput(new Text("foo"), new LongWritable(FOO_IN_B))
              .withInput(new Text("bar"), new LongWritable(BAR_IN))
              .run();
    } catch (IOException ioe) {
      fail();
    }

    List<Pair<Text, LongWritable>> expected =
      new ArrayList<Pair<Text, LongWritable>>();
    expected.add(new Pair<Text, LongWritable>(new Text("bar"),
            new LongWritable(BAR_IN)));
    expected.add(new Pair<Text, LongWritable>(new Text("foo"),
            new LongWritable(FOO_OUT)));

    assertListEquals(out, expected);
  }

  @Test
  public void testTestRun1() {
    driver
            .withInput(new Text("foo"), new LongWritable(FOO_IN_A))
            .withInput(new Text("foo"), new LongWritable(FOO_IN_B))
            .withInput(new Text("bar"), new LongWritable(BAR_IN))
            .withOutput(new Text("bar"), new LongWritable(BAR_IN))
            .withOutput(new Text("foo"), new LongWritable(FOO_OUT))
            .runTest();
  }

  @Test
  public void testTestRun2() {
    driver
            .withInput(new Text("foo"), new LongWritable(FOO_IN_A))
            .withInput(new Text("bar"), new LongWritable(BAR_IN))
            .withInput(new Text("foo"), new LongWritable(FOO_IN_B))
            .withOutput(new Text("bar"), new LongWritable(BAR_IN))
            .withOutput(new Text("foo"), new LongWritable(FOO_OUT))
            .runTest();
  }

  @Test
  public void testTestRun3() {
    try {
      driver
            .withInput(new Text("foo"), new LongWritable(FOO_IN_A))
            .withInput(new Text("bar"), new LongWritable(BAR_IN))
            .withInput(new Text("foo"), new LongWritable(FOO_IN_B))
            .withOutput(new Text("foo"), new LongWritable(FOO_OUT))
            .withOutput(new Text("bar"), new LongWritable(BAR_IN))
            .runTest();
      fail();
    } catch (RuntimeException re) {
      // expected
    }
  }

  @Test
  public void testEmptyInput() {
    driver.runTest();
  }

  @Test
  public void testEmptyInputWithOutputFails() {
    try {
      driver
              .withOutput(new Text("foo"), new LongWritable(FOO_OUT))
              .runTest();
      fail();
    } catch (RuntimeException re) {
      // expected.
    }
  }

  @Test
  public void testEmptyShuffle() {
    List<Pair<Text, Text>> inputs = new ArrayList<Pair<Text, Text>>();
    List<Pair<Text, List<Text>>> outputs = driver2.shuffle(inputs);
    assertEquals(0, outputs.size());
  }

  // just shuffle a single (k, v) pair
  @Test
  public void testSingleShuffle() {
    List<Pair<Text, Text>> inputs = new ArrayList<Pair<Text, Text>>();
    inputs.add(new Pair<Text, Text>(new Text("a"), new Text("b")));

    List<Pair<Text, List<Text>>> outputs = driver2.shuffle(inputs);

    List<Pair<Text, List<Text>>> expected = new ArrayList<Pair<Text, List<Text>>>();
    List<Text> sublist = new ArrayList<Text>();
    sublist.add(new Text("b"));
    expected.add(new Pair<Text, List<Text>>(new Text("a"), sublist));

    assertListEquals(expected, outputs);
  }

  // shuffle multiple values from the same key.
  @Test
  public void testShuffleOneKey() {
    List<Pair<Text, Text>> inputs = new ArrayList<Pair<Text, Text>>();
    inputs.add(new Pair<Text, Text>(new Text("a"), new Text("b")));
    inputs.add(new Pair<Text, Text>(new Text("a"), new Text("c")));

    List<Pair<Text, List<Text>>> outputs = driver2.shuffle(inputs);

    List<Pair<Text, List<Text>>> expected = new ArrayList<Pair<Text, List<Text>>>();
    List<Text> sublist = new ArrayList<Text>();
    sublist.add(new Text("b"));
    sublist.add(new Text("c"));
    expected.add(new Pair<Text, List<Text>>(new Text("a"), sublist));

    assertListEquals(expected, outputs);
  }

  // shuffle multiple keys
  @Test
  public void testMultiShuffle1() {
    List<Pair<Text, Text>> inputs = new ArrayList<Pair<Text, Text>>();
    inputs.add(new Pair<Text, Text>(new Text("a"), new Text("x")));
    inputs.add(new Pair<Text, Text>(new Text("b"), new Text("z")));
    inputs.add(new Pair<Text, Text>(new Text("b"), new Text("w")));
    inputs.add(new Pair<Text, Text>(new Text("a"), new Text("y")));

    List<Pair<Text, List<Text>>> outputs = driver2.shuffle(inputs);

    List<Pair<Text, List<Text>>> expected = new ArrayList<Pair<Text, List<Text>>>();
    List<Text> sublist1 = new ArrayList<Text>();
    sublist1.add(new Text("x"));
    sublist1.add(new Text("y"));
    expected.add(new Pair<Text, List<Text>>(new Text("a"), sublist1));

    List<Text> sublist2 = new ArrayList<Text>();
    sublist2.add(new Text("z"));
    sublist2.add(new Text("w"));
    expected.add(new Pair<Text, List<Text>>(new Text("b"), sublist2));

    assertListEquals(expected, outputs);
  }


  // shuffle multiple keys that are out-of-order to start.
  @Test
  public void testMultiShuffle2() {
    List<Pair<Text, Text>> inputs = new ArrayList<Pair<Text, Text>>();
    inputs.add(new Pair<Text, Text>(new Text("b"), new Text("z")));
    inputs.add(new Pair<Text, Text>(new Text("a"), new Text("x")));
    inputs.add(new Pair<Text, Text>(new Text("b"), new Text("w")));
    inputs.add(new Pair<Text, Text>(new Text("a"), new Text("y")));

    List<Pair<Text, List<Text>>> outputs = driver2.shuffle(inputs);

    List<Pair<Text, List<Text>>> expected = new ArrayList<Pair<Text, List<Text>>>();
    List<Text> sublist1 = new ArrayList<Text>();
    sublist1.add(new Text("x"));
    sublist1.add(new Text("y"));
    expected.add(new Pair<Text, List<Text>>(new Text("a"), sublist1));

    List<Text> sublist2 = new ArrayList<Text>();
    sublist2.add(new Text("z"));
    sublist2.add(new Text("w"));
    expected.add(new Pair<Text, List<Text>>(new Text("b"), sublist2));

    assertListEquals(expected, outputs);
  }

  // Test "combining" with an IdentityReducer. Result should be the same.
  @Test
  public void testIdentityCombiner() {
    driver
            .withCombiner(new IdentityReducer<Text, LongWritable>())
            .withInput(new Text("foo"), new LongWritable(FOO_IN_A))
            .withInput(new Text("foo"), new LongWritable(FOO_IN_B))
            .withInput(new Text("bar"), new LongWritable(BAR_IN))
            .withOutput(new Text("bar"), new LongWritable(BAR_IN))
            .withOutput(new Text("foo"), new LongWritable(FOO_OUT))
            .runTest();
  }

  // Test "combining" with another LongSumReducer. Result should be the same.
  @Test
  public void testLongSumCombiner() {
    driver
            .withCombiner(new LongSumReducer<Text>())
            .withInput(new Text("foo"), new LongWritable(FOO_IN_A))
            .withInput(new Text("foo"), new LongWritable(FOO_IN_B))
            .withInput(new Text("bar"), new LongWritable(BAR_IN))
            .withOutput(new Text("bar"), new LongWritable(BAR_IN))
            .withOutput(new Text("foo"), new LongWritable(FOO_OUT))
            .runTest();
  }

  // Test "combining" with another LongSumReducer, and with the Reducer
  // set to IdentityReducer. Result should be the same.
  @Test
  public void testLongSumCombinerAndIdentityReduce() {
    driver
            .withCombiner(new LongSumReducer<Text>())
            .withReducer(new IdentityReducer<Text, LongWritable>())
            .withInput(new Text("foo"), new LongWritable(FOO_IN_A))
            .withInput(new Text("foo"), new LongWritable(FOO_IN_B))
            .withInput(new Text("bar"), new LongWritable(BAR_IN))
            .withOutput(new Text("bar"), new LongWritable(BAR_IN))
            .withOutput(new Text("foo"), new LongWritable(FOO_OUT))
            .runTest();
  }
  
  // Test the key grouping and value ordering comparators
  @Test
  public void testComparators() {
    // group comparator - group by first character
    RawComparator groupComparator = new RawComparator() {
      @Override
      public int compare(Object o1, Object o2) {
        return o1.toString().substring(0, 1).compareTo(
            o2.toString().substring(0, 1));
      }

      @Override
      public int compare(byte[] arg0, int arg1, int arg2, byte[] arg3,
          int arg4, int arg5) {
        throw new RuntimeException("Not implemented");
      }  
    };
    
    // value order comparator - order by second character
    RawComparator orderComparator = new RawComparator() {
      @Override
      public int compare(Object o1, Object o2) {
        return o1.toString().substring(1, 2).compareTo(
            o2.toString().substring(1, 2));
      }
      
      @Override
      public int compare(byte[] arg0, int arg1, int arg2, byte[] arg3,
          int arg4, int arg5) {
        throw new RuntimeException("Not implemented");
      }
    };
    
    // reducer to track the order of the input values using bit shifting
    driver.withReducer(new Reducer<Text, LongWritable, Text, LongWritable>() {
      @Override
      public void reduce(Text key, Iterator<LongWritable> values,
          OutputCollector<Text, LongWritable> output, Reporter reporter)
          throws IOException {
        long outputValue = 0;
        int count = 0;
        while (values.hasNext()) {
          outputValue |= (values.next().get() << (count++*8));
        }
        
        output.collect(key, new LongWritable(outputValue));
      }

      @Override
      public void configure(JobConf job) {}

      @Override
      public void close() throws IOException {}
    });
    
    driver.withKeyGroupingComparator(groupComparator);
    driver.withKeyOrderComparator(orderComparator);
    
    driver.addInput(new Text("a1"), new LongWritable(1));
    driver.addInput(new Text("b1"), new LongWritable(1));
    driver.addInput(new Text("a3"), new LongWritable(3));
    driver.addInput(new Text("a2"), new LongWritable(2));
    
    driver.addOutput(new Text("a1"), new LongWritable(0x1 | (0x2 << 8) | (0x3 << 16)));
    driver.addOutput(new Text("b1"), new LongWritable(0x1));
    
    driver.runTest();
  }
}

