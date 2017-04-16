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

import static org.apache.hadoop.mrunit.testutil.ExtendedAssert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;
import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

public class TestMapDriver extends TestCase {

  private Mapper<Text, Text, Text, Text> mapper;
  private MapDriver<Text, Text, Text, Text> driver;

  @Before
  public void setUp() {
    mapper = new Mapper<Text, Text, Text, Text>(); // default action is identity mapper.
    driver = new MapDriver<Text, Text, Text, Text>(mapper);
  }

  @Test
  public void testRun() {
    List<Pair<Text, Text>> out = null;

    try {
      out = driver.withInput(new Text("foo"), new Text("bar")).run();
    } catch (IOException ioe) {
      fail();
    }

    List<Pair<Text, Text>> expected = new ArrayList<Pair<Text, Text>>();
    expected.add(new Pair<Text, Text>(new Text("foo"), new Text("bar")));

    assertListEquals(out, expected);
  }

  @Test
  public void testTestRun1() {
    driver.withInput(new Text("foo"), new Text("bar"))
            .withOutput(new Text("foo"), new Text("bar"))
            .runTest();
  }

  @Test
  public void testTestRun2() {
    try {
      driver.withInput(new Text("foo"), new Text("bar"))
            .runTest();
      fail();
    } catch (RuntimeException re) {
      // expected.
    }
  }

  @Test
  public void testTestRun3() {
    try {
      driver.withInput(new Text("foo"), new Text("bar"))
            .withOutput(new Text("foo"), new Text("bar"))
            .withOutput(new Text("foo"), new Text("bar"))
            .runTest();
      fail();
    } catch (RuntimeException re) {
      // expected.
    }
  }

  @Test
  public void testTestRun4() {
    try {
      driver.withInput(new Text("foo"), new Text("bar"))
            .withOutput(new Text("foo"), new Text("bar"))
            .withOutput(new Text("bonusfoo"), new Text("bar"))
            .runTest();
      fail();
    } catch (RuntimeException re) {
      // expected.
    }

  }
  @Test
  public void testTestRun5() {
    try {
      driver.withInput(new Text("foo"), new Text("bar"))
            .withOutput(new Text("foo"), new Text("somethingelse"))
            .runTest();
      fail();
    } catch (RuntimeException re) {
      // expected.
    }
  }

  @Test
  public void testTestRun6() {
    try {
      driver.withInput(new Text("foo"), new Text("bar"))
              .withOutput(new Text("someotherkey"), new Text("bar"))
              .runTest();
      fail();
    } catch (RuntimeException re) {
      // expected.
    }
  }

  @Test
  public void testTestRun7() {
    try {
      driver.withInput(new Text("foo"), new Text("bar"))
            .withOutput(new Text("someotherkey"), new Text("bar"))
            .withOutput(new Text("foo"), new Text("bar"))
            .runTest();
      fail();
    } catch (RuntimeException re) {
      // expected.
    }
  }

  @Test
  public void testSetInput() {
    try {
      driver.setInput(new Pair<Text, Text>(new Text("foo"), new Text("bar")));
    } catch (Exception e) {
      fail();
    }

    assertEquals(driver.getInputKey(), new Text("foo"));
    assertEquals(driver.getInputValue(), new Text("bar"));
  }

  @Test
  public void testSetInputNull() {
    try {
      driver.setInput((Pair<Text, Text>) null);
      fail();
    } catch (Exception e) {
      // expect this.
    }
  }

  @Test
  public void testEmptyInput() {
    // MapDriver will forcibly map (null, null) as undefined input;
    // identity mapper expects (null, null) back.
    driver.withOutput(null, null).runTest();
  }

  @Test
  public void testEmptyInput2() {
    // it is an error to expect no output because we expect
    // the mapper to be fed (null, null) as an input if the
    // user doesn't set any input.
    try {
      driver.runTest();
      fail();
    } catch (RuntimeException re) {
      // expected.
    }
  }
  
  @Test
  public void testConfiguration() {
	  Configuration conf = new Configuration();
	  conf.set("TestKey", "TestValue");
	  MapDriver<NullWritable, NullWritable, NullWritable, NullWritable> confDriver 
	      = new MapDriver<NullWritable, NullWritable, NullWritable, NullWritable>();
	  ConfigurationMapper<NullWritable, NullWritable, NullWritable, NullWritable> mapper 
	      = new ConfigurationMapper<NullWritable, NullWritable, NullWritable, NullWritable>();
	  confDriver.withMapper(mapper).withConfiguration(conf).
	      withInput(NullWritable.get(),NullWritable.get()).
	      withOutput(NullWritable.get(),NullWritable.get()).runTest();
	  assertEquals(mapper.setupConfiguration.get("TestKey"), "TestValue");
  }

  /**
   * Test mapper which stores the configuration object it was passed during its setup method
   */
  public static class ConfigurationMapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> extends Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {
	public Configuration setupConfiguration;
	
	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		setupConfiguration = context.getConfiguration();
	}
  }
}

