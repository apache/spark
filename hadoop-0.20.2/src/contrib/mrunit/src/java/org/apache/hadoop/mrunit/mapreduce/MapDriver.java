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


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mrunit.MapDriverBase;
import org.apache.hadoop.mrunit.mapreduce.mock.MockMapContextWrapper;
import org.apache.hadoop.mrunit.types.Pair;

/**
 * Harness that allows you to test a Mapper instance. You provide the input
 * key and value that should be sent to the Mapper, and outputs you expect to
 * be sent by the Mapper to the collector for those inputs. By calling
 * runTest(), the harness will deliver the input to the Mapper and will check
 * its outputs against the expected results. This is designed to handle a
 * single (k, v) -> (k, v)* case from the Mapper, representing a single unit
 * test. Multiple input (k, v) pairs should go in separate unit tests.
 */
public class MapDriver<K1, V1, K2, V2> extends MapDriverBase<K1, V1, K2, V2> {

  public static final Log LOG = LogFactory.getLog(MapDriver.class);

  private Mapper<K1, V1, K2, V2> myMapper;
  private Counters counters;

  public MapDriver(final Mapper<K1, V1, K2, V2> m) {
    myMapper = m;
    counters = new Counters();
  }

  public MapDriver() {
    counters = new Counters();
  }


  /**
   * Set the Mapper instance to use with this test driver
   *
   * @param m the Mapper instance to use
   */
  public void setMapper(Mapper<K1, V1, K2, V2> m) {
    myMapper = m;
  }

  /** Sets the Mapper instance to use and returns self for fluent style */
  public MapDriver<K1, V1, K2, V2> withMapper(Mapper<K1, V1, K2, V2> m) {
    setMapper(m);
    return this;
  }

  /**
   * @return the Mapper object being used by this test
   */
  public Mapper<K1, V1, K2, V2> getMapper() {
    return myMapper;
  }

  /** @return the counters used in this test */
  public Counters getCounters() {
    return counters;
  }

  /** Sets the counters object to use for this test.
   * @param ctrs The counters object to use.
   */
  public void setCounters(final Counters ctrs) {
    this.counters = ctrs;
  }

  /** Sets the counters to use and returns self for fluent style */
  public MapDriver<K1, V1, K2, V2> withCounters(final Counters ctrs) {
    setCounters(ctrs);
    return this;
  }

  /**
   * Identical to setInputKey() but with fluent programming style
   *
   * @return this
   */
  public MapDriver<K1, V1, K2, V2> withInputKey(K1 key) {
    setInputKey(key);
    return this;
  }

  /**
   * Identical to setInputValue() but with fluent programming style
   *
   * @param val
   * @return this
   */
  public MapDriver<K1, V1, K2, V2> withInputValue(V1 val) {
    setInputValue(val);
    return this;
  }

  /**
   * Identical to setInput() but returns self for fluent programming style
   *
   * @return this
   */
  public MapDriver<K1, V1, K2, V2> withInput(K1 key, V1 val) {
    setInput(key, val);
    return this;
  }

  /**
   * Identical to setInput() but returns self for fluent programming style
   *
   * @param inputRecord
   * @return this
   */
  public MapDriver<K1, V1, K2, V2> withInput(Pair<K1, V1> inputRecord) {
    setInput(inputRecord);
    return this;
  }

  /**
   * Works like addOutput(), but returns self for fluent style
   *
   * @param outputRecord
   * @return this
   */
  public MapDriver<K1, V1, K2, V2> withOutput(Pair<K2, V2> outputRecord) {
    addOutput(outputRecord);
    return this;
  }

  /**
   * Functions like addOutput() but returns self for fluent programming
   * style
   *
   * @return this
   */
  public MapDriver<K1, V1, K2, V2> withOutput(K2 key, V2 val) {
    addOutput(key, val);
    return this;
  }

  /**
   * Identical to setInputFromString, but with a fluent programming style
   *
   * @param input
   *          A string of the form "key \t val". Trims any whitespace.
   * @return this
   */
  public MapDriver<K1, V1, K2, V2> withInputFromString(String input) {
    setInputFromString(input);
    return this;
  }

  /**
   * Identical to addOutputFromString, but with a fluent programming style
   *
   * @param output
   *          A string of the form "key \t val". Trims any whitespace.
   * @return this
   */
  public MapDriver<K1, V1, K2, V2> withOutputFromString(String output) {
    addOutputFromString(output);
    return this;
  }

  @Override
  public List<Pair<K2, V2>> run() throws IOException {
    List<Pair<K1, V1>> inputs = new ArrayList<Pair<K1, V1>>();
    inputs.add(new Pair<K1, V1>(inputKey, inputVal));

    try {
      MockMapContextWrapper<K1, V1, K2, V2> wrapper = new MockMapContextWrapper();
      MockMapContextWrapper<K1, V1, K2, V2>.MockMapContext context =
          wrapper.getMockContext(configuration, inputs, getCounters());

      myMapper.run(context);
      return context.getOutputs();
    } catch (InterruptedException ie) {
      throw new IOException(ie);
    }
  }

  @Override
  public String toString() {
    return "MapDriver (0.20+) (" + myMapper + ")";
  }
  
  /** 
   * @param configuration The configuration object that will given to the mapper
   *        associated with the driver
   * @return this object for fluent coding
   */
  public MapDriver<K1, V1, K2, V2> withConfiguration(Configuration configuration) {
    setConfiguration(configuration);
	  return this;
  }
}

