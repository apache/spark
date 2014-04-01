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


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mock.MockOutputCollector;
import org.apache.hadoop.mrunit.mock.MockReporter;
import org.apache.hadoop.mrunit.types.Pair;

/**
 * Harness that allows you to test a Reducer instance. You provide a key and a
 * set of intermediate values for that key that represent inputs that should
 * be sent to the Reducer (as if they came from a Mapper), and outputs you
 * expect to be sent by the Reducer to the collector. By calling runTest(),
 * the harness will deliver the input to the Reducer and will check its
 * outputs against the expected results. This is designed to handle a single
 * (k, v*) -> (k, v)* case from the Reducer, representing a single unit test.
 * Multiple input (k, v*) sets should go in separate unit tests.
 */
public abstract class ReduceDriverBase<K1, V1, K2, V2> extends TestDriver<K1, V1, K2, V2> {

  protected K1 inputKey;
  protected List<V1> inputValues;

  public ReduceDriverBase() {
    inputValues = new ArrayList<V1>();
  }

  /**
   * Sets the input key to send to the Reducer
   *
   */
  public void setInputKey(K1 key) {
    inputKey = key;
  }

  /**
   * adds an input value to send to the reducer
   *
   * @param val
   */
  public void addInputValue(V1 val) {
    inputValues.add(val);
  }

  /**
   * Sets the input values to send to the reducer; overwrites existing ones
   *
   * @param values
   */
  public void setInputValues(List<V1> values) {
    inputValues.clear();
    inputValues.addAll(values);
  }

  /**
   * Adds a set of input values to send to the reducer
   *
   * @param values
   */
  public void addInputValues(List<V1> values) {
    inputValues.addAll(values);
  }

  /**
   * Sets the input to send to the reducer
   *
   * @param values
   */
  public void setInput(K1 key, List<V1> values) {
    setInputKey(key);
    setInputValues(values);
  }

  /**
   * Adds an output (k, v) pair we expect from the Reducer
   *
   * @param outputRecord
   *          The (k, v) pair to add
   */
  public void addOutput(Pair<K2, V2> outputRecord) {
    if (null != outputRecord) {
      expectedOutputs.add(outputRecord);
    } else {
      throw new IllegalArgumentException("Tried to add null outputRecord");
    }
  }

  /**
   * Adds an output (k, v) pair we expect from the Reducer
   *
   * @param key The key part of a (k, v) pair to add
   * @param val The val part of a (k, v) pair to add
   */
  public void addOutput(K2 key, V2 val) {
    addOutput(new Pair<K2, V2>(key, val));
  }

  /**
   * Expects an input of the form "key \t val, val, val..." Forces the Reducer
   * input types to Text.
   *
   * @param input
   *          A string of the form "key \t val,val,val". Trims any whitespace.
   */
  public void setInputFromString(String input) {
    if (null == input) {
      throw new IllegalArgumentException("null input given to setInputFromString");
    } else {
      Pair<Text, Text> inputPair = parseTabbedPair(input);
      if (null != inputPair) {
        // I know this is not type-safe, but I don't know a better way to do
        // this.
        setInputKey((K1) inputPair.getFirst());
        setInputValues((List<V1>) parseCommaDelimitedList(inputPair.getSecond()
                .toString()));
      } else {
        throw new IllegalArgumentException(
            "Could not parse input pair in setInputFromString");
      }
    }
  }

  /**
   * Expects an input of the form "key \t val" Forces the Reducer output types
   * to Text.
   *
   * @param output
   *          A string of the form "key \t val". Trims any whitespace.
   */
  public void addOutputFromString(String output) {
    if (null == output) {
      throw new IllegalArgumentException("null input given to setOutput");
    } else {
      Pair<Text, Text> outputPair = parseTabbedPair(output);
      if (null != outputPair) {
        // I know this is not type-safe, but I don't know a better way to do
        // this.
        addOutput((Pair<K2, V2>) outputPair);
      } else {
        throw new IllegalArgumentException("Could not parse output pair in setOutput");
      }
    }
  }

  public abstract List<Pair<K2, V2>> run() throws IOException;

  @Override
  public void runTest() throws RuntimeException {

    String inputKeyStr = "(null)";

    if (null != inputKey) {
      inputKeyStr = inputKey.toString();
    }

    StringBuilder sb = new StringBuilder();
    formatValueList(inputValues, sb);

    LOG.debug("Reducing input (" + inputKeyStr + ", " + sb.toString() + ")");

    List<Pair<K2, V2>> outputs = null;
    try {
      outputs = run();
      validate(outputs);
    } catch (IOException ioe) {
      LOG.error("IOException in reducer: " + ioe.toString());
      LOG.debug("Setting success to false based on IOException");
      throw new RuntimeException();
    }
  }
}

