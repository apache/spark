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
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mock.MockOutputCollector;
import org.apache.hadoop.mrunit.mock.MockReporter;
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
public abstract class MapDriverBase<K1, V1, K2, V2> extends TestDriver<K1, V1, K2, V2> {

  public static final Log LOG = LogFactory.getLog(MapDriverBase.class);

  protected K1 inputKey;
  protected V1 inputVal;


  /**
   * Sets the input key to send to the mapper
   *
   */
  public void setInputKey(K1 key) {
    inputKey = key;
  }

  public K1 getInputKey() {
    return inputKey;
  }

  /**
   * Sets the input value to send to the mapper
   *
   * @param val
   */
  public void setInputValue(V1 val) {
    inputVal = val;
  }

  public V1 getInputValue() {
    return inputVal;
  }

  /**
   * Sets the input to send to the mapper
   *
   */
  public void setInput(K1 key, V1 val) {
    setInputKey(key);
    setInputValue(val);
  }

  /**
   * Sets the input to send to the mapper
   *
   * @param inputRecord
   *          a (key, val) pair
   */
  public void setInput(Pair<K1, V1> inputRecord) {
    if (null != inputRecord) {
      setInputKey(inputRecord.getFirst());
      setInputValue(inputRecord.getSecond());
    } else {
      throw new IllegalArgumentException("null inputRecord in setInput()");
    }
  }

  /**
   * Adds an output (k, v) pair we expect from the Mapper
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
   * Adds a (k, v) pair we expect as output from the mapper
   *
   */
  public void addOutput(K2 key, V2 val) {
    addOutput(new Pair<K2, V2>(key, val));
  }

  /**
   * Expects an input of the form "key \t val" Forces the Mapper input types
   * to Text.
   *
   * @param input
   *          A string of the form "key \t val".
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
        setInputValue((V1) inputPair.getSecond());
      } else {
        throw new IllegalArgumentException(
            "Could not parse input pair in setInputFromString");
      }
    }
  }

  /**
   * Expects an input of the form "key \t val" Forces the Mapper output types
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
    String inputValStr = "(null)";

    if (null != inputKey) {
      inputKeyStr = inputKey.toString();
    }

    if (null != inputVal) {
      inputValStr = inputVal.toString();
    }

    LOG.debug("Mapping input (" + inputKeyStr + ", " + inputValStr + ")");

    List<Pair<K2, V2>> outputs = null;

    try {
      outputs = run();
      validate(outputs);
    } catch (IOException ioe) {
      LOG.error("IOException in mapper: " + ioe.toString());
      LOG.debug("Setting success to false based on IOException");
      throw new RuntimeException();
    }
  }
}

