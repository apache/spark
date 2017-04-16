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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.types.Pair;

public abstract class TestDriver<K1, V1, K2, V2> {

  public static final Log LOG = LogFactory.getLog(TestDriver.class);

  protected List<Pair<K2, V2>> expectedOutputs;
  
  protected Configuration configuration;

  public TestDriver() {
    expectedOutputs = new ArrayList<Pair<K2, V2>>();
    configuration = new Configuration();
  }

  /**
   * @return the list of (k, v) pairs expected as output from this driver
   */
  public List<Pair<K2, V2>> getExpectedOutputs() {
    return expectedOutputs;
  }

  /**
   * Clears the list of outputs expected from this driver
   */
  public void resetOutput() {
    expectedOutputs.clear();
  }

  /**
   * Runs the test but returns the result set instead of validating it
   * (ignores any addOutput(), etc calls made before this)
   * @return the list of (k, v) pairs returned as output from the test
   */
  public abstract List<Pair<K2, V2>> run() throws IOException;

  /**
   * Runs the test and validates the results
   * @return void if the tests passed
   * @throws RuntimeException if they don't
   * *
   */
  public abstract void runTest() throws RuntimeException;

  /**
   * Split "key \t val" into Pair(Text(key), Text(val))
   * @param tabSeparatedPair
   */
  public static Pair<Text, Text> parseTabbedPair(String tabSeparatedPair) {

    String key, val;

    if (null == tabSeparatedPair) {
      return null;
    }

    int split = tabSeparatedPair.indexOf('\t');
    if (-1 == split) {
      return null;
    }

    key = tabSeparatedPair.substring(0, split);
    val = tabSeparatedPair.substring(split + 1);

    return new Pair<Text, Text>(new Text(key), new Text(val));
  }

  /**
   * Split "val,val,val,val..." into a List of Text(val) objects.
   * @param commaDelimList A list of values separated by commas
   */
  protected static List<Text> parseCommaDelimitedList(String commaDelimList) {
    ArrayList<Text> outList = new ArrayList<Text>();

    if (null == commaDelimList) {
      return null;
    }

    int len = commaDelimList.length();
    int curPos = 0;
    int curComma = commaDelimList.indexOf(',');
    if (curComma == -1) {
      curComma = len;
    }

    while (curPos < len) {
      outList.add(new Text(
              commaDelimList.substring(curPos, curComma).trim()));
      curPos = curComma + 1;
      curComma = commaDelimList.indexOf(',', curPos);
      if (curComma == -1) {
        curComma = len;
      }
    }

    return outList;
  }

  /**
   * check the outputs against the expected inputs in record
   * @param outputs The actual output (k, v) pairs from the Mapper
   * @return void if they all pass
   * @throws RuntimeException if they don't
   */
  protected void validate(List<Pair<K2, V2>> outputs) throws RuntimeException {

    boolean success = true;

    // were we supposed to get output in the first place?
    // return false if we don't.
    if (expectedOutputs.size() == 0 && outputs.size() > 0) {
      LOG.error("Expected no outputs; got " + outputs.size() + " outputs.");
      success = false;
    }

    // make sure all actual outputs are in the expected set,
    // and at the proper position.
    for (int i = 0; i < outputs.size(); i++) {
      Pair<K2, V2> actual = outputs.get(i);
      success = lookupExpectedValue(actual, i) && success;
    }

    // make sure all expected outputs were accounted for.
    if (expectedOutputs.size() != outputs.size() || !success) {
      // something is unaccounted for. Figure out what.

      ArrayList<Pair<K2, V2>> actuals = new ArrayList<Pair<K2, V2>>();
      actuals.addAll(outputs);

      for (int i = 0; i < expectedOutputs.size(); i++) {
        Pair<K2, V2> expected = expectedOutputs.get(i);

        boolean found = false;
        for (int j = 0; j < actuals.size() && !found; j++) {
          Pair<K2, V2> actual = actuals.get(j);

          if (actual.equals(expected)) {
            // don't match against this actual output again
            actuals.remove(j);
            found = true;
          }
        }

        if (!found) {
          String expectedStr = "(null)";
          if (null != expected) {
            expectedStr = expected.toString();
          }

          LOG.error("Missing expected output " + expectedStr + " at position "
              + i);
        }
      }

      success = false;
    }

    if (!success) {
      throw new RuntimeException();
    }
  }

  /**
   * Part of the validation system.
   * @param actualVal A (k, v) pair we got from the Mapper
   * @param actualPos The position of this pair in the actual output
   * @return true if the expected val at 'actualPos' in the expected
   *              list equals actualVal
   */
  private boolean lookupExpectedValue(Pair<K2, V2> actualVal, int actualPos) {

    // first: Do we have the success condition?
    if (expectedOutputs.size() > actualPos
            && expectedOutputs.get(actualPos).equals(actualVal)) {
      LOG.debug("Matched expected output " + actualVal.toString()
          + " at position " + actualPos);
      return true;
    }

    // second: can we find this output somewhere else in
    // the expected list?
    boolean foundSomewhere = false;

    for (int i = 0; i < expectedOutputs.size() && !foundSomewhere; i++) {
      Pair<K2, V2> expected = expectedOutputs.get(i);

      if (expected.equals(actualVal)) {
        LOG.error("Matched expected output "
                + actualVal.toString() + " but at incorrect position "
                + actualPos + " (expected position " + i + ")");
        foundSomewhere = true;
      }
    }

    if (!foundSomewhere) {
      LOG.error("Received unexpected output " + actualVal.toString());
    }

    return false;
  }

  protected static void formatValueList(List values, StringBuilder sb) {
    sb.append("(");

    if (null != values) {
      boolean first = true;

      for (Object val : values) {
        if (!first) {
          sb.append(", ");
        }

        first = false;
        sb.append(val.toString());
      }
    }

    sb.append(")");
  }

  /** 
   * @return The configuration object that will given to the mapper and/or 
   *         reducer associated with the driver (new API only)
   */
  public Configuration getConfiguration() {
    return configuration;
  }
  
  /**
   * @param configuration The configuration object that will given to the 
   *        mapper and/or reducer associated with the driver (new API only)
   */
  public void setConfiguration(Configuration configuration) {
    this.configuration = configuration;
  }
}
