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

package org.apache.hadoop.cli.util;

import java.util.Vector;

/**
 *
 * Class to store CLI Test Comparators Data
 */
public class ComparatorData {
  private String expectedOutput = null;
  private String actualOutput = null;
  private boolean testResult = false;
  private int exitCode = 0;
  private String comparatorType = null;
  
  public ComparatorData() {

  }

  /**
   * @return the expectedOutput
   */
  public String getExpectedOutput() {
    return expectedOutput;
  }

  /**
   * @param expectedOutput the expectedOutput to set
   */
  public void setExpectedOutput(String expectedOutput) {
    this.expectedOutput = expectedOutput;
  }

  /**
   * @return the actualOutput
   */
  public String getActualOutput() {
    return actualOutput;
  }

  /**
   * @param actualOutput the actualOutput to set
   */
  public void setActualOutput(String actualOutput) {
    this.actualOutput = actualOutput;
  }

  /**
   * @return the testResult
   */
  public boolean getTestResult() {
    return testResult;
  }

  /**
   * @param testResult the testResult to set
   */
  public void setTestResult(boolean testResult) {
    this.testResult = testResult;
  }

  /**
   * @return the exitCode
   */
  public int getExitCode() {
    return exitCode;
  }

  /**
   * @param exitCode the exitCode to set
   */
  public void setExitCode(int exitCode) {
    this.exitCode = exitCode;
  }

  /**
   * @return the comparatorType
   */
  public String getComparatorType() {
    return comparatorType;
  }

  /**
   * @param comparatorType the comparatorType to set
   */
  public void setComparatorType(String comparatorType) {
    this.comparatorType = comparatorType;
  }

}
