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

import java.util.ArrayList;

/**
 *
 * Class to store CLI Test Data
 */
public class CLITestData {
  private String testDesc = null;
  private ArrayList<TestCmd> testCommands = null;
  private ArrayList<TestCmd> cleanupCommands = null;
  private ArrayList<ComparatorData> comparatorData = null;
  private boolean testResult = false;
  
  public CLITestData() {

  }

  /**
   * Class to define Test Command. includes type of the command and command itself
   * Valid types FS, DFSADMIN and MRADMIN.
   */
  static public class TestCmd {
    public enum CommandType {
        FS,
        DFSADMIN,
        MRADMIN
    }
    private final CommandType type;
    private final String cmd;

    public TestCmd(String str, CommandType type) {
      cmd = str;
      this.type = type;
    }
    public CommandType getType() {
      return type;
    }
    public String getCmd() {
      return cmd;
    }
    public String toString() {
      return cmd;
    }
  }
  
  /**
   * @return the testDesc
   */
  public String getTestDesc() {
    return testDesc;
  }

  /**
   * @param testDesc the testDesc to set
   */
  public void setTestDesc(String testDesc) {
    this.testDesc = testDesc;
  }

  /**
   * @return the testCommands
   */
  public ArrayList<TestCmd> getTestCommands() {
    return testCommands;
  }

  /**
   * @param testCommands the testCommands to set
   */
  public void setTestCommands(ArrayList<TestCmd> testCommands) {
    this.testCommands = testCommands;
  }

  /**
   * @return the comparatorData
   */
  public ArrayList<ComparatorData> getComparatorData() {
    return comparatorData;
  }

  /**
   * @param comparatorData the comparatorData to set
   */
  public void setComparatorData(ArrayList<ComparatorData> comparatorData) {
    this.comparatorData = comparatorData;
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
   * @return the cleanupCommands
   */
  public ArrayList<TestCmd> getCleanupCommands() {
    return cleanupCommands;
  }

  /**
   * @param cleanupCommands the cleanupCommands to set
   */
  public void setCleanupCommands(ArrayList<TestCmd> cleanupCommands) {
    this.cleanupCommands = cleanupCommands;
  }
}
