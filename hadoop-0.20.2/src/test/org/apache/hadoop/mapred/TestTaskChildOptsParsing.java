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

import static org.junit.Assert.*;

import org.junit.Test;

public class TestTaskChildOptsParsing {
  
  @SuppressWarnings("deprecation")
  private static final TaskAttemptID TASK_ID = new TaskAttemptID();
  private static final String[] EXPECTED_RESULTS = new String[]{"-Dfoo=bar", "-Dbaz=biz"};
  
  private void performTest(String input) {
    String[] result = TaskRunner.parseChildJavaOpts(input, TASK_ID);
    assertArrayEquals(EXPECTED_RESULTS, result);
  }
  
  @Test
  public void testParseChildJavaOptsLeadingSpace() {
    performTest(" -Dfoo=bar -Dbaz=biz");
  }
  
  @Test
  public void testParseChildJavaOptsTrailingSpace() {
    performTest("-Dfoo=bar -Dbaz=biz ");
  }
  
  @Test
  public void testParseChildJavaOptsOneSpace() {
    performTest("-Dfoo=bar -Dbaz=biz");
  }
  
  @Test
  public void testParseChildJavaOptsMulitpleSpaces() {
    performTest("-Dfoo=bar  -Dbaz=biz");
  }
  
  @Test
  public void testParseChildJavaOptsOneTab() {
    performTest("-Dfoo=bar\t-Dbaz=biz");
  }
  
  @Test
  public void testParseChildJavaOptsMultipleTabs() {
    performTest("-Dfoo=bar\t\t-Dbaz=biz");
  }
}
