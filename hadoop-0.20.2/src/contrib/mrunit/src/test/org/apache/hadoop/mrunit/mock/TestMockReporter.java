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
package org.apache.hadoop.mrunit.mock;

import junit.framework.TestCase;

import org.apache.hadoop.mapred.InputSplit;
import org.junit.Test;


public class TestMockReporter extends TestCase {

  @Test
  public void testGetInputSplitForMapper() {
    InputSplit split = new MockReporter(MockReporter.ReporterType.Mapper, null).getInputSplit();
    assertTrue(null != split);
  }

  // reporter is contractually obligated to throw an exception
  // if the reducer tries to grab the input split.
  @Test
  public void testGetInputSplitForReducer() {
    try {
      new MockReporter(MockReporter.ReporterType.Reducer, null).getInputSplit();
      fail(); // shouldn't get here
    } catch (UnsupportedOperationException uoe) {
      // expected this.
    }
  }
}

