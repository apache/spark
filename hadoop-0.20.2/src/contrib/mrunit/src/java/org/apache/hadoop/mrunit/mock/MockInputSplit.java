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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;

/**
 * A fake InputSplit object to be returned by our MockReporter to the user
 * class.
 */
public class MockInputSplit extends FileSplit implements InputSplit {

  private static final Path MOCK_PATH = new Path("somefile");

  public MockInputSplit() {
    super(MOCK_PATH, 0, 0, (String []) null);
  }

  @Override
  public long getLength() {
    return 0;
  }

  @Override
  public String[] getLocations() throws IOException {
    return null;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
  }

  @Override
  public void write(DataOutput out) throws IOException {
  }

  public String toString() {
    return "MockInputSplit";
  }


  /**
   * Return the path object represented by this as a FileSplit.
   */
  public static Path getMockPath() {
    return MOCK_PATH;
  }
}

