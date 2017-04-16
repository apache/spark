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

package org.apache.hadoop.mrunit.mapreduce.mock;

import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.util.Progress;
import org.apache.hadoop.mapred.RawKeyValueIterator;

import java.io.IOException;

/**
 * Mock implementation of RawKeyValueIterator that does nothing.
 */
public class MockRawKeyValueIterator implements RawKeyValueIterator {
  public DataInputBuffer getKey() {
    return null;
  }

  public DataInputBuffer getValue() {
    return null;
  }

  public boolean next() {
    return false;
  }

  public void close() {
  }

  public Progress getProgress() {
    return null;
  }
}

