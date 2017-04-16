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

import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.StatusReporter;

public class MockReporter extends StatusReporter {

  private Counters counters;

  public MockReporter(final Counters ctrs) {
    this.counters = ctrs;
  }

  @Override
  public void setStatus(String status) {
    // do nothing.
  }

  @Override
  public void progress() {
    // do nothing.
  }

  @Override
  public Counter getCounter(String group, String name) {
    Counter counter = null;
    if (counters != null) {
      counter = counters.findCounter(group, name);
    }

    return counter;
  }

  @Override
  public Counter getCounter(Enum key) {
    Counter counter = null;
    if (counters != null) {
      counter = counters.findCounter(key);
    }

    return counter;
  }
}

