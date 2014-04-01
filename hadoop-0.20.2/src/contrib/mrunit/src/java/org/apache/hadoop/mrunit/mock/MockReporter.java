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

import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.Counters.Counter;

public class MockReporter implements Reporter {

  private MockInputSplit inputSplit = new MockInputSplit();
  private Counters counters;

  public enum ReporterType {
    Mapper,
    Reducer
  }

  private ReporterType typ;

  public MockReporter(final ReporterType kind, final Counters ctrs) {
    this.typ = kind;
    this.counters = ctrs;
  }

  @Override
  public InputSplit getInputSplit() {
    if (typ == ReporterType.Reducer) {
      throw new UnsupportedOperationException(
              "Reducer cannot call getInputSplit()");
    } else {
      return inputSplit;
    }
  }

  @Override
  public void incrCounter(Enum key, long amount) {
    if (null != counters) {
      counters.incrCounter(key, amount);
    }
  }

  @Override
  public void incrCounter(String group, String counter, long amount) {
    if (null != counters) {
      counters.incrCounter(group, counter, amount);
    }
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
    Counters.Counter counter = null;
    if (counters != null) {
      counter = counters.findCounter(group, name);
    }

    return counter;
  }

  @Override
  public Counter getCounter(Enum key) {
    Counters.Counter counter = null;
    if (counters != null) {
      counter = counters.findCounter(key);
    }

    return counter;
  }
}

