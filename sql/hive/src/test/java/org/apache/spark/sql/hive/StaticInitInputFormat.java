/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.hive;

import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

/**
 * Test-only {@link InputFormat} whose static initializer flips
 * {@link StaticInitFlags#inputFormatInitialized}. It lets a test verify whether resolving the
 * format class by name runs its static initializer. The reader methods are never called.
 */
public class StaticInitInputFormat implements InputFormat<Void, Void> {
  static {
    StaticInitFlags.inputFormatInitialized = true;
  }

  @Override
  public InputSplit[] getSplits(JobConf job, int numSplits) {
    throw new UnsupportedOperationException();
  }

  @Override
  public RecordReader<Void, Void> getRecordReader(
      InputSplit split, JobConf job, Reporter reporter) {
    throw new UnsupportedOperationException();
  }
}
