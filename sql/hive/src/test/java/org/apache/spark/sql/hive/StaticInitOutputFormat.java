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

import java.util.Properties;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.util.Progressable;

/**
 * Test-only {@link HiveOutputFormat} whose static initializer flips
 * {@link StaticInitFlags#outputFormatInitialized}. It lets a test verify whether resolving the
 * format class by name runs its static initializer. The writer methods are never called.
 */
public class StaticInitOutputFormat implements HiveOutputFormat<Void, Void> {
  static {
    StaticInitFlags.outputFormatInitialized = true;
  }

  @Override
  public FileSinkOperator.RecordWriter getHiveRecordWriter(
      JobConf jc,
      Path finalOutPath,
      Class<? extends Writable> valueClass,
      boolean isCompressed,
      Properties tableProperties,
      Progressable progress) {
    throw new UnsupportedOperationException();
  }

  @Override
  public RecordWriter<Void, Void> getRecordWriter(
      FileSystem ignored, JobConf job, String name, Progressable progress) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void checkOutputSpecs(FileSystem ignored, JobConf job) {
    throw new UnsupportedOperationException();
  }
}
