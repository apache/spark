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

package org.apache.spark.mllib.input;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * The specific InputFormat reads files in HDFS or local disk. It will be called by
 * HadoopRDD to generate new WholeTextFileRecordReader.
 */
public class WholeTextFileInputFormat
  extends CombineFileInputFormat<String, Text> {

  @Override
  protected boolean isSplitable(JobContext context, Path file) {
    return false;
  }
  @Override
  public RecordReader<String, Text> createRecordReader(
      InputSplit split,
      TaskAttemptContext context) throws IOException {
    return new CombineFileRecordReader<String, Text>(
      (CombineFileSplit)split,
      context,
      (Class) WholeTextFileRecordReader.class);
  }
}
