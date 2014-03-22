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

import com.google.common.io.Closeables;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * Reads an entire file out in (filename, content) format. Each element in split is an record of a
 * unique, whole file. File name is full path name for easy deduplicate.
 */
public class WholeTextFileRecordReader extends RecordReader<String, Text> {
  private Path path;

  private String key = null;
  private Text value = null;

  private boolean processed = false;

  private FileSystem fs;

  public WholeTextFileRecordReader(
      CombineFileSplit split,
      TaskAttemptContext context,
      Integer index)
    throws IOException {
    path = split.getPath(index);
    fs = path.getFileSystem(context.getConfiguration());
  }

  @Override
  public void initialize(InputSplit arg0, TaskAttemptContext arg1)
    throws IOException, InterruptedException {
  }

  @Override
  public void close() throws IOException {
  }

  @Override
  public float getProgress() throws IOException {
    return processed ? 1.0f : 0.0f;
  }

  @Override
  public String getCurrentKey() throws IOException, InterruptedException {
    return key;
  }

  @Override
  public Text getCurrentValue() throws IOException, InterruptedException{
    return value;
  }

  @Override
  public boolean nextKeyValue() throws IOException {
    if (!processed) {
      if (key == null) {
        key = path.toString();
      }
      if (value == null) {
        value = new Text();
      }

      FSDataInputStream fileIn = null;
      try {
        fileIn = fs.open(path);
        byte[] innerBuffer = IOUtils.toByteArray(fileIn);
        value.set(innerBuffer, 0, innerBuffer.length);
      } finally {
        Closeables.close(fileIn, false);
      }
      processed = true;
      return true;
    }
    return false;
  }
}
