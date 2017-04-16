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
package org.apache.hadoop.mapreduce.lib.input;

import java.io.IOException;

import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * This is a delegating RecordReader, which delegates the functionality to the
 * underlying record reader in {@link TaggedInputSplit}  
 */
public class DelegatingRecordReader<K, V> extends RecordReader<K, V> {
  RecordReader<K, V> originalRR;

  /**
   * Constructs the DelegatingRecordReader.
   * 
   * @param split TaggegInputSplit object
   * @param context TaskAttemptContext object
   *  
   * @throws IOException
   * @throws InterruptedException
   */
  @SuppressWarnings("unchecked")
  public DelegatingRecordReader(InputSplit split, TaskAttemptContext context)
      throws IOException, InterruptedException {
    // Find the InputFormat and then the RecordReader from the
    // TaggedInputSplit.
    TaggedInputSplit taggedInputSplit = (TaggedInputSplit) split;
    InputFormat<K, V> inputFormat = (InputFormat<K, V>) ReflectionUtils
        .newInstance(taggedInputSplit.getInputFormatClass(), context
            .getConfiguration());
    originalRR = inputFormat.createRecordReader(taggedInputSplit
        .getInputSplit(), context);
  }

  @Override
  public void close() throws IOException {
    originalRR.close();
  }

  @Override
  public K getCurrentKey() throws IOException, InterruptedException {
    return originalRR.getCurrentKey();
  }

  @Override
  public V getCurrentValue() throws IOException, InterruptedException {
    return originalRR.getCurrentValue();
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    return originalRR.getProgress();
  }

  @Override
  public void initialize(InputSplit split, TaskAttemptContext context)
      throws IOException, InterruptedException {
    originalRR.initialize(((TaggedInputSplit) split).getInputSplit(), context);
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    return originalRR.nextKeyValue();
  }

}
