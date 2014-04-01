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
package org.apache.hadoop.mapred;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;

/**
  * InputFormat which simulates the absence of input data
  * by returning zero split.
  */
public class EmptyInputFormat<K, V> implements InputFormat<K, V> {

  public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
    return new InputSplit[0];
  }

  public RecordReader<K, V> getRecordReader(InputSplit split, JobConf job,
                              Reporter reporter) throws IOException {
    return new RecordReader<K,V>() {
      public boolean next(K key, V value) throws IOException { return false; }
      public K createKey() { return null; }
      public V createValue() { return null; }
      public long getPos() throws IOException { return 0L; }
      public void close() throws IOException { }
      public float getProgress() throws IOException { return 0.0f; }
    };
  }
}
