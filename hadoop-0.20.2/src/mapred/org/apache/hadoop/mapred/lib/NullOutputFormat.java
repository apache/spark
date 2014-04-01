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

package org.apache.hadoop.mapred.lib;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;

/**
 * Consume all outputs and put them in /dev/null. 
 */
public class NullOutputFormat<K, V> implements OutputFormat<K, V> {
  
  public RecordWriter<K, V> getRecordWriter(FileSystem ignored, JobConf job, 
                                      String name, Progressable progress) {
    return new RecordWriter<K, V>(){
        public void write(K key, V value) { }
        public void close(Reporter reporter) { }
      };
  }
  
  public void checkOutputSpecs(FileSystem ignored, JobConf job) { }
}
