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

package org.apache.hadoop.mapreduce.lib.output;

import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * Consume all outputs and put them in /dev/null. 
 */
public class NullOutputFormat<K, V> extends OutputFormat<K, V> {
  
  @Override
  public RecordWriter<K, V> 
         getRecordWriter(TaskAttemptContext context) {
    return new RecordWriter<K, V>(){
        public void write(K key, V value) { }
        public void close(TaskAttemptContext context) { }
      };
  }
  
  @Override
  public void checkOutputSpecs(JobContext context) { }
  
  @Override
  public OutputCommitter getOutputCommitter(TaskAttemptContext context) {
    return new OutputCommitter() {
      public void abortTask(TaskAttemptContext taskContext) { }
      public void cleanupJob(JobContext jobContext) { }
      public void commitJob(JobContext jobContext) { }
      public void commitTask(TaskAttemptContext taskContext) { }
      public boolean needsTaskCommit(TaskAttemptContext taskContext) {
        return false;
      }
      public void setupJob(JobContext jobContext) { }
      public void setupTask(TaskAttemptContext taskContext) { }
    };
  }
}
