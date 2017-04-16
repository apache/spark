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

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * A Convenience class that creates output lazily.  
 */
public class LazyOutputFormat <K,V> extends FilterOutputFormat<K, V> {
  public static String OUTPUT_FORMAT = 
    "mapreduce.output.lazyoutputformat.outputformat";
  /**
   * Set the underlying output format for LazyOutputFormat.
   * @param job the {@link Job} to modify
   * @param theClass the underlying class
   */
  @SuppressWarnings("unchecked")
  public static void  setOutputFormatClass(Job job, 
                                     Class<? extends OutputFormat> theClass) {
      job.setOutputFormatClass(LazyOutputFormat.class);
      job.getConfiguration().setClass(OUTPUT_FORMAT, 
          theClass, OutputFormat.class);
  }

  @SuppressWarnings("unchecked")
  private void getBaseOutputFormat(Configuration conf) 
  throws IOException {
    baseOut =  ((OutputFormat<K, V>) ReflectionUtils.newInstance(
      conf.getClass(OUTPUT_FORMAT, null), conf));
    if (baseOut == null) {
      throw new IOException("Output Format not set for LazyOutputFormat");
    }
  }

  @Override
  public RecordWriter<K, V> getRecordWriter(TaskAttemptContext context)
  throws IOException, InterruptedException {
    if (baseOut == null) {
      getBaseOutputFormat(context.getConfiguration());
    }
    return new LazyRecordWriter<K, V>(baseOut, context);
  }
  
  @Override
  public void checkOutputSpecs(JobContext context) 
  throws IOException, InterruptedException {
    if (baseOut == null) {
      getBaseOutputFormat(context.getConfiguration());
    }
   super.checkOutputSpecs(context);
  }
  
  @Override
  public OutputCommitter getOutputCommitter(TaskAttemptContext context) 
  throws IOException, InterruptedException {
    if (baseOut == null) {
      getBaseOutputFormat(context.getConfiguration());
    }
    return super.getOutputCommitter(context);
  }
  
  /**
   * A convenience class to be used with LazyOutputFormat
   */
  private static class LazyRecordWriter<K,V> extends FilterRecordWriter<K,V> {

    final OutputFormat<K,V> outputFormat;
    final TaskAttemptContext taskContext;

    public LazyRecordWriter(OutputFormat<K,V> out, 
                            TaskAttemptContext taskContext)
    throws IOException, InterruptedException {
      this.outputFormat = out;
      this.taskContext = taskContext;
    }

    @Override
    public void write(K key, V value) throws IOException, InterruptedException {
      if (rawWriter == null) {
        rawWriter = outputFormat.getRecordWriter(taskContext);
      }
      rawWriter.write(key, value);
    }

    @Override
    public void close(TaskAttemptContext context) 
    throws IOException, InterruptedException {
      if (rawWriter != null) {
        rawWriter.close(context);
      }
    }

  }
}
