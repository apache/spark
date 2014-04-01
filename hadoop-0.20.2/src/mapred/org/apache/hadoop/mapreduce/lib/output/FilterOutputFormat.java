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

import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * FilterOutputFormat is a convenience class that wraps OutputFormat. 
 */
public class FilterOutputFormat <K,V> extends OutputFormat<K, V> {

  protected OutputFormat<K,V> baseOut;

  public FilterOutputFormat() {
    this.baseOut = null;
  }
  
  /**
   * Create a FilterOutputFormat based on the underlying output format.
   * @param baseOut the underlying OutputFormat
   */
  public FilterOutputFormat(OutputFormat<K,V> baseOut) {
    this.baseOut = baseOut;
  }

  @Override
  public RecordWriter<K, V> getRecordWriter(TaskAttemptContext context) 
  throws IOException, InterruptedException {
    return getBaseOut().getRecordWriter(context);
  }

  @Override
  public void checkOutputSpecs(JobContext context) 
  throws IOException, InterruptedException {
    getBaseOut().checkOutputSpecs(context);
  }

  @Override
  public OutputCommitter getOutputCommitter(TaskAttemptContext context) 
  throws IOException, InterruptedException {
    return getBaseOut().getOutputCommitter(context);
  }

  private OutputFormat<K,V> getBaseOut() throws IOException {
    if (baseOut == null) {
      throw new IOException("OutputFormat not set for FilterOutputFormat");
    }
    return baseOut;
  }
  /**
   * <code>FilterRecordWriter</code> is a convenience wrapper
   * class that extends the {@link RecordWriter}.
   */

  public static class FilterRecordWriter<K,V> extends RecordWriter<K,V> {

    protected RecordWriter<K,V> rawWriter = null;

    public FilterRecordWriter() {
      rawWriter = null;
    }
    
    public FilterRecordWriter(RecordWriter<K,V> rwriter) {
      this.rawWriter = rwriter;
    }
    
    @Override
    public void write(K key, V value) throws IOException, InterruptedException {
      getRawWriter().write(key, value);
    }

    @Override
    public void close(TaskAttemptContext context) 
    throws IOException, InterruptedException {
      getRawWriter().close(context);
    }
    
    private RecordWriter<K,V> getRawWriter() throws IOException {
      if (rawWriter == null) {
        throw new IOException("Record Writer not set for FilterRecordWriter");
      }
      return rawWriter;
    }
  }
}
