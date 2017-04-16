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
import org.apache.hadoop.util.Progressable;

/** 
 * <code>OutputFormat</code> describes the output-specification for a 
 * Map-Reduce job.
 *
 * <p>The Map-Reduce framework relies on the <code>OutputFormat</code> of the
 * job to:<p>
 * <ol>
 *   <li>
 *   Validate the output-specification of the job. For e.g. check that the 
 *   output directory doesn't already exist. 
 *   <li>
 *   Provide the {@link RecordWriter} implementation to be used to write out
 *   the output files of the job. Output files are stored in a 
 *   {@link FileSystem}.
 *   </li>
 * </ol>
 * 
 * @see RecordWriter
 * @see JobConf
 */
public interface OutputFormat<K, V> {

  /** 
   * Get the {@link RecordWriter} for the given job.
   *
   * @param ignored
   * @param job configuration for the job whose output is being written.
   * @param name the unique name for this part of the output.
   * @param progress mechanism for reporting progress while writing to file.
   * @return a {@link RecordWriter} to write the output for the job.
   * @throws IOException
   */
  RecordWriter<K, V> getRecordWriter(FileSystem ignored, JobConf job,
                                     String name, Progressable progress)
  throws IOException;

  /** 
   * Check for validity of the output-specification for the job.
   *  
   * <p>This is to validate the output specification for the job when it is
   * a job is submitted.  Typically checks that it does not already exist,
   * throwing an exception when it already exists, so that output is not
   * overwritten.</p>
   *
   * @param ignored
   * @param job job configuration.
   * @throws IOException when output should not be attempted
   */
  void checkOutputSpecs(FileSystem ignored, JobConf job) throws IOException;
}

