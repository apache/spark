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
 * <code>InputFormat</code> describes the input-specification for a 
 * Map-Reduce job. 
 * 
 * <p>The Map-Reduce framework relies on the <code>InputFormat</code> of the
 * job to:<p>
 * <ol>
 *   <li>
 *   Validate the input-specification of the job. 
 *   <li>
 *   Split-up the input file(s) into logical {@link InputSplit}s, each of 
 *   which is then assigned to an individual {@link Mapper}.
 *   </li>
 *   <li>
 *   Provide the {@link RecordReader} implementation to be used to glean
 *   input records from the logical <code>InputSplit</code> for processing by 
 *   the {@link Mapper}.
 *   </li>
 * </ol>
 * 
 * <p>The default behavior of file-based {@link InputFormat}s, typically 
 * sub-classes of {@link FileInputFormat}, is to split the 
 * input into <i>logical</i> {@link InputSplit}s based on the total size, in 
 * bytes, of the input files. However, the {@link FileSystem} blocksize of  
 * the input files is treated as an upper bound for input splits. A lower bound 
 * on the split size can be set via 
 * <a href="{@docRoot}/../mapred-default.html#mapred.min.split.size">
 * mapred.min.split.size</a>.</p>
 * 
 * <p>Clearly, logical splits based on input-size is insufficient for many 
 * applications since record boundaries are to respected. In such cases, the
 * application has to also implement a {@link RecordReader} on whom lies the
 * responsibilty to respect record-boundaries and present a record-oriented
 * view of the logical <code>InputSplit</code> to the individual task.
 *
 * @see InputSplit
 * @see RecordReader
 * @see JobClient
 * @see FileInputFormat
 */
public interface InputFormat<K, V> {

  /** 
   * Logically split the set of input files for the job.  
   * 
   * <p>Each {@link InputSplit} is then assigned to an individual {@link Mapper}
   * for processing.</p>
   *
   * <p><i>Note</i>: The split is a <i>logical</i> split of the inputs and the
   * input files are not physically split into chunks. For e.g. a split could
   * be <i>&lt;input-file-path, start, offset&gt;</i> tuple.
   * 
   * @param job job configuration.
   * @param numSplits the desired number of splits, a hint.
   * @return an array of {@link InputSplit}s for the job.
   */
  InputSplit[] getSplits(JobConf job, int numSplits) throws IOException;

  /** 
   * Get the {@link RecordReader} for the given {@link InputSplit}.
   *
   * <p>It is the responsibility of the <code>RecordReader</code> to respect
   * record boundaries while processing the logical split to present a 
   * record-oriented view to the individual task.</p>
   * 
   * @param split the {@link InputSplit}
   * @param job the job that this split belongs to
   * @return a {@link RecordReader}
   */
  RecordReader<K, V> getRecordReader(InputSplit split,
                                     JobConf job, 
                                     Reporter reporter) throws IOException;
}

