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

/**
 * Expert: Generic interface for {@link Mapper}s.
 * 
 * <p>Custom implementations of <code>MapRunnable</code> can exert greater 
 * control on map processing e.g. multi-threaded, asynchronous mappers etc.</p>
 * 
 * @see Mapper
 */
public interface MapRunnable<K1, V1, K2, V2>
    extends JobConfigurable {
  
  /** 
   * Start mapping input <tt>&lt;key, value&gt;</tt> pairs.
   *  
   * <p>Mapping of input records to output records is complete when this method 
   * returns.</p>
   * 
   * @param input the {@link RecordReader} to read the input records.
   * @param output the {@link OutputCollector} to collect the outputrecords.
   * @param reporter {@link Reporter} to report progress, status-updates etc.
   * @throws IOException
   */
  void run(RecordReader<K1, V1> input, OutputCollector<K2, V2> output,
           Reporter reporter)
    throws IOException;
}
