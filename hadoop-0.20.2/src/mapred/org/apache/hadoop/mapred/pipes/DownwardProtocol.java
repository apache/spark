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

package org.apache.hadoop.mapred.pipes;

import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;

/**
 * The abstract description of the downward (from Java to C++) Pipes protocol.
 * All of these calls are asynchronous and return before the message has been 
 * processed.
 */
interface DownwardProtocol<K extends WritableComparable, V extends Writable> {
  /**
   * request authentication
   * @throws IOException
   */
  void authenticate(String digest, String challenge) throws IOException;
  
  /**
   * Start communication
   * @throws IOException
   */
  void start() throws IOException;
  
  /**
   * Set the JobConf for the task.
   * @param conf
   * @throws IOException
   */
  void setJobConf(JobConf conf) throws IOException;
  
  /**
   * Set the input types for Maps.
   * @param keyType the name of the key's type
   * @param valueType the name of the value's type
   * @throws IOException
   */
  void setInputTypes(String keyType, String valueType) throws IOException;
  
  /**
   * Run a map task in the child.
   * @param split The input split for this map.
   * @param numReduces The number of reduces for this job.
   * @param pipedInput Is the input coming from Java?
   * @throws IOException
   */
  void runMap(InputSplit split, int numReduces, 
              boolean pipedInput) throws IOException;
  
  /**
   * For maps with pipedInput, the key/value pairs are sent via this messaage.
   * @param key The record's key
   * @param value The record's value
   * @throws IOException
   */
  void mapItem(K key, V value) throws IOException;
  
  /**
   * Run a reduce task in the child
   * @param reduce the index of the reduce (0 .. numReduces - 1)
   * @param pipedOutput is the output being sent to Java?
   * @throws IOException
   */
  void runReduce(int reduce, boolean pipedOutput) throws IOException;
  
  /**
   * The reduce should be given a new key
   * @param key the new key
   * @throws IOException
   */
  void reduceKey(K key) throws IOException;
  
  /**
   * The reduce should be given a new value
   * @param value the new value
   * @throws IOException
   */
  void reduceValue(V value) throws IOException;
  
  /**
   * The task has no more input coming, but it should finish processing it's 
   * input.
   * @throws IOException
   */
  void endOfInput() throws IOException;
  
  /**
   * The task should stop as soon as possible, because something has gone wrong.
   * @throws IOException
   */
  void abort() throws IOException;
  
  /**
   * Flush the data through any buffers.
   */
  void flush() throws IOException;
  
  /**
   * Close the connection.
   */
  void close() throws IOException, InterruptedException;
}
