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

package org.apache.hadoop.mapreduce;

import java.io.IOException;

import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;

/**
 * <code>InputSplit</code> represents the data to be processed by an 
 * individual {@link Mapper}. 
 *
 * <p>Typically, it presents a byte-oriented view on the input and is the 
 * responsibility of {@link RecordReader} of the job to process this and present
 * a record-oriented view.
 * 
 * @see InputFormat
 * @see RecordReader
 */
public abstract class InputSplit {
  /**
   * Get the size of the split, so that the input splits can be sorted by size.
   * @return the number of bytes in the split
   * @throws IOException
   * @throws InterruptedException
   */
  public abstract long getLength() throws IOException, InterruptedException;

  /**
   * Get the list of nodes by name where the data for the split would be local.
   * The locations do not need to be serialized.
   * @return a new array of the node nodes.
   * @throws IOException
   * @throws InterruptedException
   */
  public abstract 
    String[] getLocations() throws IOException, InterruptedException;
}