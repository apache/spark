/*
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
import org.apache.hadoop.io.Writable;

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
public interface InputSplit extends Writable {

  /**
   * Get the total number of bytes in the data of the <code>InputSplit</code>.
   * 
   * @return the number of bytes in the input split.
   * @throws IOException
   */
  long getLength() throws IOException;
  
  /**
   * Get the list of hostnames where the input split is located.
   * 
   * @return list of hostnames where data of the <code>InputSplit</code> is
   *         located as an array of <code>String</code>s.
   * @throws IOException
   */
  String[] getLocations() throws IOException;
}
