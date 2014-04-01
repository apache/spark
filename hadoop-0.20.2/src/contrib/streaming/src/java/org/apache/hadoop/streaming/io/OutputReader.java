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

package org.apache.hadoop.streaming.io;

import java.io.IOException;

import org.apache.hadoop.streaming.PipeMapRed;

/**
 * Abstract base for classes that read the client's output.
 */
public abstract class OutputReader<K, V> {
  
  /**
   * Initializes the OutputReader. This method has to be called before
   * calling any of the other methods.
   */
  public void initialize(PipeMapRed pipeMapRed) throws IOException {
    // nothing here yet, but that might change in the future
  }
  
  /**
   * Read the next key/value pair outputted by the client.
   * @return true iff a key/value pair was read
   */
  public abstract boolean readKeyValue() throws IOException;
  
  /**
   * Returns the current key.
   */
  public abstract K getCurrentKey() throws IOException;
  
  /**
   * Returns the current value.
   */
  public abstract V getCurrentValue() throws IOException;
  
  /**
   * Returns the last output from the client as a String.
   */
  public abstract String getLastOutput();
  
}
