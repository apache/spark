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
package org.apache.hadoop.tools.rumen;

public class TaskInfo {
  private final long bytesIn;
  private final int recsIn;
  private final long bytesOut;
  private final int recsOut;
  private final long maxMemory;

  public TaskInfo(long bytesIn, int recsIn, long bytesOut, int recsOut,
      long maxMemory) {
    this.bytesIn = bytesIn;
    this.recsIn = recsIn;
    this.bytesOut = bytesOut;
    this.recsOut = recsOut;
    this.maxMemory = maxMemory;
  }

  /**
   * @return Raw bytes read from the FileSystem into the task. Note that this
   *         may not always match the input bytes to the task.
   */
  public long getInputBytes() {
    return bytesIn;
  }

  /**
   * @return Number of records input to this task.
   */
  public int getInputRecords() {
    return recsIn;
  }

  /**
   * @return Raw bytes written to the destination FileSystem. Note that this may
   *         not match output bytes.
   */
  public long getOutputBytes() {
    return bytesOut;
  }

  /**
   * @return Number of records output from this task.
   */
  public int getOutputRecords() {
    return recsOut;
  }

  /**
   * @return Memory used by the task leq the heap size.
   */
  public long getTaskMemory() {
    return maxMemory;
  }

}
