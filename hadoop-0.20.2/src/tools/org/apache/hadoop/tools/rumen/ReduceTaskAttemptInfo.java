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

import org.apache.hadoop.mapred.TaskStatus.State;

/**
 * {@link ReduceTaskAttemptInfo} represents the information with regard to a
 * reduce task attempt.
 */
public class ReduceTaskAttemptInfo extends TaskAttemptInfo {
  private long shuffleTime;
  private long mergeTime;
  private long reduceTime;

  public ReduceTaskAttemptInfo(State state, TaskInfo taskInfo, long shuffleTime,
      long mergeTime, long reduceTime) {
    super(state, taskInfo);
    this.shuffleTime = shuffleTime;
    this.mergeTime = mergeTime;
    this.reduceTime = reduceTime;
  }

  /**
   * Get the runtime for the <b>reduce</b> phase of the reduce task-attempt.
   * 
   * @return the runtime for the <b>reduce</b> phase of the reduce task-attempt
   */
  public long getReduceRuntime() {
    return reduceTime;
  }

  /**
   * Get the runtime for the <b>shuffle</b> phase of the reduce task-attempt.
   * 
   * @return the runtime for the <b>shuffle</b> phase of the reduce task-attempt
   */
  public long getShuffleRuntime() {
    return shuffleTime;
  }

  /**
   * Get the runtime for the <b>merge</b> phase of the reduce task-attempt
   * 
   * @return the runtime for the <b>merge</b> phase of the reduce task-attempt
   */
  public long getMergeRuntime() {
    return mergeTime;
  }

  @Override
  public long getRuntime() {
    return (getShuffleRuntime() + getMergeRuntime() + getReduceRuntime());
  }

}
