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

package org.apache.hadoop.test.system;

import java.util.Map;

import org.apache.hadoop.io.Writable;

/**
 * Daemon system level process information.
 */
public interface ProcessInfo extends Writable {
  /**
   * Get the current time in the millisecond.<br/>
   * 
   * @return current time on daemon clock in millisecond.
   */
  public long currentTimeMillis();

  /**
   * Get the environment that was used to start the Daemon process.<br/>
   * 
   * @return the environment variable list.
   */
  public Map<String,String> getEnv();

  /**
   * Get the System properties of the Daemon process.<br/>
   * 
   * @return the properties list.
   */
  public Map<String,String> getSystemProperties();

  /**
   * Get the number of active threads in Daemon VM.<br/>
   * 
   * @return number of active threads in Daemon VM.
   */
  public int activeThreadCount();

  /**
   * Get the maximum heap size that is configured for the Daemon VM. <br/>
   * 
   * @return maximum heap size.
   */
  public long maxMemory();

  /**
   * Get the free memory in Daemon VM.<br/>
   * 
   * @return free memory.
   */
  public long freeMemory();

  /**
   * Get the total used memory in Demon VM. <br/>
   * 
   * @return total used memory.
   */
  public long totalMemory();
}