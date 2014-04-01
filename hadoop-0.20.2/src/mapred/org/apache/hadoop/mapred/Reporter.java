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

import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.util.Progressable;

/** 
 * A facility for Map-Reduce applications to report progress and update 
 * counters, status information etc.
 * 
 * <p>{@link Mapper} and {@link Reducer} can use the <code>Reporter</code>
 * provided to report progress or just indicate that they are alive. In 
 * scenarios where the application takes an insignificant amount of time to 
 * process individual key/value pairs, this is crucial since the framework 
 * might assume that the task has timed-out and kill that task.
 *
 * <p>Applications can also update {@link Counters} via the provided 
 * <code>Reporter</code> .</p>
 * 
 * @see Progressable
 * @see Counters
 */
public interface Reporter extends Progressable {
  
  /**
   * A constant of Reporter type that does nothing.
   */
  public static final Reporter NULL = new Reporter() {
      public void setStatus(String s) {
      }
      public void progress() {
      }
      public Counter getCounter(Enum<?> name) {
        return null;
      }
      public Counter getCounter(String group, String name) {
        return null;
      }
      public void incrCounter(Enum<?> key, long amount) {
      }
      public void incrCounter(String group, String counter, long amount) {
      }
      public InputSplit getInputSplit() throws UnsupportedOperationException {
        throw new UnsupportedOperationException("NULL reporter has no input");
      }
    };

  /**
   * Set the status description for the task.
   * 
   * @param status brief description of the current status.
   */
  public abstract void setStatus(String status);
  
  /**
   * Get the {@link Counter} of the given group with the given name.
   * 
   * @param name counter name
   * @return the <code>Counter</code> of the given group/name.
   */
  public abstract Counter getCounter(Enum<?> name);

  /**
   * Get the {@link Counter} of the given group with the given name.
   * 
   * @param group counter group
   * @param name counter name
   * @return the <code>Counter</code> of the given group/name.
   */
  public abstract Counter getCounter(String group, String name);
  
  /**
   * Increments the counter identified by the key, which can be of
   * any {@link Enum} type, by the specified amount.
   * 
   * @param key key to identify the counter to be incremented. The key can be
   *            be any <code>Enum</code>. 
   * @param amount A non-negative amount by which the counter is to 
   *               be incremented.
   */
  public abstract void incrCounter(Enum<?> key, long amount);
  
  /**
   * Increments the counter identified by the group and counter name
   * by the specified amount.
   * 
   * @param group name to identify the group of the counter to be incremented.
   * @param counter name to identify the counter within the group.
   * @param amount A non-negative amount by which the counter is to 
   *               be incremented.
   */
  public abstract void incrCounter(String group, String counter, long amount);
  
  /**
   * Get the {@link InputSplit} object for a map.
   * 
   * @return the <code>InputSplit</code> that the map is reading from.
   * @throws UnsupportedOperationException if called outside a mapper
   */
  public abstract InputSplit getInputSplit() 
    throws UnsupportedOperationException;
}
