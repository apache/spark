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

package org.apache.hadoop.contrib.failmon;

/**********************************************************
 * Represents objects that monitor specific hardware resources and
 * can query them to get EventRecords describing the state of these
 * resources.
 *
 **********************************************************/

public interface Monitored {
  /**
   * Get an array of all EventRecords that can be extracted for
   * the represented hardware component.
   * 
   * @return The array of EventRecords
   */
  public EventRecord[] monitor();
  
  /**
   * Inserts all EventRecords that can be extracted for
   * the represented hardware component into a LocalStore.
   * 
   * @param ls the LocalStore into which the EventRecords 
   * are to be stored.
   */
  public void monitor(LocalStore ls);
  
  /**
   * Return a String with information about the implementing
   * class 
   * 
   * @return A String describing the implementing class
   */
  public String getInfo();
}
