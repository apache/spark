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
package org.apache.hadoop.mapreduce.server.tasktracker.userlogs;

import org.apache.hadoop.mapred.TaskTracker;

/**
 * A directive from the various components of {@link TaskTracker} to the
 * {@link UserLogManager} to inform about an event.
 */
public abstract class UserLogEvent {
  
  public enum EventType {
    JVM_FINISHED,
    JOB_STARTED,
    JOB_COMPLETED,
    DELETE_JOB,
  };

  private EventType eventType;
  
  protected UserLogEvent(EventType eventType) {
    this.eventType = eventType;
  }
  
  /**
   * Return the {@link EventType}.
   * @return the {@link EventType}.
   */
  public EventType getEventType() {
    return eventType;
  }

}
