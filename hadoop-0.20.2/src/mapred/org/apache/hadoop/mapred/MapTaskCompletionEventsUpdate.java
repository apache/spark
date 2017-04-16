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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * A class that represents the communication between the tasktracker and child
 * tasks w.r.t the map task completion events. It also indicates whether the 
 * child task should reset its events index.
 */
public class MapTaskCompletionEventsUpdate implements Writable {
  TaskCompletionEvent[] events;
  boolean reset;

  public MapTaskCompletionEventsUpdate() { }

  public MapTaskCompletionEventsUpdate(TaskCompletionEvent[] events,
      boolean reset) {
    this.events = events;
    this.reset = reset;
  }

  public boolean shouldReset() {
    return reset;
  }

  public TaskCompletionEvent[] getMapTaskCompletionEvents() {
    return events;
  }

  public void write(DataOutput out) throws IOException {
    out.writeBoolean(reset);
    out.writeInt(events.length);
    for (TaskCompletionEvent event : events) {
      event.write(out);
    }
  }

  public void readFields(DataInput in) throws IOException {
    reset = in.readBoolean();
    events = new TaskCompletionEvent[in.readInt()];
    for (int i = 0; i < events.length; ++i) {
      events[i] = new TaskCompletionEvent();
      events[i].readFields(in);
    }
  }
}
