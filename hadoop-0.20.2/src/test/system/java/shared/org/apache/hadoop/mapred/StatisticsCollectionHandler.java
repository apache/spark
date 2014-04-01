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

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/*
 * Object which stores all the tasktracker info
 */
public class StatisticsCollectionHandler implements Writable{

  private int sinceStartTotalTasks = 0;
  private int sinceStartSucceededTasks = 0;
  private int lastHourTotalTasks = 0;
  private int lastHourSucceededTasks = 0;
  private int lastDayTotalTasks = 0;
  private int lastDaySucceededTasks = 0;

  public int getSinceStartTotalTasks() {
    return sinceStartTotalTasks;
  }

  public int getSinceStartSucceededTasks() {
    return sinceStartSucceededTasks;
  }

  public int getLastHourTotalTasks() {
    return lastHourTotalTasks;
  }

  public int getLastHourSucceededTasks() {
    return lastHourSucceededTasks;
  }

  public int getLastDayTotalTasks() {
    return lastDayTotalTasks;
  }

  public int getLastDaySucceededTasks() {
    return lastDaySucceededTasks;
  }

  public void setSinceStartTotalTasks(int value) {
    sinceStartTotalTasks = value;
  }

  public void setSinceStartSucceededTasks(int value) {
    sinceStartSucceededTasks = value;
  }

  public void setLastHourTotalTasks(int value) {
    lastHourTotalTasks = value;
  }

  public void setLastHourSucceededTasks(int value) {
    lastHourSucceededTasks = value;
  }

  public void setLastDayTotalTasks(int value) {
    lastDayTotalTasks = value;
  }

  public void setLastDaySucceededTasks(int value) {
    lastDaySucceededTasks = value;
  }
  
  @Override
  public void readFields(DataInput in) throws IOException {
    sinceStartTotalTasks = WritableUtils.readVInt(in);
    sinceStartSucceededTasks = WritableUtils.readVInt(in);
    lastHourTotalTasks = WritableUtils.readVInt(in);
    lastHourSucceededTasks = WritableUtils.readVInt(in);
    lastDayTotalTasks = WritableUtils.readVInt(in);
    lastDaySucceededTasks = WritableUtils.readVInt(in);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    WritableUtils.writeVInt(out, sinceStartTotalTasks);
    WritableUtils.writeVInt(out, sinceStartSucceededTasks);
    WritableUtils.writeVInt(out, lastHourTotalTasks);
    WritableUtils.writeVInt(out, lastHourSucceededTasks);
    WritableUtils.writeVInt(out, lastDayTotalTasks);
    WritableUtils.writeVInt(out, lastDaySucceededTasks);
  }
  
}

