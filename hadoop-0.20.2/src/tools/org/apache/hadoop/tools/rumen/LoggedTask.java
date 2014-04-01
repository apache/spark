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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.codehaus.jackson.annotate.JsonAnySetter;

/**
 * A {@link LoggedTask} represents a [hadoop] task that is part of a hadoop job.
 * It knows about the [pssibly empty] sequence of attempts, its I/O footprint,
 * and its runtime.
 * 
 * All of the public methods are simply accessors for the instance variables we
 * want to write out in the JSON files.
 * 
 */
public class LoggedTask implements DeepCompare {
  long inputBytes = -1L;
  long inputRecords = -1L;
  long outputBytes = -1L;
  long outputRecords = -1L;
  String taskID;
  long startTime = -1L;
  long finishTime = -1L;
  Pre21JobHistoryConstants.Values taskType;
  Pre21JobHistoryConstants.Values taskStatus;
  List<LoggedTaskAttempt> attempts = new ArrayList<LoggedTaskAttempt>();
  List<LoggedLocation> preferredLocations = Collections.emptyList();

  int numberMaps = -1;
  int numberReduces = -1;

  static private Set<String> alreadySeenAnySetterAttributes =
      new TreeSet<String>();

  @SuppressWarnings("unused")
  // for input parameter ignored.
  @JsonAnySetter
  public void setUnknownAttribute(String attributeName, Object ignored) {
    if (!alreadySeenAnySetterAttributes.contains(attributeName)) {
      alreadySeenAnySetterAttributes.add(attributeName);
      System.err.println("In LoggedJob, we saw the unknown attribute "
          + attributeName + ".");
    }
  }

  LoggedTask() {
    super();
  }

  public long getInputBytes() {
    return inputBytes;
  }

  void setInputBytes(long inputBytes) {
    this.inputBytes = inputBytes;
  }

  public long getInputRecords() {
    return inputRecords;
  }

  void setInputRecords(long inputRecords) {
    this.inputRecords = inputRecords;
  }

  public long getOutputBytes() {
    return outputBytes;
  }

  void setOutputBytes(long outputBytes) {
    this.outputBytes = outputBytes;
  }

  public long getOutputRecords() {
    return outputRecords;
  }

  void setOutputRecords(long outputRecords) {
    this.outputRecords = outputRecords;
  }

  public String getTaskID() {
    return taskID;
  }

  void setTaskID(String taskID) {
    this.taskID = taskID;
  }

  public long getStartTime() {
    return startTime;
  }

  void setStartTime(long startTime) {
    this.startTime = startTime;
  }

  public long getFinishTime() {
    return finishTime;
  }

  void setFinishTime(long finishTime) {
    this.finishTime = finishTime;
  }

  public List<LoggedTaskAttempt> getAttempts() {
    return attempts;
  }

  void setAttempts(List<LoggedTaskAttempt> attempts) {
    if (attempts == null) {
      this.attempts = new ArrayList<LoggedTaskAttempt>();
    } else {
      this.attempts = attempts;
    }
  }

  public List<LoggedLocation> getPreferredLocations() {
    return preferredLocations;
  }

  void setPreferredLocations(List<LoggedLocation> preferredLocations) {
    if (preferredLocations == null || preferredLocations.isEmpty()) {
      this.preferredLocations = Collections.emptyList();
    } else {
      this.preferredLocations = preferredLocations;
    }
  }

  public int getNumberMaps() {
    return numberMaps;
  }

  void setNumberMaps(int numberMaps) {
    this.numberMaps = numberMaps;
  }

  public int getNumberReduces() {
    return numberReduces;
  }

  void setNumberReduces(int numberReduces) {
    this.numberReduces = numberReduces;
  }

  public Pre21JobHistoryConstants.Values getTaskStatus() {
    return taskStatus;
  }

  void setTaskStatus(Pre21JobHistoryConstants.Values taskStatus) {
    this.taskStatus = taskStatus;
  }

  public Pre21JobHistoryConstants.Values getTaskType() {
    return taskType;
  }

  void setTaskType(Pre21JobHistoryConstants.Values taskType) {
    this.taskType = taskType;
  }

  private void compare1(long c1, long c2, TreePath loc, String eltname)
      throws DeepInequalityException {
    if (c1 != c2) {
      throw new DeepInequalityException(eltname + " miscompared", new TreePath(
          loc, eltname));
    }
  }

  private void compare1(String c1, String c2, TreePath loc, String eltname)
      throws DeepInequalityException {
    if (c1 == null && c2 == null) {
      return;
    }
    if (c1 == null || c2 == null || !c1.equals(c2)) {
      throw new DeepInequalityException(eltname + " miscompared", new TreePath(
          loc, eltname));
    }
  }

  private void compare1(Pre21JobHistoryConstants.Values c1,
      Pre21JobHistoryConstants.Values c2, TreePath loc, String eltname)
      throws DeepInequalityException {
    if (c1 == null && c2 == null) {
      return;
    }
    if (c1 == null || c2 == null || !c1.equals(c2)) {
      throw new DeepInequalityException(eltname + " miscompared", new TreePath(
          loc, eltname));
    }
  }

  private void compareLoggedLocations(List<LoggedLocation> c1,
      List<LoggedLocation> c2, TreePath loc, String eltname)
      throws DeepInequalityException {
    if (c1 == null && c2 == null) {
      return;
    }

    if (c1 == null || c2 == null || c1.size() != c2.size()) {
      throw new DeepInequalityException(eltname + " miscompared", new TreePath(
          loc, eltname));
    }

    for (int i = 0; i < c1.size(); ++i) {
      c1.get(i).deepCompare(c2.get(i), new TreePath(loc, eltname, i));
    }
  }

  private void compareLoggedTaskAttempts(List<LoggedTaskAttempt> c1,
      List<LoggedTaskAttempt> c2, TreePath loc, String eltname)
      throws DeepInequalityException {
    if (c1 == null && c2 == null) {
      return;
    }

    if (c1 == null || c2 == null || c1.size() != c2.size()) {
      throw new DeepInequalityException(eltname + " miscompared", new TreePath(
          loc, eltname));
    }

    for (int i = 0; i < c1.size(); ++i) {
      c1.get(i).deepCompare(c2.get(i), new TreePath(loc, eltname, i));
    }
  }

  public void deepCompare(DeepCompare comparand, TreePath loc)
      throws DeepInequalityException {
    if (!(comparand instanceof LoggedTask)) {
      throw new DeepInequalityException("comparand has wrong type", loc);
    }

    LoggedTask other = (LoggedTask) comparand;

    compare1(inputBytes, other.inputBytes, loc, "inputBytes");
    compare1(inputRecords, other.inputRecords, loc, "inputRecords");
    compare1(outputBytes, other.outputBytes, loc, "outputBytes");
    compare1(outputRecords, other.outputRecords, loc, "outputRecords");

    compare1(taskID, other.taskID, loc, "taskID");

    compare1(startTime, other.startTime, loc, "startTime");
    compare1(finishTime, other.finishTime, loc, "finishTime");

    compare1(taskType, other.taskType, loc, "taskType");
    compare1(taskStatus, other.taskStatus, loc, "taskStatus");

    compareLoggedTaskAttempts(attempts, other.attempts, loc, "attempts");
    compareLoggedLocations(preferredLocations, other.preferredLocations, loc,
        "preferredLocations");
  }
}
