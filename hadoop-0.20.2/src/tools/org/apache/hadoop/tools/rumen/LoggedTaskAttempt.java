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

import java.util.Set;
import java.util.TreeSet;

import org.codehaus.jackson.annotate.JsonAnySetter;

// HACK ALERT!!!  This "should" have have two subclasses, which might be called
//                LoggedMapTaskAttempt and LoggedReduceTaskAttempt, but 
//                the Jackson implementation of JSON doesn't handle a 
//                superclass-valued field.

/**
 * A {@link LoggedTaskAttempt} represents an attempt to run an hadoop task in a
 * hadoop job. Note that a task can have several attempts.
 * 
 * All of the public methods are simply accessors for the instance variables we
 * want to write out in the JSON files.
 * 
 */
public class LoggedTaskAttempt implements DeepCompare {

  String attemptID;
  Pre21JobHistoryConstants.Values result;
  long startTime = -1L;
  long finishTime = -1L;
  String hostName;

  long hdfsBytesRead = -1L;
  long hdfsBytesWritten = -1L;
  long fileBytesRead = -1L;
  long fileBytesWritten = -1L;
  long mapInputRecords = -1L;
  long mapInputBytes = -1L;
  long mapOutputBytes = -1L;
  long mapOutputRecords = -1L;
  long combineInputRecords = -1L;
  long reduceInputGroups = -1L;
  long reduceInputRecords = -1L;
  long reduceShuffleBytes = -1L;
  long reduceOutputRecords = -1L;
  long spilledRecords = -1L;

  long shuffleFinished = -1L;
  long sortFinished = -1L;

  LoggedLocation location;

  LoggedTaskAttempt() {
    super();
  }

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

  public long getShuffleFinished() {
    return shuffleFinished;
  }

  void setShuffleFinished(long shuffleFinished) {
    this.shuffleFinished = shuffleFinished;
  }

  public long getSortFinished() {
    return sortFinished;
  }

  void setSortFinished(long sortFinished) {
    this.sortFinished = sortFinished;
  }

  public String getAttemptID() {
    return attemptID;
  }

  void setAttemptID(String attemptID) {
    this.attemptID = attemptID;
  }

  public Pre21JobHistoryConstants.Values getResult() {
    return result;
  }

  void setResult(Pre21JobHistoryConstants.Values result) {
    this.result = result;
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

  public String getHostName() {
    return hostName;
  }

  void setHostName(String hostName) {
    this.hostName = (hostName == null) ? null : hostName.intern();
  }

  public long getHdfsBytesRead() {
    return hdfsBytesRead;
  }

  void setHdfsBytesRead(long hdfsBytesRead) {
    this.hdfsBytesRead = hdfsBytesRead;
  }

  public long getHdfsBytesWritten() {
    return hdfsBytesWritten;
  }

  void setHdfsBytesWritten(long hdfsBytesWritten) {
    this.hdfsBytesWritten = hdfsBytesWritten;
  }

  public long getFileBytesRead() {
    return fileBytesRead;
  }

  void setFileBytesRead(long fileBytesRead) {
    this.fileBytesRead = fileBytesRead;
  }

  public long getFileBytesWritten() {
    return fileBytesWritten;
  }

  void setFileBytesWritten(long fileBytesWritten) {
    this.fileBytesWritten = fileBytesWritten;
  }

  public long getMapInputRecords() {
    return mapInputRecords;
  }

  void setMapInputRecords(long mapInputRecords) {
    this.mapInputRecords = mapInputRecords;
  }

  public long getMapOutputBytes() {
    return mapOutputBytes;
  }

  void setMapOutputBytes(long mapOutputBytes) {
    this.mapOutputBytes = mapOutputBytes;
  }

  public long getMapOutputRecords() {
    return mapOutputRecords;
  }

  void setMapOutputRecords(long mapOutputRecords) {
    this.mapOutputRecords = mapOutputRecords;
  }

  public long getCombineInputRecords() {
    return combineInputRecords;
  }

  void setCombineInputRecords(long combineInputRecords) {
    this.combineInputRecords = combineInputRecords;
  }

  public long getReduceInputGroups() {
    return reduceInputGroups;
  }

  void setReduceInputGroups(long reduceInputGroups) {
    this.reduceInputGroups = reduceInputGroups;
  }

  public long getReduceInputRecords() {
    return reduceInputRecords;
  }

  void setReduceInputRecords(long reduceInputRecords) {
    this.reduceInputRecords = reduceInputRecords;
  }

  public long getReduceShuffleBytes() {
    return reduceShuffleBytes;
  }

  void setReduceShuffleBytes(long reduceShuffleBytes) {
    this.reduceShuffleBytes = reduceShuffleBytes;
  }

  public long getReduceOutputRecords() {
    return reduceOutputRecords;
  }

  void setReduceOutputRecords(long reduceOutputRecords) {
    this.reduceOutputRecords = reduceOutputRecords;
  }

  public long getSpilledRecords() {
    return spilledRecords;
  }

  void setSpilledRecords(long spilledRecords) {
    this.spilledRecords = spilledRecords;
  }

  public LoggedLocation getLocation() {
    return location;
  }

  void setLocation(LoggedLocation location) {
    this.location = location;
  }

  public long getMapInputBytes() {
    return mapInputBytes;
  }

  void setMapInputBytes(long mapInputBytes) {
    this.mapInputBytes = mapInputBytes;
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

  private void compare1(long c1, long c2, TreePath loc, String eltname)
      throws DeepInequalityException {
    if (c1 != c2) {
      throw new DeepInequalityException(eltname + " miscompared", new TreePath(
          loc, eltname));
    }
  }

  private void compare1(Pre21JobHistoryConstants.Values c1,
      Pre21JobHistoryConstants.Values c2, TreePath loc, String eltname)
      throws DeepInequalityException {
    if (c1 != c2) {
      throw new DeepInequalityException(eltname + " miscompared", new TreePath(
          loc, eltname));
    }
  }

  private void compare1(LoggedLocation c1, LoggedLocation c2, TreePath loc,
      String eltname) throws DeepInequalityException {
    if (c1 == null && c2 == null) {
      return;
    }

    TreePath recurse = new TreePath(loc, eltname);

    if (c1 == null || c2 == null) {
      throw new DeepInequalityException(eltname + " miscompared", recurse);
    }

    c1.deepCompare(c2, recurse);
  }

  public void deepCompare(DeepCompare comparand, TreePath loc)
      throws DeepInequalityException {
    if (!(comparand instanceof LoggedTaskAttempt)) {
      throw new DeepInequalityException("comparand has wrong type", loc);
    }

    LoggedTaskAttempt other = (LoggedTaskAttempt) comparand;

    compare1(attemptID, other.attemptID, loc, "attemptID");
    compare1(result, other.result, loc, "result");
    compare1(startTime, other.startTime, loc, "startTime");
    compare1(finishTime, other.finishTime, loc, "finishTime");
    compare1(hostName, other.hostName, loc, "hostName");

    compare1(hdfsBytesRead, other.hdfsBytesRead, loc, "hdfsBytesRead");
    compare1(hdfsBytesWritten, other.hdfsBytesWritten, loc, "hdfsBytesWritten");
    compare1(fileBytesRead, other.fileBytesRead, loc, "fileBytesRead");
    compare1(fileBytesWritten, other.fileBytesWritten, loc, "fileBytesWritten");
    compare1(mapInputBytes, other.mapInputBytes, loc, "mapInputBytes");
    compare1(mapInputRecords, other.mapInputRecords, loc, "mapInputRecords");
    compare1(mapOutputBytes, other.mapOutputBytes, loc, "mapOutputBytes");
    compare1(mapOutputRecords, other.mapOutputRecords, loc, "mapOutputRecords");
    compare1(combineInputRecords, other.combineInputRecords, loc,
        "combineInputRecords");
    compare1(reduceInputGroups, other.reduceInputGroups, loc,
        "reduceInputGroups");
    compare1(reduceInputRecords, other.reduceInputRecords, loc,
        "reduceInputRecords");
    compare1(reduceShuffleBytes, other.reduceShuffleBytes, loc,
        "reduceShuffleBytes");
    compare1(reduceOutputRecords, other.reduceOutputRecords, loc,
        "reduceOutputRecords");
    compare1(spilledRecords, other.spilledRecords, loc, "spilledRecords");

    compare1(shuffleFinished, other.shuffleFinished, loc, "shuffleFinished");
    compare1(sortFinished, other.sortFinished, loc, "sortFinished");

    compare1(location, other.location, loc, "location");
  }
}
