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
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.util.StringUtils;
/**************************************************
 * Describes the current status of a task.  This is
 * not intended to be a comprehensive piece of data.
 *
 **************************************************/
public abstract class TaskStatus implements Writable, Cloneable {
  static final Log LOG =
    LogFactory.getLog(TaskStatus.class.getName());
  
  //enumeration for reporting current phase of a task. 
  public static enum Phase{STARTING, MAP, SHUFFLE, SORT, REDUCE, CLEANUP}

  // what state is the task in?
  public static enum State {RUNNING, SUCCEEDED, FAILED, UNASSIGNED, KILLED, 
                            COMMIT_PENDING, FAILED_UNCLEAN, KILLED_UNCLEAN}
    
  private final TaskAttemptID taskid;
  private float progress;
  private volatile State runState;
  private String diagnosticInfo;
  private String stateString;
  private String taskTracker;
  private int numSlots;
    
  private long startTime; 
  private long finishTime; 
  private long outputSize = -1L;
    
  private volatile Phase phase = Phase.STARTING; 
  private Counters counters;
  private boolean includeCounters;
  private SortedRanges.Range nextRecordRange = new SortedRanges.Range();
  
  // max task-status string size
  static final int MAX_STRING_SIZE = 1024;

  /**
   * Testcases can override {@link #getMaxStringSize()} to control the max-size 
   * of strings in {@link TaskStatus}. Note that the {@link TaskStatus} is never
   * exposed to clients or users (i.e Map or Reduce) and hence users cannot 
   * override this api to pass large strings in {@link TaskStatus}.
   */
  protected int getMaxStringSize() {
    return MAX_STRING_SIZE;
  }
  
  public TaskStatus() {
    taskid = new TaskAttemptID();
    numSlots = 0;
  }

  public TaskStatus(TaskAttemptID taskid, float progress, int numSlots,
                    State runState, String diagnosticInfo,
                    String stateString, String taskTracker,
                    Phase phase, Counters counters) {
    this.taskid = taskid;    
    this.progress = progress;
    this.numSlots = numSlots;
    this.runState = runState;
    setDiagnosticInfo(diagnosticInfo);
    setStateString(stateString);
    this.taskTracker = taskTracker;
    this.phase = phase;
    this.counters = counters;
    this.includeCounters = true;
  }
  
  public TaskAttemptID getTaskID() { return taskid; }
  public abstract boolean getIsMap();
  public int getNumSlots() {
    return numSlots;
  }

  public float getProgress() { return progress; }
  public void setProgress(float progress) { this.progress = progress; } 
  public State getRunState() { return runState; }
  public String getTaskTracker() {return taskTracker;}
  public void setTaskTracker(String tracker) { this.taskTracker = tracker;}
  public void setRunState(State runState) { this.runState = runState; }
  public String getDiagnosticInfo() { return diagnosticInfo; }
  public void setDiagnosticInfo(String info) {
    // if the diag-info has already reached its max then log and return
    if (diagnosticInfo != null 
        && diagnosticInfo.length() == getMaxStringSize()) {
      LOG.info("task-diagnostic-info for task " + taskid + " : " + info);
      return;
    }
    diagnosticInfo = 
      ((diagnosticInfo == null) ? info : diagnosticInfo.concat(info)); 
    // trim the string to MAX_STRING_SIZE if needed
    if (diagnosticInfo != null 
        && diagnosticInfo.length() > getMaxStringSize()) {
      LOG.info("task-diagnostic-info for task " + taskid + " : " 
               + diagnosticInfo);
      diagnosticInfo = diagnosticInfo.substring(0, getMaxStringSize());
    }
  }
  public String getStateString() { return stateString; }
  /**
   * Set the state of the {@link TaskStatus}.
   */
  public void setStateString(String stateString) {
    if (stateString != null) {
      if (stateString.length() <= getMaxStringSize()) {
        this.stateString = stateString;
      } else {
        // log it
        LOG.info("state-string for task " + taskid + " : " + stateString);
        // trim the state string
        this.stateString = stateString.substring(0, getMaxStringSize());
      }
    }
  }
  
  /**
   * Get the next record range which is going to be processed by Task.
   * @return nextRecordRange
   */
  public SortedRanges.Range getNextRecordRange() {
    return nextRecordRange;
  }

  /**
   * Set the next record range which is going to be processed by Task.
   * @param nextRecordRange
   */
  public void setNextRecordRange(SortedRanges.Range nextRecordRange) {
    this.nextRecordRange = nextRecordRange;
  }
  
  /**
   * Get task finish time. if shuffleFinishTime and sortFinishTime 
   * are not set before, these are set to finishTime. It takes care of 
   * the case when shuffle, sort and finish are completed with in the 
   * heartbeat interval and are not reported separately. if task state is 
   * TaskStatus.FAILED then finish time represents when the task failed.
   * @return finish time of the task. 
   */
  public long getFinishTime() {
    return finishTime;
  }

  /**
   * Sets finishTime for the task status if and only if the
   * start time is set and passed finish time is greater than
   * zero.
   * 
   * @param finishTime finish time of task.
   */
  void setFinishTime(long finishTime) {
    if(this.getStartTime() > 0 && finishTime > 0) {
      this.finishTime = finishTime;
    } else {
      //Using String utils to get the stack trace.
      LOG.error("Trying to set finish time for task " + taskid + 
          " when no start time is set, stackTrace is : " + 
          StringUtils.stringifyException(new Exception()));
    }
  }
  
  /**
   * Get shuffle finish time for the task. If shuffle finish time was 
   * not set due to shuffle/sort/finish phases ending within same
   * heartbeat interval, it is set to finish time of next phase i.e. sort 
   * or task finish when these are set.  
   * @return 0 if shuffleFinishTime, sortFinishTime and finish time are not set. else 
   * it returns approximate shuffle finish time.  
   */
  public long getShuffleFinishTime() {
    return 0;
  }

  /**
   * Set shuffle finish time. 
   * @param shuffleFinishTime 
   */
  void setShuffleFinishTime(long shuffleFinishTime) {}

  /**
   * Get sort finish time for the task,. If sort finish time was not set 
   * due to sort and reduce phase finishing in same heartebat interval, it is 
   * set to finish time, when finish time is set. 
   * @return 0 if sort finish time and finish time are not set, else returns sort
   * finish time if that is set, else it returns finish time. 
   */
  public long getSortFinishTime() {
    return 0;
  }

  /**
   * Sets sortFinishTime, if shuffleFinishTime is not set before 
   * then its set to sortFinishTime.  
   * @param sortFinishTime
   */
  void setSortFinishTime(long sortFinishTime) {}

  /**
   * Get start time of the task. 
   * @return 0 is start time is not set, else returns start time. 
   */
  public long getStartTime() {
    return startTime;
  }

  /**
   * Set startTime of the task if start time is greater than zero.
   * @param startTime start time
   */
  void setStartTime(long startTime) {
    //Making the assumption of passed startTime to be a positive
    //long value explicit.
    if (startTime > 0) {
      this.startTime = startTime;
    } else {
      //Using String utils to get the stack trace.
      LOG.error("Trying to set illegal startTime for task : " + taskid +
          ".Stack trace is : " +
          StringUtils.stringifyException(new Exception()));
    }
  }
  
  /**
   * Get current phase of this task. Phase.Map in case of map tasks, 
   * for reduce one of Phase.SHUFFLE, Phase.SORT or Phase.REDUCE. 
   * @return . 
   */
  public Phase getPhase(){
    return this.phase; 
  }
  /**
   * Set current phase of this task.  
   * @param phase phase of this task
   */
  void setPhase(Phase phase){
    TaskStatus.Phase oldPhase = getPhase();
    if (oldPhase != phase){
      // sort phase started
      if (phase == TaskStatus.Phase.SORT){
        setShuffleFinishTime(System.currentTimeMillis());
      }else if (phase == TaskStatus.Phase.REDUCE){
        setSortFinishTime(System.currentTimeMillis());
      }
    }
    this.phase = phase; 
  }

  boolean inTaskCleanupPhase() {
    return (this.phase == TaskStatus.Phase.CLEANUP && 
      (this.runState == TaskStatus.State.FAILED_UNCLEAN || 
      this.runState == TaskStatus.State.KILLED_UNCLEAN));
  }
  
  public boolean getIncludeCounters() {
    return includeCounters; 
  }
  
  public void setIncludeCounters(boolean send) {
    includeCounters = send;
  }
  
  /**
   * Get task's counters.
   */
  public Counters getCounters() {
    return counters;
  }
  /**
   * Set the task's counters.
   * @param counters
   */
  public void setCounters(Counters counters) {
    this.counters = counters;
  }
  
  /**
   * Returns the number of bytes of output from this map.
   */
  public long getOutputSize() {
    return outputSize;
  }
  
  /**
   * Set the size on disk of this task's output.
   * @param l the number of map output bytes
   */
  void setOutputSize(long l)  {
    outputSize = l;
  }
  
  /**
   * Get the list of maps from which output-fetches failed.
   * 
   * @return the list of maps from which output-fetches failed.
   */
  public List<TaskAttemptID> getFetchFailedMaps() {
    return null;
  }
  
  /**
   * Add to the list of maps from which output-fetches failed.
   *  
   * @param mapTaskId map from which fetch failed
   */
  synchronized void addFetchFailedMap(TaskAttemptID mapTaskId) {}

  /**
   * Update the status of the task.
   * 
   * This update is done by ping thread before sending the status. 
   * 
   * @param progress
   * @param state
   * @param counters
   */
  synchronized void statusUpdate(float progress,
                                 String state, 
                                 Counters counters) {
    setProgress(progress);
    setStateString(state);
    setCounters(counters);
  }
  
  /**
   * Update the status of the task.
   * 
   * @param status updated status
   */
  synchronized void statusUpdate(TaskStatus status) {
    this.progress = status.getProgress();
    this.runState = status.getRunState();
    setStateString(status.getStateString());
    this.nextRecordRange = status.getNextRecordRange();

    setDiagnosticInfo(status.getDiagnosticInfo());
    
    if (status.getStartTime() > 0) {
      this.startTime = status.getStartTime(); 
    }
    if (status.getFinishTime() > 0) {
      setFinishTime(status.getFinishTime()); 
    }
    
    this.phase = status.getPhase();
    this.counters = status.getCounters();
    this.outputSize = status.outputSize;
  }

  /**
   * Update specific fields of task status
   * 
   * This update is done in JobTracker when a cleanup attempt of task
   * reports its status. Then update only specific fields, not all.
   * 
   * @param runState
   * @param progress
   * @param state
   * @param phase
   * @param finishTime
   */
  synchronized void statusUpdate(State runState, 
                                 float progress,
                                 String state, 
                                 Phase phase,
                                 long finishTime) {
    setRunState(runState);
    setProgress(progress);
    setStateString(state);
    setPhase(phase);
    if (finishTime > 0) {
      setFinishTime(finishTime); 
    }
  }

  /**
   * Clear out transient information after sending out a status-update
   * from either the {@link Task} to the {@link TaskTracker} or from the
   * {@link TaskTracker} to the {@link JobTracker}. 
   */
  synchronized void clearStatus() {
    // Clear diagnosticInfo
    diagnosticInfo = "";
  }

  @Override
  public Object clone() {
    try {
      return super.clone();
    } catch (CloneNotSupportedException cnse) {
      // Shouldn't happen since we do implement Clonable
      throw new InternalError(cnse.toString());
    }
  }
  
  //////////////////////////////////////////////
  // Writable
  //////////////////////////////////////////////
  public void write(DataOutput out) throws IOException {
    taskid.write(out);
    out.writeFloat(progress);
    out.writeInt(numSlots);
    WritableUtils.writeEnum(out, runState);
    Text.writeString(out, diagnosticInfo);
    Text.writeString(out, stateString);
    WritableUtils.writeEnum(out, phase);
    out.writeLong(startTime);
    out.writeLong(finishTime);
    out.writeBoolean(includeCounters);
    out.writeLong(outputSize);
    if (includeCounters) {
      counters.write(out);
    }
    nextRecordRange.write(out);
  }

  public void readFields(DataInput in) throws IOException {
    this.taskid.readFields(in);
    this.progress = in.readFloat();
    this.numSlots = in.readInt();
    this.runState = WritableUtils.readEnum(in, State.class);
    setDiagnosticInfo(Text.readString(in));
    setStateString(Text.readString(in));
    this.phase = WritableUtils.readEnum(in, Phase.class); 
    this.startTime = in.readLong(); 
    this.finishTime = in.readLong(); 
    counters = new Counters();
    this.includeCounters = in.readBoolean();
    this.outputSize = in.readLong();
    if (includeCounters) {
      counters.readFields(in);
    }
    nextRecordRange.readFields(in);
  }
  
  //////////////////////////////////////////////////////////////////////////////
  // Factory-like methods to create/read/write appropriate TaskStatus objects
  //////////////////////////////////////////////////////////////////////////////
  
  static TaskStatus createTaskStatus(DataInput in, TaskAttemptID taskId, 
                                     float progress, int numSlots,
                                     State runState, String diagnosticInfo,
                                     String stateString, String taskTracker,
                                     Phase phase, Counters counters) 
  throws IOException {
    boolean isMap = in.readBoolean();
    return createTaskStatus(isMap, taskId, progress, numSlots, runState, 
                            diagnosticInfo, stateString, taskTracker, phase, 
                            counters);
  }
  
  static TaskStatus createTaskStatus(boolean isMap, TaskAttemptID taskId, 
                                     float progress, int numSlots,
                                     State runState, String diagnosticInfo,
                                     String stateString, String taskTracker,
                                     Phase phase, Counters counters) { 
    return (isMap) ? new MapTaskStatus(taskId, progress, numSlots, runState, 
                                       diagnosticInfo, stateString, taskTracker, 
                                       phase, counters) :
                     new ReduceTaskStatus(taskId, progress, numSlots, runState, 
                                          diagnosticInfo, stateString, 
                                          taskTracker, phase, counters);
  }
  
  static TaskStatus createTaskStatus(boolean isMap) {
    return (isMap) ? new MapTaskStatus() : new ReduceTaskStatus();
  }

  static TaskStatus readTaskStatus(DataInput in) throws IOException {
    boolean isMap = in.readBoolean();
    TaskStatus taskStatus = createTaskStatus(isMap);
    taskStatus.readFields(in);
    return taskStatus;
  }
  
  static void writeTaskStatus(DataOutput out, TaskStatus taskStatus) 
  throws IOException {
    out.writeBoolean(taskStatus.getIsMap());
    taskStatus.write(out);
  }
}

