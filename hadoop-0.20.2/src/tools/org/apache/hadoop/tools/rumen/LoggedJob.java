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
/**
 * 
 */
package org.apache.hadoop.tools.rumen;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.codehaus.jackson.annotate.JsonAnySetter;

/**
 * A {@link LoggedDiscreteCDF} is a representation of an hadoop job, with the
 * details of this class set up to meet the requirements of the Jackson JSON
 * parser/generator.
 * 
 * All of the public methods are simply accessors for the instance variables we
 * want to write out in the JSON files.
 * 
 */
public class LoggedJob implements DeepCompare {
  public enum JobType {
    JAVA, PIG, STREAMING, PIPES, OVERALL
  };

  public enum JobPriority {
    VERY_LOW, LOW, NORMAL, HIGH, VERY_HIGH
  };

  static private Set<String> alreadySeenAnySetterAttributes =
      new TreeSet<String>();

  String jobID;
  String user;
  long computonsPerMapInputByte = -1L;
  long computonsPerMapOutputByte = -1L;
  long computonsPerReduceInputByte = -1L;
  long computonsPerReduceOutputByte = -1L;
  long submitTime = -1L;
  long launchTime = -1L;
  long finishTime = -1L;

  int heapMegabytes = -1;
  int totalMaps = -1;
  int totalReduces = -1;
  Pre21JobHistoryConstants.Values outcome = null;
  JobType jobtype = JobType.JAVA;
  JobPriority priority = JobPriority.NORMAL;

  List<String> directDependantJobs = new ArrayList<String>();
  List<LoggedTask> mapTasks = new ArrayList<LoggedTask>();
  List<LoggedTask> reduceTasks = new ArrayList<LoggedTask>();
  List<LoggedTask> otherTasks = new ArrayList<LoggedTask>();

  // There are CDFs for each level of locality -- most local first
  ArrayList<LoggedDiscreteCDF> successfulMapAttemptCDFs;
  // There are CDFs for each level of locality -- most local first
  ArrayList<LoggedDiscreteCDF> failedMapAttemptCDFs;

  LoggedDiscreteCDF successfulReduceAttemptCDF;
  LoggedDiscreteCDF failedReduceAttemptCDF;

  String queue = null;

  String jobName = null;

  int clusterMapMB = -1;
  int clusterReduceMB = -1;
  int jobMapMB = -1;
  int jobReduceMB = -1;

  long relativeTime = 0;

  double[] mapperTriesToSucceed;
  double failedMapperFraction; // !!!!!

  LoggedJob() {

  }

  LoggedJob(String jobID) {
    super();

    setJobID(jobID);
  }

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

  public String getUser() {
    return user;
  }

  void setUser(String user) {
    this.user = user;
  }

  public String getJobID() {
    return jobID;
  }

  void setJobID(String jobID) {
    this.jobID = jobID;
  }

  public JobPriority getPriority() {
    return priority;
  }

  void setPriority(JobPriority priority) {
    this.priority = priority;
  }

  public long getComputonsPerMapInputByte() {
    return computonsPerMapInputByte;
  }

  void setComputonsPerMapInputByte(long computonsPerMapInputByte) {
    this.computonsPerMapInputByte = computonsPerMapInputByte;
  }

  public long getComputonsPerMapOutputByte() {
    return computonsPerMapOutputByte;
  }

  void setComputonsPerMapOutputByte(long computonsPerMapOutputByte) {
    this.computonsPerMapOutputByte = computonsPerMapOutputByte;
  }

  public long getComputonsPerReduceInputByte() {
    return computonsPerReduceInputByte;
  }

  void setComputonsPerReduceInputByte(long computonsPerReduceInputByte) {
    this.computonsPerReduceInputByte = computonsPerReduceInputByte;
  }

  public long getComputonsPerReduceOutputByte() {
    return computonsPerReduceOutputByte;
  }

  void setComputonsPerReduceOutputByte(long computonsPerReduceOutputByte) {
    this.computonsPerReduceOutputByte = computonsPerReduceOutputByte; // !!!!!
  }

  public long getSubmitTime() {
    return submitTime;
  }

  void setSubmitTime(long submitTime) {
    this.submitTime = submitTime;
  }

  public long getLaunchTime() {
    return launchTime;
  }

  void setLaunchTime(long startTime) {
    this.launchTime = startTime;
  }

  public long getFinishTime() {
    return finishTime;
  }

  void setFinishTime(long finishTime) {
    this.finishTime = finishTime;
  }

  public int getHeapMegabytes() {
    return heapMegabytes;
  }

  void setHeapMegabytes(int heapMegabytes) {
    this.heapMegabytes = heapMegabytes;
  }

  public int getTotalMaps() {
    return totalMaps;
  }

  void setTotalMaps(int totalMaps) {
    this.totalMaps = totalMaps;
  }

  public int getTotalReduces() {
    return totalReduces;
  }

  void setTotalReduces(int totalReduces) {
    this.totalReduces = totalReduces;
  }

  public Pre21JobHistoryConstants.Values getOutcome() {
    return outcome;
  }

  void setOutcome(Pre21JobHistoryConstants.Values outcome) {
    this.outcome = outcome;
  }

  public JobType getJobtype() {
    return jobtype;
  }

  void setJobtype(JobType jobtype) {
    this.jobtype = jobtype;
  }

  public List<String> getDirectDependantJobs() {
    return directDependantJobs;
  }

  void setDirectDependantJobs(List<String> directDependantJobs) {
    this.directDependantJobs = directDependantJobs;
  }

  public List<LoggedTask> getMapTasks() {
    return mapTasks;
  }

  void setMapTasks(List<LoggedTask> mapTasks) {
    this.mapTasks = mapTasks;
  }

  public List<LoggedTask> getReduceTasks() {
    return reduceTasks;
  }

  void setReduceTasks(List<LoggedTask> reduceTasks) {
    this.reduceTasks = reduceTasks;
  }

  public List<LoggedTask> getOtherTasks() {
    return otherTasks;
  }

  void setOtherTasks(List<LoggedTask> otherTasks) {
    this.otherTasks = otherTasks;
  }

  public ArrayList<LoggedDiscreteCDF> getSuccessfulMapAttemptCDFs() {
    return successfulMapAttemptCDFs;
  }

  void setSuccessfulMapAttemptCDFs(
      ArrayList<LoggedDiscreteCDF> successfulMapAttemptCDFs) {
    this.successfulMapAttemptCDFs = successfulMapAttemptCDFs;
  }

  public ArrayList<LoggedDiscreteCDF> getFailedMapAttemptCDFs() {
    return failedMapAttemptCDFs;
  }

  void setFailedMapAttemptCDFs(ArrayList<LoggedDiscreteCDF> failedMapAttemptCDFs) {
    this.failedMapAttemptCDFs = failedMapAttemptCDFs;
  }

  public LoggedDiscreteCDF getSuccessfulReduceAttemptCDF() {
    return successfulReduceAttemptCDF;
  }

  void setSuccessfulReduceAttemptCDF(
      LoggedDiscreteCDF successfulReduceAttemptCDF) {
    this.successfulReduceAttemptCDF = successfulReduceAttemptCDF;
  }

  public LoggedDiscreteCDF getFailedReduceAttemptCDF() {
    return failedReduceAttemptCDF;
  }

  void setFailedReduceAttemptCDF(LoggedDiscreteCDF failedReduceAttemptCDF) {
    this.failedReduceAttemptCDF = failedReduceAttemptCDF;
  }

  public double[] getMapperTriesToSucceed() {
    return mapperTriesToSucceed;
  }

  void setMapperTriesToSucceed(double[] mapperTriesToSucceed) {
    this.mapperTriesToSucceed = mapperTriesToSucceed;
  }

  public double getFailedMapperFraction() {
    return failedMapperFraction;
  }

  void setFailedMapperFraction(double failedMapperFraction) {
    this.failedMapperFraction = failedMapperFraction;
  }

  public long getRelativeTime() {
    return relativeTime;
  }

  void setRelativeTime(long relativeTime) {
    this.relativeTime = relativeTime;
  }

  public String getQueue() {
    return queue;
  }

  void setQueue(String queue) {
    this.queue = queue;
  }

  public String getJobName() {
    return jobName;
  }

  void setJobName(String jobName) {
    this.jobName = jobName;
  }

  public int getClusterMapMB() {
    return clusterMapMB;
  }

  void setClusterMapMB(int clusterMapMB) {
    this.clusterMapMB = clusterMapMB;
  }

  public int getClusterReduceMB() {
    return clusterReduceMB;
  }

  void setClusterReduceMB(int clusterReduceMB) {
    this.clusterReduceMB = clusterReduceMB;
  }

  public int getJobMapMB() {
    return jobMapMB;
  }

  void setJobMapMB(int jobMapMB) {
    this.jobMapMB = jobMapMB;
  }

  public int getJobReduceMB() {
    return jobReduceMB;
  }

  void setJobReduceMB(int jobReduceMB) {
    this.jobReduceMB = jobReduceMB;
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

  private void compare1(JobType c1, JobType c2, TreePath loc, String eltname)
      throws DeepInequalityException {
    if (c1 != c2) {
      throw new DeepInequalityException(eltname + " miscompared", new TreePath(
          loc, eltname));
    }
  }

  private void compare1(JobPriority c1, JobPriority c2, TreePath loc,
      String eltname) throws DeepInequalityException {
    if (c1 != c2) {
      throw new DeepInequalityException(eltname + " miscompared", new TreePath(
          loc, eltname));
    }
  }

  private void compare1(int c1, int c2, TreePath loc, String eltname)
      throws DeepInequalityException {
    if (c1 != c2) {
      throw new DeepInequalityException(eltname + " miscompared", new TreePath(
          loc, eltname));
    }
  }

  private void compare1(double c1, double c2, TreePath loc, String eltname)
      throws DeepInequalityException {
    if (c1 != c2) {
      throw new DeepInequalityException(eltname + " miscompared", new TreePath(
          loc, eltname));
    }
  }

  private void compare1(double[] c1, double[] c2, TreePath loc, String eltname)
      throws DeepInequalityException {
    if (c1 == null && c2 == null) {
      return;
    }

    TreePath recursePath = new TreePath(loc, eltname);

    if (c1 == null || c2 == null || c1.length != c2.length) {
      throw new DeepInequalityException(eltname + " miscompared", recursePath);
    }

    for (int i = 0; i < c1.length; ++i) {
      if (c1[i] != c2[i]) {
        throw new DeepInequalityException(eltname + " miscompared",
            new TreePath(loc, eltname, i));
      }
    }
  }

  private void compare1(DeepCompare c1, DeepCompare c2, TreePath loc,
      String eltname, int index) throws DeepInequalityException {
    if (c1 == null && c2 == null) {
      return;
    }

    TreePath recursePath = new TreePath(loc, eltname, index);

    if (c1 == null || c2 == null) {
      if (index == -1) {
        throw new DeepInequalityException(eltname + " miscompared", recursePath);
      } else {
        throw new DeepInequalityException(eltname + "[" + index
            + "] miscompared", recursePath);
      }
    }

    c1.deepCompare(c2, recursePath);
  }

  // I'll treat this as an atomic object type
  private void compareStrings(List<String> c1, List<String> c2, TreePath loc,
      String eltname) throws DeepInequalityException {
    if (c1 == null && c2 == null) {
      return;
    }

    TreePath recursePath = new TreePath(loc, eltname);

    if (c1 == null || c2 == null || !c1.equals(c2)) {
      throw new DeepInequalityException(eltname + " miscompared", recursePath);
    }
  }

  private void compareLoggedTasks(List<LoggedTask> c1, List<LoggedTask> c2,
      TreePath loc, String eltname) throws DeepInequalityException {
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

  private void compareCDFs(List<LoggedDiscreteCDF> c1,
      List<LoggedDiscreteCDF> c2, TreePath loc, String eltname)
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
    if (!(comparand instanceof LoggedJob)) {
      throw new DeepInequalityException("comparand has wrong type", loc);
    }

    LoggedJob other = (LoggedJob) comparand;

    compare1(jobID, other.jobID, loc, "jobID");
    compare1(user, other.user, loc, "user");

    compare1(computonsPerMapInputByte, other.computonsPerMapInputByte, loc,
        "computonsPerMapInputByte");
    compare1(computonsPerMapOutputByte, other.computonsPerMapOutputByte, loc,
        "computonsPerMapOutputByte");
    compare1(computonsPerReduceInputByte, other.computonsPerReduceInputByte,
        loc, "computonsPerReduceInputByte");
    compare1(computonsPerReduceOutputByte, other.computonsPerReduceOutputByte,
        loc, "computonsPerReduceOutputByte");

    compare1(submitTime, other.submitTime, loc, "submitTime");
    compare1(launchTime, other.launchTime, loc, "launchTime");
    compare1(finishTime, other.finishTime, loc, "finishTime");

    compare1(heapMegabytes, other.heapMegabytes, loc, "heapMegabytes");

    compare1(totalMaps, other.totalMaps, loc, "totalMaps");
    compare1(totalReduces, other.totalReduces, loc, "totalReduces");

    compare1(outcome, other.outcome, loc, "outcome");
    compare1(jobtype, other.jobtype, loc, "jobtype");
    compare1(priority, other.priority, loc, "priority");

    compareStrings(directDependantJobs, other.directDependantJobs, loc,
        "directDependantJobs");

    compareLoggedTasks(mapTasks, other.mapTasks, loc, "mapTasks");
    compareLoggedTasks(reduceTasks, other.reduceTasks, loc, "reduceTasks");
    compareLoggedTasks(otherTasks, other.otherTasks, loc, "otherTasks");

    compare1(relativeTime, other.relativeTime, loc, "relativeTime");

    compareCDFs(successfulMapAttemptCDFs, other.successfulMapAttemptCDFs, loc,
        "successfulMapAttemptCDFs");
    compareCDFs(failedMapAttemptCDFs, other.failedMapAttemptCDFs, loc,
        "failedMapAttemptCDFs");
    compare1(successfulReduceAttemptCDF, other.successfulReduceAttemptCDF, loc,
        "successfulReduceAttemptCDF", -1);
    compare1(failedReduceAttemptCDF, other.failedReduceAttemptCDF, loc,
        "failedReduceAttemptCDF", -1);

    compare1(mapperTriesToSucceed, other.mapperTriesToSucceed, loc,
        "mapperTriesToSucceed");
    compare1(failedMapperFraction, other.failedMapperFraction, loc,
        "failedMapperFraction");

    compare1(queue, other.queue, loc, "queue");
    compare1(jobName, other.jobName, loc, "jobName");

    compare1(clusterMapMB, other.clusterMapMB, loc, "clusterMapMB");
    compare1(clusterReduceMB, other.clusterReduceMB, loc, "clusterReduceMB");
    compare1(jobMapMB, other.jobMapMB, loc, "jobMapMB");
    compare1(jobReduceMB, other.jobReduceMB, loc, "jobReduceMB");
  }
}
