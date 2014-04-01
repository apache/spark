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

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.Format;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapred.Counters.Group;
import org.apache.hadoop.mapred.DefaultJobHistoryParser.*;
import org.apache.hadoop.mapred.JobHistory.*;
import org.apache.hadoop.util.StringUtils;

/**
 * This class is to view job history files.
 */
class HistoryViewer {
  private static SimpleDateFormat dateFormat = new SimpleDateFormat(
                                             "d-MMM-yyyy HH:mm:ss");
  private FileSystem fs;
  private Configuration conf;
  private Path historyLogDir;
  private String jobLogFile;
  private JobHistory.JobInfo job;
  private String trackerHostName;
  private String trackerStartTime;
  private String jobId;
  private boolean printAll;
  
  private PathFilter jobLogFileFilter = new PathFilter() {
    public boolean accept(Path path) {
      return !(path.getName().endsWith(".xml"));
    }
  };

  public HistoryViewer(String outputDir, Configuration conf, boolean printAll)
  throws IOException {
    this.conf = conf;
    this.printAll = printAll;
    Path output = new Path(outputDir);
    historyLogDir = new Path(output, "_logs/history");
    try {
      fs = output.getFileSystem(this.conf);
      if (!fs.exists(output)) {
        throw new IOException("History directory " + historyLogDir.toString()
                              + "does not exist");
      }
      Path[] jobFiles = FileUtil.stat2Paths(fs.listStatus(historyLogDir,
                                                          jobLogFileFilter));
      if (jobFiles.length == 0) {
        throw new IOException("Not a valid history directory " 
                              + historyLogDir.toString());
      }
      jobLogFile = jobFiles[0].toString();
      String[] jobDetails = 
          JobHistory.JobInfo.decodeJobHistoryFileName(jobFiles[0].getName()).
                             split("_");
      trackerHostName = jobDetails[0];
      trackerStartTime = jobDetails[1];
      jobId = jobDetails[2] + "_" + jobDetails[3] + "_" + jobDetails[4];
      job = new JobHistory.JobInfo(jobId); 
      DefaultJobHistoryParser.parseJobTasks(jobFiles[0].toString(), job, fs);
    } catch(Exception e) {
      throw new IOException("Not able to initialize History viewer", e);
    }
  }
  
  public void print() throws IOException {
    printJobDetails();
    printTaskSummary();
    printJobAnalysis();
    printTasks("SETUP", "FAILED");
    printTasks("SETUP", "KILLED");
    printTasks("MAP", "FAILED");
    printTasks("MAP", "KILLED");
    printTasks("REDUCE", "FAILED");
    printTasks("REDUCE", "KILLED");
    printTasks("CLEANUP", "FAILED");
    printTasks("CLEANUP", "KILLED");
    if (printAll) {
      printTasks("SETUP", "SUCCESS");
      printTasks("MAP", "SUCCESS");
      printTasks("REDUCE", "SUCCESS");
      printTasks("CLEANUP", "SUCCESS");
      printAllTaskAttempts("SETUP");
      printAllTaskAttempts("MAP");
      printAllTaskAttempts("REDUCE");
      printAllTaskAttempts("CLEANUP");
    }
    NodesFilter filter = new FailedOnNodesFilter();
    printFailedAttempts(filter);
    filter = new KilledOnNodesFilter();
    printFailedAttempts(filter);
  }

  private void printJobDetails() throws IOException {
    StringBuffer jobDetails = new StringBuffer();
    jobDetails.append("\nHadoop job: " ).append(jobId);
    jobDetails.append("\n=====================================");
    jobDetails.append("\nJob tracker host name: ").append(trackerHostName);
    jobDetails.append("\njob tracker start time: ").append( 
                      new Date(Long.parseLong(trackerStartTime))); 
    jobDetails.append("\nUser: ").append(job.get(Keys.USER)); 
    jobDetails.append("\nJobName: ").append(job.get(Keys.JOBNAME)); 
    jobDetails.append("\nJobConf: ").append(job.get(Keys.JOBCONF)); 
    jobDetails.append("\nSubmitted At: ").append(StringUtils.
                        getFormattedTimeWithDiff(dateFormat,
                        job.getLong(Keys.SUBMIT_TIME), 0)); 
    jobDetails.append("\nLaunched At: ").append(StringUtils.
                        getFormattedTimeWithDiff(dateFormat,
                        job.getLong(Keys.LAUNCH_TIME),
                        job.getLong(Keys.SUBMIT_TIME)));
    jobDetails.append("\nFinished At: ").append(StringUtils.
                        getFormattedTimeWithDiff(dateFormat,
                        job.getLong(Keys.FINISH_TIME),
                        job.getLong(Keys.LAUNCH_TIME)));
    jobDetails.append("\nStatus: ").append(((job.get(Keys.JOB_STATUS) == "") ? 
                      "Incomplete" :job.get(Keys.JOB_STATUS)));
    try {
      printCounters(jobDetails, job);
    } catch (ParseException p) {
      throw new IOException(p);
    }
    jobDetails.append("\n=====================================");
    System.out.println(jobDetails.toString());
  }
  
  private void printCounters(StringBuffer buff, JobHistory.JobInfo job) 
      throws ParseException {
    Counters mapCounters = 
      Counters.fromEscapedCompactString(job.get(Keys.MAP_COUNTERS));
    Counters reduceCounters = 
      Counters.fromEscapedCompactString(job.get(Keys.REDUCE_COUNTERS));
    Counters totalCounters = 
      Counters.fromEscapedCompactString(job.get(Keys.COUNTERS));
    
    // Killed jobs might not have counters
    if (totalCounters == null) {
      return;
    }
    buff.append("\nCounters: \n\n");
    buff.append(String.format("|%1$-30s|%2$-30s|%3$-10s|%4$-10s|%5$-10s|", 
      "Group Name",
      "Counter name",
      "Map Value",
      "Reduce Value",
      "Total Value"));
    buff.append("\n------------------------------------------"+
      "---------------------------------------------");
    for (String groupName : totalCounters.getGroupNames()) {
      Group totalGroup = totalCounters.getGroup(groupName);
      Group mapGroup = mapCounters.getGroup(groupName);
      Group reduceGroup = reduceCounters.getGroup(groupName);
      Format decimal = new DecimalFormat();
      Iterator<Counter> ctrItr = totalGroup.iterator();
      while (ctrItr.hasNext()) {
        Counter counter = ctrItr.next();
        String name = counter.getDisplayName();
        String mapValue = decimal.format(mapGroup.getCounter(name));
        String reduceValue = decimal.format(reduceGroup.getCounter(name));
        String totalValue = decimal.format(counter.getValue());
        buff.append(
          String.format("\n|%1$-30s|%2$-30s|%3$-10s|%4$-10s|%5$-10s", 
          totalGroup.getDisplayName(),
          counter.getDisplayName(),
          mapValue, reduceValue, totalValue));
      }
    }
  }
  
  private void printTasks(String taskType, String taskStatus) {
    Map<String, JobHistory.Task> tasks = job.getAllTasks();
    StringBuffer taskList = new StringBuffer();
    taskList.append("\n").append(taskStatus).append(" ");
    taskList.append(taskType).append(" task list for ").append(jobId);
    taskList.append("\nTaskId\t\tStartTime\tFinishTime\tError");
    if (Values.MAP.name().equals(taskType)) {
      taskList.append("\tInputSplits");
    }
    taskList.append("\n====================================================");
    System.out.println(taskList.toString());
    for (JobHistory.Task task : tasks.values()) {
      if (taskType.equals(task.get(Keys.TASK_TYPE)) &&
         (taskStatus.equals(task.get(Keys.TASK_STATUS))
          || taskStatus.equals("all"))) {
        taskList.setLength(0);
        taskList.append(task.get(Keys.TASKID));
        taskList.append("\t").append(StringUtils.getFormattedTimeWithDiff(
                   dateFormat, task.getLong(Keys.START_TIME), 0));
        taskList.append("\t").append(StringUtils.getFormattedTimeWithDiff(
                   dateFormat, task.getLong(Keys.FINISH_TIME),
                   task.getLong(Keys.START_TIME))); 
        taskList.append("\t").append(task.get(Keys.ERROR));
        if (Values.MAP.name().equals(taskType)) {
          taskList.append("\t").append(task.get(Keys.SPLITS));
        }
        System.out.println(taskList.toString());
      }
    }
  }
  
  private void printAllTaskAttempts(String taskType) {
    Map<String, JobHistory.Task> tasks = job.getAllTasks();
    StringBuffer taskList = new StringBuffer();
    taskList.append("\n").append(taskType);
    taskList.append(" task list for ").append(jobId);
    taskList.append("\nTaskId\t\tStartTime");
    if (Values.REDUCE.name().equals(taskType)) {
      taskList.append("\tShuffleFinished\tSortFinished");
    }
    taskList.append("\tFinishTime\tHostName\tError\tTaskLogs");
    taskList.append("\n====================================================");
    System.out.println(taskList.toString());
    for (JobHistory.Task task : tasks.values()) {
      for (JobHistory.TaskAttempt attempt : task.getTaskAttempts().values()) {
        if (taskType.equals(task.get(Keys.TASK_TYPE))){
          taskList.setLength(0); 
          taskList.append(attempt.get(Keys.TASK_ATTEMPT_ID)).append("\t");
          taskList.append(StringUtils.getFormattedTimeWithDiff(dateFormat,
                          attempt.getLong(Keys.START_TIME), 0)).append("\t");
          if (Values.REDUCE.name().equals(taskType)) {
            ReduceAttempt reduceAttempt = (ReduceAttempt)attempt; 
            taskList.append(StringUtils.getFormattedTimeWithDiff(dateFormat,
                            reduceAttempt.getLong(Keys.SHUFFLE_FINISHED),
                            reduceAttempt.getLong(Keys.START_TIME)));
            taskList.append("\t"); 
            taskList.append(StringUtils.getFormattedTimeWithDiff(dateFormat, 
                            reduceAttempt.getLong(Keys.SORT_FINISHED),
                            reduceAttempt.getLong(Keys.SHUFFLE_FINISHED))); 
          } 
          taskList.append(StringUtils.getFormattedTimeWithDiff(dateFormat,
                          attempt.getLong(Keys.FINISH_TIME),
                          attempt.getLong(Keys.START_TIME))); 
          taskList.append("\t"); 
          taskList.append(attempt.get(Keys.HOSTNAME)).append("\t");
          taskList.append(attempt.get(Keys.ERROR));
          String taskLogsUrl = JobHistory.getTaskLogsUrl(attempt);
          taskList.append(taskLogsUrl != null ? taskLogsUrl : "n/a");
          System.out.println(taskList.toString());
        }
      }
    }
  }
  
  private void printTaskSummary() {
    Map<String, JobHistory.Task> tasks = job.getAllTasks();
    int totalMaps = 0; 
    int totalReduces = 0; 
    int totalCleanups = 0;
    int totalSetups = 0;
    int numFailedMaps = 0; 
    int numKilledMaps = 0;
    int numFailedReduces = 0; 
    int numKilledReduces = 0;
    int numFinishedCleanups = 0;
    int numFailedCleanups = 0;
    int numKilledCleanups = 0;
    int numFinishedSetups = 0;
    int numFailedSetups = 0;
    int numKilledSetups = 0;
    long mapStarted = 0; 
    long mapFinished = 0; 
    long reduceStarted = 0; 
    long reduceFinished = 0; 
    long cleanupStarted = 0;
    long cleanupFinished = 0;
    long setupStarted = 0;
    long setupFinished = 0;

    Map <String, String> allHosts = new TreeMap<String, String>();

    for (JobHistory.Task task : tasks.values()) {
      Map<String, TaskAttempt> attempts = task.getTaskAttempts();
      allHosts.put(task.get(Keys.HOSTNAME), "");
      for (TaskAttempt attempt : attempts.values()) {
        long startTime = attempt.getLong(Keys.START_TIME); 
        long finishTime = attempt.getLong(Keys.FINISH_TIME); 
        if (Values.MAP.name().equals(task.get(Keys.TASK_TYPE))) {
          if (mapStarted==0 || mapStarted > startTime) {
            mapStarted = startTime; 
          }
          if (mapFinished < finishTime) {
            mapFinished = finishTime; 
          }
          totalMaps++; 
          if (Values.FAILED.name().equals(attempt.get(Keys.TASK_STATUS))) {
            numFailedMaps++; 
          } else if (Values.KILLED.name().equals(
                                            attempt.get(Keys.TASK_STATUS))) {
            numKilledMaps++;
          }
        } else if (Values.REDUCE.name().equals(task.get(Keys.TASK_TYPE))) {
          if (reduceStarted==0||reduceStarted > startTime) {
            reduceStarted = startTime; 
          }
          if (reduceFinished < finishTime) {
            reduceFinished = finishTime; 
          }
          totalReduces++; 
          if (Values.FAILED.name().equals(attempt.get(Keys.TASK_STATUS))) {
            numFailedReduces++;
          } else if (Values.KILLED.name().equals(
                                            attempt.get(Keys.TASK_STATUS))) {
            numKilledReduces++;
          }
        } else if (Values.CLEANUP.name().equals(task.get(Keys.TASK_TYPE))){
          if (cleanupStarted==0||cleanupStarted > startTime) {
            cleanupStarted = startTime; 
          }
          if (cleanupFinished < finishTime) {
            cleanupFinished = finishTime; 
          }
          totalCleanups++; 
          if (Values.SUCCESS.name().equals(attempt.get(Keys.TASK_STATUS))) {
            numFinishedCleanups++;
          } else if (Values.FAILED.name().equals(
                                            attempt.get(Keys.TASK_STATUS))) {
            numFailedCleanups++;
          } else if (Values.KILLED.name().equals(
                                            attempt.get(Keys.TASK_STATUS))) {
            numKilledCleanups++;
          }
        } else if (Values.SETUP.name().equals(task.get(Keys.TASK_TYPE))){
          if (setupStarted==0||setupStarted > startTime) {
            setupStarted = startTime; 
          }
          if (setupFinished < finishTime) {
            setupFinished = finishTime; 
          }
          totalSetups++; 
          if (Values.SUCCESS.name().equals(attempt.get(Keys.TASK_STATUS))) {
            numFinishedSetups++;
          } else if (Values.FAILED.name().equals(
                                            attempt.get(Keys.TASK_STATUS))) {
            numFailedSetups++;
          } else if (Values.KILLED.name().equals(
                                            attempt.get(Keys.TASK_STATUS))) {
            numKilledSetups++;
          }
        }
      }
    }
    
    StringBuffer taskSummary = new StringBuffer();
    taskSummary.append("\nTask Summary");
    taskSummary.append("\n============================");
    taskSummary.append("\nKind\tTotal\t");
    taskSummary.append("Successful\tFailed\tKilled\tStartTime\tFinishTime");
    taskSummary.append("\n");
    taskSummary.append("\nSetup\t").append(totalSetups);
    taskSummary.append("\t").append(numFinishedSetups);
    taskSummary.append("\t\t").append(numFailedSetups);
    taskSummary.append("\t").append(numKilledSetups);
    taskSummary.append("\t").append(StringUtils.getFormattedTimeWithDiff(
                               dateFormat, setupStarted, 0));
    taskSummary.append("\t").append(StringUtils.getFormattedTimeWithDiff(
                               dateFormat, setupFinished, setupStarted)); 
    taskSummary.append("\nMap\t").append(totalMaps);
    taskSummary.append("\t").append(job.getInt(Keys.FINISHED_MAPS));
    taskSummary.append("\t\t").append(numFailedMaps);
    taskSummary.append("\t").append(numKilledMaps);
    taskSummary.append("\t").append(StringUtils.getFormattedTimeWithDiff(
                               dateFormat, mapStarted, 0));
    taskSummary.append("\t").append(StringUtils.getFormattedTimeWithDiff(
                               dateFormat, mapFinished, mapStarted));
    taskSummary.append("\nReduce\t").append(totalReduces);
    taskSummary.append("\t").append(job.getInt(Keys.FINISHED_REDUCES));
    taskSummary.append("\t\t").append(numFailedReduces);
    taskSummary.append("\t").append(numKilledReduces);
    taskSummary.append("\t").append(StringUtils.getFormattedTimeWithDiff(
                               dateFormat, reduceStarted, 0));
    taskSummary.append("\t").append(StringUtils.getFormattedTimeWithDiff(
                               dateFormat, reduceFinished, reduceStarted)); 
    taskSummary.append("\nCleanup\t").append(totalCleanups);
    taskSummary.append("\t").append(numFinishedCleanups);
    taskSummary.append("\t\t").append(numFailedCleanups);
    taskSummary.append("\t").append(numKilledCleanups);
    taskSummary.append("\t").append(StringUtils.getFormattedTimeWithDiff(
                               dateFormat, cleanupStarted, 0));
    taskSummary.append("\t").append(StringUtils.getFormattedTimeWithDiff(
                               dateFormat, cleanupFinished, cleanupStarted)); 
    taskSummary.append("\n============================\n");
    System.out.println(taskSummary.toString());
  }
  
  private void printFailedAttempts(NodesFilter filter) throws IOException {
    JobHistory.parseHistoryFromFS(jobLogFile, filter, fs); 
    Map<String, Set<String>> badNodes = filter.getValues();
    StringBuffer attempts = new StringBuffer(); 
    if (badNodes.size() > 0) {
      attempts.append("\n").append(filter.getFailureType());
      attempts.append(" task attempts by nodes");
      attempts.append("\nHostname\tFailedTasks");
      attempts.append("\n===============================");
      System.out.println(attempts.toString());
      for (Map.Entry<String, Set<String>> entry : badNodes.entrySet()) {
        String node = entry.getKey();
        Set<String> failedTasks = entry.getValue();
        attempts.setLength(0);
        attempts.append(node).append("\t");
        for (String t : failedTasks) {
          attempts.append(t).append(", ");
        }
        System.out.println(attempts.toString());
      }
    }
  }
  
  private void printJobAnalysis() {
    if (!Values.SUCCESS.name().equals(job.get(Keys.JOB_STATUS))) {
      System.out.println("No Analysis available as job did not finish");
      return;
    }
    
    Map<String, JobHistory.Task> tasks = job.getAllTasks();
    int finishedMaps = job.getInt(Keys.FINISHED_MAPS);
    int finishedReduces = job.getInt(Keys.FINISHED_REDUCES);
    JobHistory.Task [] mapTasks = new JobHistory.Task[finishedMaps]; 
    JobHistory.Task [] reduceTasks = new JobHistory.Task[finishedReduces]; 
    int mapIndex = 0 , reduceIndex=0; 
    long avgMapTime = 0;
    long avgReduceTime = 0;
    long avgShuffleTime = 0;

    for (JobHistory.Task task : tasks.values()) {
      Map<String, TaskAttempt> attempts = task.getTaskAttempts();
      for (JobHistory.TaskAttempt attempt : attempts.values()) {
        if (attempt.get(Keys.TASK_STATUS).equals(Values.SUCCESS.name())) {
          long avgFinishTime = (attempt.getLong(Keys.FINISH_TIME) -
                                attempt.getLong(Keys.START_TIME));
          if (Values.MAP.name().equals(task.get(Keys.TASK_TYPE))) {
            mapTasks[mapIndex++] = attempt; 
            avgMapTime += avgFinishTime;
          } else if (Values.REDUCE.name().equals(task.get(Keys.TASK_TYPE))) { 
            reduceTasks[reduceIndex++] = attempt;
            avgShuffleTime += (attempt.getLong(Keys.SHUFFLE_FINISHED) - 
                               attempt.getLong(Keys.START_TIME));
            avgReduceTime += (attempt.getLong(Keys.FINISH_TIME) -
                              attempt.getLong(Keys.SHUFFLE_FINISHED));
          }
          break;
        }
      }
    }
    if (finishedMaps > 0) {
      avgMapTime /= finishedMaps;
    }
    if (finishedReduces > 0) {
      avgReduceTime /= finishedReduces;
      avgShuffleTime /= finishedReduces;
    }
    System.out.println("\nAnalysis");
    System.out.println("=========");
    printAnalysis(mapTasks, cMap, "map", avgMapTime, 10);
    printLast(mapTasks, "map", cFinishMapRed);

    if (reduceTasks.length > 0) {
      printAnalysis(reduceTasks, cShuffle, "shuffle", avgShuffleTime, 10);
      printLast(reduceTasks, "shuffle", cFinishShuffle);

      printAnalysis(reduceTasks, cReduce, "reduce", avgReduceTime, 10);
      printLast(reduceTasks, "reduce", cFinishMapRed);
    }
    System.out.println("=========");
  }
  
  private void printLast(JobHistory.Task [] tasks,
                         String taskType,
                         Comparator<JobHistory.Task> cmp
                         ) {
    Arrays.sort(tasks, cFinishMapRed);
    JobHistory.Task last = tasks[0];
    StringBuffer lastBuf = new StringBuffer();
    lastBuf.append("The last ").append(taskType);
    lastBuf.append(" task ").append(last.get(Keys.TASKID));
    Long finishTime;
    if ("shuffle".equals(taskType)) {
      finishTime = last.getLong(Keys.SHUFFLE_FINISHED);
    } else {
      finishTime = last.getLong(Keys.FINISH_TIME);
    }
    lastBuf.append(" finished at (relative to the Job launch time): ");
    lastBuf.append(StringUtils.getFormattedTimeWithDiff(dateFormat,
                                 finishTime, job.getLong(Keys.LAUNCH_TIME)));
    System.out.println(lastBuf.toString());
  }

  private void printAnalysis(JobHistory.Task [] tasks,
                             Comparator<JobHistory.Task> cmp,
                             String taskType,
                             long avg,
                             int showTasks) {
    Arrays.sort(tasks, cmp);
    JobHistory.Task min = tasks[tasks.length-1];
    StringBuffer details = new StringBuffer();
    details.append("\nTime taken by best performing ");
    details.append(taskType).append(" task ");
    details.append(min.get(Keys.TASKID)).append(": ");
    if ("map".equals(taskType)) {
      details.append(StringUtils.formatTimeDiff(
                     min.getLong(Keys.FINISH_TIME),
                     min.getLong(Keys.START_TIME)));
    } else if ("shuffle".equals(taskType)) {
      details.append(StringUtils.formatTimeDiff(
                     min.getLong(Keys.SHUFFLE_FINISHED),
                     min.getLong(Keys.START_TIME)));
    } else {
      details.append(StringUtils.formatTimeDiff(
                min.getLong(Keys.FINISH_TIME),
                min.getLong(Keys.SHUFFLE_FINISHED)));
    }
    details.append("\nAverage time taken by ");
    details.append(taskType).append(" tasks: "); 
    details.append(StringUtils.formatTimeDiff(avg, 0));
    details.append("\nWorse performing ");
    details.append(taskType).append(" tasks: ");
    details.append("\nTaskId\t\tTimetaken");
    System.out.println(details.toString());
    for (int i = 0; i < showTasks && i < tasks.length; i++) {
      details.setLength(0);
      details.append(tasks[i].get(Keys.TASKID)).append(" ");
      if ("map".equals(taskType)) {
        details.append(StringUtils.formatTimeDiff(
                       tasks[i].getLong(Keys.FINISH_TIME),
                       tasks[i].getLong(Keys.START_TIME)));
      } else if ("shuffle".equals(taskType)) {
        details.append(StringUtils.formatTimeDiff(
                       tasks[i].getLong(Keys.SHUFFLE_FINISHED),
                       tasks[i].getLong(Keys.START_TIME)));
      } else {
        details.append(StringUtils.formatTimeDiff(
                       tasks[i].getLong(Keys.FINISH_TIME),
                       tasks[i].getLong(Keys.SHUFFLE_FINISHED)));
      }
      System.out.println(details.toString());
    }
  }
  
  private Comparator<JobHistory.Task> cMap = 
                                        new Comparator<JobHistory.Task>() {
    public int compare(JobHistory.Task t1, JobHistory.Task t2) {
      long l1 = t1.getLong(Keys.FINISH_TIME) - t1.getLong(Keys.START_TIME);
      long l2 = t2.getLong(Keys.FINISH_TIME) - t2.getLong(Keys.START_TIME);
      return (l2 < l1 ? -1 : (l2 == l1 ? 0 : 1));
    }
  };
  
  private Comparator<JobHistory.Task> cShuffle = 
    new Comparator<JobHistory.Task>() {
    public int compare(JobHistory.Task t1, JobHistory.Task t2) {
      long l1 = t1.getLong(Keys.SHUFFLE_FINISHED) - 
                t1.getLong(Keys.START_TIME);
      long l2 = t2.getLong(Keys.SHUFFLE_FINISHED) -
                t2.getLong(Keys.START_TIME);
      return (l2 < l1 ? -1 : (l2 == l1 ? 0 : 1));
    }
  };

  private Comparator<JobHistory.Task> cFinishShuffle = 
    new Comparator<JobHistory.Task>() {
    public int compare(JobHistory.Task t1, JobHistory.Task t2) {
      long l1 = t1.getLong(Keys.SHUFFLE_FINISHED); 
      long l2 = t2.getLong(Keys.SHUFFLE_FINISHED);
      return (l2 < l1 ? -1 : (l2 == l1 ? 0 : 1));
    }
  };

  private Comparator<JobHistory.Task> cFinishMapRed = 
    new Comparator<JobHistory.Task>() {
    public int compare(JobHistory.Task t1, JobHistory.Task t2) {
      long l1 = t1.getLong(Keys.FINISH_TIME); 
      long l2 = t2.getLong(Keys.FINISH_TIME);
      return (l2 < l1 ? -1 : (l2 == l1 ? 0 : 1));
    }
  };
  
  private Comparator<JobHistory.Task> cReduce = 
    new Comparator<JobHistory.Task>() {
    public int compare(JobHistory.Task t1, JobHistory.Task t2) {
      long l1 = t1.getLong(Keys.FINISH_TIME) -
                t1.getLong(Keys.SHUFFLE_FINISHED);
      long l2 = t2.getLong(Keys.FINISH_TIME) -
                t2.getLong(Keys.SHUFFLE_FINISHED);
      return (l2 < l1 ? -1 : (l2 == l1 ? 0 : 1));
    }
  }; 
}
