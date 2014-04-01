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
import java.util.List;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.mapred.JobHistory.Keys;
import org.apache.hadoop.mapred.JobTracker.RetireJobInfo;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapred.TaskTrackerStatus;
import org.apache.hadoop.mapred.StatisticsCollector;
import org.apache.hadoop.mapred.StatisticsCollectionHandler;
import org.apache.hadoop.mapreduce.server.jobtracker.TaskTracker;
import org.apache.hadoop.mapreduce.test.system.JTProtocol;
import org.apache.hadoop.mapreduce.test.system.JobInfo;
import org.apache.hadoop.mapreduce.test.system.TTInfo;
import org.apache.hadoop.mapreduce.test.system.TaskInfo;
import org.apache.hadoop.mapreduce.ClusterMetrics;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.test.system.DaemonProtocol;
import org.apache.hadoop.util.Shell.ShellCommandExecutor;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.util.StringUtils;

/**
 * Aspect class which injects the code for {@link JobTracker} class.
 * 
 */
public privileged aspect JobTrackerAspect {


  private static JobTracker tracker;
  
  public Configuration JobTracker.getDaemonConf() throws IOException {
    return conf;
  }
  /**
   * Method to get the read only view of the job and its associated information.
   * 
   * @param jobID
   *          id of the job for which information is required.
   * @return JobInfo of the job requested
   * @throws IOException
   */
  public JobInfo JobTracker.getJobInfo(JobID jobID) throws IOException {
    JobInProgress jip = jobs.get(org.apache.hadoop.mapred.JobID
        .downgrade(jobID));
    if (jip == null) {
      LOG.warn("No job present for : " + jobID);
      return null;
    }
    JobInfo info;
    synchronized (jip) {
      info = jip.getJobInfo();
    }
    return info;
  }

  /**
   * Method to get the read only view of the task and its associated
   * information.
   * 
   * @param taskID
   * @return
   * @throws IOException
   */
  public TaskInfo JobTracker.getTaskInfo(TaskID taskID) throws IOException {
    TaskInProgress tip = getTip(org.apache.hadoop.mapred.TaskID
        .downgrade(taskID));

    if (tip == null) {
      LOG.warn("No task present for : " + taskID);
      return null;
    }
    return getTaskInfo(tip);
  }

  public TTInfo JobTracker.getTTInfo(String trackerName) throws IOException {
    org.apache.hadoop.mapreduce.server.jobtracker.TaskTracker tt = taskTrackers
        .get(trackerName);
    if (tt == null) {
      LOG.warn("No task tracker with name : " + trackerName + " found");
      return null;
    }
    TaskTrackerStatus status = tt.getStatus();
    TTInfo info = new TTInfoImpl(status.trackerName, status);
    return info;
  }

  // XXX Below two method don't reuse getJobInfo and getTaskInfo as there is a
  // possibility that retire job can run and remove the job from JT memory
  // during
  // processing of the RPC call.
  public JobInfo[] JobTracker.getAllJobInfo() throws IOException {
    List<JobInfo> infoList = new ArrayList<JobInfo>();
    synchronized (jobs) {
      for (JobInProgress jip : jobs.values()) {
        JobInfo info = jip.getJobInfo();
        infoList.add(info);
      }
    }
    return (JobInfo[]) infoList.toArray(new JobInfo[infoList.size()]);
  }

  public TaskInfo[] JobTracker.getTaskInfo(JobID jobID) throws IOException {
    JobInProgress jip = jobs.get(org.apache.hadoop.mapred.JobID
        .downgrade(jobID));
    if (jip == null) {
      LOG.warn("Unable to find job : " + jobID);
      return null;
    }
    List<TaskInfo> infoList = new ArrayList<TaskInfo>();
    synchronized (jip) {
      for (TaskInProgress tip : jip.setup) {
        infoList.add(getTaskInfo(tip));
      }
      for (TaskInProgress tip : jip.maps) {
        infoList.add(getTaskInfo(tip));
      }
      for (TaskInProgress tip : jip.reduces) {
        infoList.add(getTaskInfo(tip));
      }
      for (TaskInProgress tip : jip.cleanup) {
        infoList.add(getTaskInfo(tip));
      }
    }
    return (TaskInfo[]) infoList.toArray(new TaskInfo[infoList.size()]);
  }

  public TTInfo[] JobTracker.getAllTTInfo() throws IOException {
    List<TTInfo> infoList = new ArrayList<TTInfo>();
    synchronized (taskTrackers) {
      for (TaskTracker tt : taskTrackers.values()) {
        TaskTrackerStatus status = tt.getStatus();
        TTInfo info = new TTInfoImpl(status.trackerName, status);
        infoList.add(info);
      }
    }
    return (TTInfo[]) infoList.toArray(new TTInfo[infoList.size()]);
  }
  
  public boolean JobTracker.isJobRetired(JobID id) throws IOException {
    return retireJobs.get(
        org.apache.hadoop.mapred.JobID.downgrade(id))!=null?true:false;
  }
  
  public boolean JobTracker.isBlackListed(String trackerName) throws IOException {
    return isBlacklisted(trackerName);
  }

  public String JobTracker.getJobHistoryLocationForRetiredJob(
      JobID id) throws IOException {
    RetireJobInfo retInfo = retireJobs.get(
        org.apache.hadoop.mapred.JobID.downgrade(id));
    if(retInfo == null) {
      throw new IOException("The retired job information for the job : " 
          + id +" is not found");
    } else {
      return retInfo.getHistoryFile();
    }
  }
  pointcut getVersionAspect(String protocol, long clientVersion) : 
    execution(public long JobTracker.getProtocolVersion(String , 
      long) throws IOException) && args(protocol, clientVersion);

  long around(String protocol, long clientVersion) :  
    getVersionAspect(protocol, clientVersion) {
    if (protocol.equals(DaemonProtocol.class.getName())) {
      return DaemonProtocol.versionID;
    } else if (protocol.equals(JTProtocol.class.getName())) {
      return JTProtocol.versionID;
    } else {
      return proceed(protocol, clientVersion);
    }
  }

  /**
   * Point cut which monitors for the start of the jobtracker and sets the right
   * value if the jobtracker is started.
   * 
   * @param conf
   * @param jobtrackerIndentifier
   */
  pointcut jtConstructorPointCut(JobConf conf, String jobtrackerIndentifier) : 
        call(JobTracker.new(JobConf,String)) 
        && args(conf, jobtrackerIndentifier) ;

  after(JobConf conf, String jobtrackerIndentifier) 
    returning (JobTracker tracker): jtConstructorPointCut(conf, 
        jobtrackerIndentifier) {
    try {
      UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
      tracker.setUser(ugi.getShortUserName());
    } catch (IOException e) {
      tracker.LOG.warn("Unable to get the user information for the " +
      		"Jobtracker");
    }
    this.tracker = tracker;
    tracker.setReady(true);
  }
  
  private TaskInfo JobTracker.getTaskInfo(TaskInProgress tip) {
    TaskStatus[] status = tip.getTaskStatuses();
    if (status == null) {
      if (tip.isMapTask()) {
        status = new MapTaskStatus[]{};
      }
      else {
        status = new ReduceTaskStatus[]{};
      }
    }
    String[] trackers =
        (String[]) (tip.getActiveTasks().values()).toArray(new String[tip
            .getActiveTasks().values().size()]);
    TaskInfo info =
        new TaskInfoImpl(tip.getTIPId(), tip.getProgress(), tip
            .getActiveTasks().size(), tip.numKilledTasks(), tip
            .numTaskFailures(), status, (tip.isJobSetupTask() || tip
            .isJobCleanupTask()), trackers);
    return info;
  }
  
  /**
   * Get the job summary details from the jobtracker log files.
   * @param jobId - job id
   * @param filePattern - jobtracker log file pattern.
   * @return String - Job summary details of given job id.
   * @throws IOException if any I/O error occurs.
   */
  public String JobTracker.getJobSummaryFromLogs(JobID jobId,
      String filePattern) throws IOException {
    String pattern = "JobId=" + jobId.toString() + ",submitTime";
    String[] cmd = new String[] {
                   "bash",
                   "-c",
                   "grep -i " 
                 + pattern + " " 
                 + filePattern + " " 
                 + "| sed s/'JobSummary: '/'^'/g | cut -d'^' -f2"};
    ShellCommandExecutor shexec = new ShellCommandExecutor(cmd);
    shexec.execute();
    return shexec.getOutput();
  }
  
  /**
   * Get the job summary information for given job id.
   * @param jobId - job id.
   * @return String - Job summary details as key value pair.
   * @throws IOException if any I/O error occurs.
   */
  public String JobTracker.getJobSummaryInfo(JobID jobId) throws IOException {
    StringBuffer jobSummary = new StringBuffer();
    JobInProgress jip = jobs.
        get(org.apache.hadoop.mapred.JobID.downgrade(jobId));
    if (jip == null) {
      LOG.warn("Job has not been found - " + jobId);
      return null;
    }
    JobProfile profile = jip.getProfile();
    JobStatus status = jip.getStatus();
    final char[] charsToEscape = {StringUtils.COMMA, '=', 
        StringUtils.ESCAPE_CHAR};
    String user = StringUtils.escapeString(profile.getUser(), 
        StringUtils.ESCAPE_CHAR, charsToEscape);
    String queue = StringUtils.escapeString(profile.getQueueName(), 
        StringUtils.ESCAPE_CHAR, charsToEscape);
    Counters jobCounters = jip.getJobCounters();
    long mapSlotSeconds = (jobCounters.getCounter(
        JobInProgress.Counter.SLOTS_MILLIS_MAPS) + 
        jobCounters.getCounter(JobInProgress.
        Counter.FALLOW_SLOTS_MILLIS_MAPS)) / 1000;
    long reduceSlotSeconds = (jobCounters.getCounter(
        JobInProgress.Counter.SLOTS_MILLIS_REDUCES) + 
       jobCounters.getCounter(JobInProgress.
       Counter.FALLOW_SLOTS_MILLIS_REDUCES)) / 1000;
    jobSummary.append("jobId=");
    jobSummary.append(jip.getJobID());
    jobSummary.append(",");
    jobSummary.append("startTime=");
    jobSummary.append(jip.getStartTime());
    jobSummary.append(",");
    jobSummary.append("launchTime=");
    jobSummary.append(jip.getLaunchTime());
    jobSummary.append(",");
    jobSummary.append("finishTime=");
    jobSummary.append(jip.getFinishTime());
    jobSummary.append(",");
    jobSummary.append("numMaps=");
    jobSummary.append(jip.getTasks(TaskType.MAP).length);
    jobSummary.append(",");
    jobSummary.append("numSlotsPerMap=");
    jobSummary.append(jip.getNumSlotsPerMap() );
    jobSummary.append(",");
    jobSummary.append("numReduces=");
    jobSummary.append(jip.getTasks(TaskType.REDUCE).length);
    jobSummary.append(",");
    jobSummary.append("numSlotsPerReduce=");
    jobSummary.append(jip.getNumSlotsPerReduce());
    jobSummary.append(",");
    jobSummary.append("user=");
    jobSummary.append(user);
    jobSummary.append(",");
    jobSummary.append("queue=");
    jobSummary.append(queue);
    jobSummary.append(",");
    jobSummary.append("status=");
    jobSummary.append(JobStatus.getJobRunState(status.getRunState()));
    jobSummary.append(",");
    jobSummary.append("mapSlotSeconds=");
    jobSummary.append(mapSlotSeconds);
    jobSummary.append(",");
    jobSummary.append("reduceSlotsSeconds=");
    jobSummary.append(reduceSlotSeconds);
    jobSummary.append(",");
    jobSummary.append("clusterMapCapacity=");
    jobSummary.append(tracker.getClusterMetrics().getMapSlotCapacity());
    jobSummary.append(",");
    jobSummary.append("clusterReduceCapacity=");
    jobSummary.append(tracker.getClusterMetrics().getReduceSlotCapacity());
    return jobSummary.toString();
  }

  /**
   * This gets the value of one task tracker window in the tasktracker page. 
   *
   * @param TaskTrackerStatus, 
   * timePeriod and totalTasksOrSucceededTasks, which are requried to 
   * identify the window
   * @return The number of tasks info in a particular window in 
   * tasktracker page. 
   */
  public int JobTracker.getTaskTrackerLevelStatistics( 
      TaskTrackerStatus ttStatus, String timePeriod,
      String totalTasksOrSucceededTasks) throws IOException {

    LOG.info("ttStatus host :" + ttStatus.getHost());
    if (timePeriod.matches("since_start")) {
      StatisticsCollector.TimeWindow window = getStatistics().
          collector.DEFAULT_COLLECT_WINDOWS[0];
      return(getNumberOfTasks(window, ttStatus , 
          totalTasksOrSucceededTasks));
    } else if (timePeriod.matches("last_day")) {
      StatisticsCollector.TimeWindow window = getStatistics().
          collector.DEFAULT_COLLECT_WINDOWS[1];
      return(getNumberOfTasks(window, ttStatus, 
          totalTasksOrSucceededTasks));
    } else if (timePeriod.matches("last_hour")) {
      StatisticsCollector.TimeWindow window = getStatistics().
          collector.DEFAULT_COLLECT_WINDOWS[2];
      return(getNumberOfTasks(window, ttStatus , 
          totalTasksOrSucceededTasks));
    }
    return -1;
  }

  /**
   * Get Information for Time Period and TaskType box
   * from all tasktrackers
   *
   * @param 
   * timePeriod and totalTasksOrSucceededTasks, which are requried to
   * identify the window
   * @return The total number of tasks info for a particular column in
   * tasktracker page.
   */
  public int JobTracker.getInfoFromAllClients(String timePeriod,
      String totalTasksOrSucceededTasks) throws IOException {
   
    int totalTasksCount = 0;
    int totalTasksRanForJob = 0;
    for (TaskTracker tt : taskTrackers.values()) {
      TaskTrackerStatus ttStatus = tt.getStatus();
      String tasktrackerName = ttStatus.getHost();
      List<Integer> taskTrackerValues = new LinkedList<Integer>();
      JobTrackerStatistics.TaskTrackerStat ttStat = getStatistics().
             getTaskTrackerStat(ttStatus.getTrackerName());
      int totalTasks = getTaskTrackerLevelStatistics(
          ttStatus, timePeriod, totalTasksOrSucceededTasks);
      totalTasksCount += totalTasks;
    }
    return totalTasksCount;
  }

  private int JobTracker.getNumberOfTasks(StatisticsCollector.TimeWindow 
    window, TaskTrackerStatus ttStatus, String totalTasksOrSucceededTasks ) { 
    JobTrackerStatistics.TaskTrackerStat ttStat = getStatistics().
             getTaskTrackerStat(ttStatus.getTrackerName());
    if (totalTasksOrSucceededTasks.matches("total_tasks")) {
      return ttStat.totalTasksStat.getValues().
          get(window).getValue();
    } else if (totalTasksOrSucceededTasks.matches("succeeded_tasks")) {
      return ttStat.succeededTasksStat.getValues().
          get(window).getValue();
    }
    return -1;
  }

  /**
   * This gets the value of all task trackers windows in the tasktracker page.
   *
   * @param none,
   * @return StatisticsCollectionHandler class which holds the number
   * of all jobs ran from all tasktrackers, in the sequence given below
   * "since_start - total_tasks"
   * "since_start - succeeded_tasks"
   * "last_hour - total_tasks"
   * "last_hour - succeeded_tasks"
   * "last_day - total_tasks"
   * "last_day - succeeded_tasks"
   */
  public StatisticsCollectionHandler JobTracker.
    getInfoFromAllClientsForAllTaskType() throws Exception { 

    //The outer list will have a list of each tasktracker list.
    //The inner list will have a list of all number of tasks in 
    //one tasktracker.
    List<List<Integer>> ttInfoList = new LinkedList<List<Integer>>();

    // Go through each tasktracker and get all the number of tasks
    // six window's values of that tasktracker.Each window points to 
    // specific value for that tasktracker.  
    //"since_start - total_tasks"
    //"since_start - succeeded_tasks"
    //"last_hour - total_tasks"
    //"last_hour - succeeded_tasks"
    //"last_day - total_tasks"
    //"last_day - succeeded_tasks"

    for (TaskTracker tt : taskTrackers.values()) {
      TaskTrackerStatus ttStatus = tt.getStatus();
      String tasktrackerName = ttStatus.getHost();
      List<Integer> taskTrackerValues = new LinkedList<Integer>(); 
      JobTrackerStatistics.TaskTrackerStat ttStat = getStatistics().
             getTaskTrackerStat(ttStatus.getTrackerName());

      int value;
      int totalCount = 0;
      for (int i = 0; i < 3; i++) { 
        StatisticsCollector.TimeWindow window = getStatistics().
          collector.DEFAULT_COLLECT_WINDOWS[i];
        value=0;
        value = ttStat.totalTasksStat.getValues().
          get(window).getValue();
        taskTrackerValues.add(value);
        value=0;
        value  = ttStat.succeededTasksStat.getValues().
          get(window).getValue(); 
        taskTrackerValues.add(value);
      }
      ttInfoList.add(taskTrackerValues);
    }

    //The info is collected in the order described above  by going 
    //through each tasktracker list 
    int totalInfoValues = 0; 
    StatisticsCollectionHandler statisticsCollectionHandler = 
      new StatisticsCollectionHandler();
    for (int i = 0; i < 6; i++) {
      totalInfoValues = 0;
      for (int j = 0; j < ttInfoList.size(); j++) { 
         List<Integer> list = ttInfoList.get(j);
         totalInfoValues += list.get(i); 
      }
      switch (i) {
        case 0: statisticsCollectionHandler.
          setSinceStartTotalTasks(totalInfoValues);
          break;
        case 1: statisticsCollectionHandler.
          setSinceStartSucceededTasks(totalInfoValues);
          break;
        case 2: statisticsCollectionHandler.
          setLastHourTotalTasks(totalInfoValues);
          break;
        case 3: statisticsCollectionHandler.
          setLastHourSucceededTasks(totalInfoValues);
          break;
        case 4: statisticsCollectionHandler.
          setLastDayTotalTasks(totalInfoValues);
          break;
        case 5: statisticsCollectionHandler.
          setLastDaySucceededTasks(totalInfoValues);
          break;
      }
    } 
      return statisticsCollectionHandler;
  }

  /*
   * Get the Tasktrcker Heart beat interval 
   */
  public int JobTracker.getTaskTrackerHeartbeatInterval()
      throws Exception {
    return (getNextHeartbeatInterval());
  }
  
  //access the job data the method only does a get on read-only data
  //it does not return anything purposely, since the test case
  //does not require this but this can be extended in future
  public void JobTracker.accessHistoryData(JobID id) throws Exception {
    String location = getJobHistoryLocationForRetiredJob(id);
    Path logFile = new Path(location);
    FileSystem fs = logFile.getFileSystem(getConf());
    JobHistory.JobInfo jobInfo  = new JobHistory.JobInfo(id.toString());
    DefaultJobHistoryParser.parseJobTasks(location,
        jobInfo, fs);
    //Now read the info so two threads can access the info at the
    //same time from client side
    LOG.info("user " +jobInfo.get(Keys.USER));
    LOG.info("jobname "+jobInfo.get(Keys.JOBNAME));
    jobInfo.get(Keys.JOBCONF);
    jobInfo.getJobACLs();
  }
  
  /**
   * Verifies whether Node is decommissioned or not
   * @param
   * tasktracker Client host name
   * @return boolean true for Decommissoned and false for not decommissioned.
   */
  public boolean JobTracker.isNodeDecommissioned(String ttClientHostName)
      throws IOException {
    Set<String> excludedNodes = hostsReader.getExcludedHosts();
    LOG.info("ttClientHostName is :" + ttClientHostName);
    boolean b =  excludedNodes.contains(ttClientHostName);
    return b;
  }
}
