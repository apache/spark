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
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.HashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TaskStatus.State;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.tools.rumen.Pre21JobHistoryConstants.Values;

/**
 * {@link ZombieJob} is a layer above {@link LoggedJob} raw JSON objects.
 * 
 * Each {@link ZombieJob} object represents a job in job history. For everything
 * that exists in job history, contents are returned unchanged faithfully. To
 * get input splits of a non-exist task, a non-exist task attempt, or an
 * ill-formed task attempt, proper objects are made up from statistical
 * sketches.
 */
@SuppressWarnings("deprecation")
public class ZombieJob implements JobStory {
  static final Log LOG = LogFactory.getLog(ZombieJob.class);
  private final LoggedJob job;
  private Map<TaskID, LoggedTask> loggedTaskMap;
  private Map<TaskAttemptID, LoggedTaskAttempt> loggedTaskAttemptMap;
  private final Random random;
  private InputSplit[] splits;
  private final ClusterStory cluster;
  private JobConf jobConf;

  private long seed;
  private boolean hasRandomSeed = false;

  private Map<LoggedDiscreteCDF, CDFRandomGenerator> interpolatorMap =
      new HashMap<LoggedDiscreteCDF, CDFRandomGenerator>();

  // TODO: Fix ZombieJob to initialize this correctly from observed data
  double rackLocalOverNodeLocal = 1.5;
  double rackRemoteOverNodeLocal = 3.0;

  /**
   * This constructor creates a {@link ZombieJob} with the same semantics as the
   * {@link LoggedJob} passed in this parameter
   * 
   * @param job
   *          The dead job this ZombieJob instance is based on.
   * @param cluster
   *          The cluster topology where the dead job ran on. This argument can
   *          be null if we do not have knowledge of the cluster topology.
   * @param seed
   *          Seed for the random number generator for filling in information
   *          not available from the ZombieJob.
   */
  public ZombieJob(LoggedJob job, ClusterStory cluster, long seed) {
    if (job == null) {
      throw new IllegalArgumentException("job is null");
    }
    this.job = job;
    this.cluster = cluster;
    random = new Random(seed);
    this.seed = seed;
    hasRandomSeed = true;
  }

  /**
   * This constructor creates a {@link ZombieJob} with the same semantics as the
   * {@link LoggedJob} passed in this parameter
   * 
   * @param job
   *          The dead job this ZombieJob instance is based on.
   * @param cluster
   *          The cluster topology where the dead job ran on. This argument can
   *          be null if we do not have knowledge of the cluster topology.
   */
  public ZombieJob(LoggedJob job, ClusterStory cluster) {
    this(job, cluster, System.nanoTime());
  }

  private static State convertState(Values status) {
    if (status == Values.SUCCESS) {
      return State.SUCCEEDED;
    } else if (status == Values.FAILED) {
      return State.FAILED;
    } else if (status == Values.KILLED) {
      return State.KILLED;
    } else {
      throw new IllegalArgumentException("unknown status " + status);
    }
  }

  @Override
  public synchronized JobConf getJobConf() {
    if (jobConf == null) {
      // TODO : add more to jobConf ?
      jobConf = new JobConf();
      jobConf.setJobName(getName());
      jobConf.setUser(getUser());
      jobConf.setNumMapTasks(getNumberMaps());
      jobConf.setNumReduceTasks(getNumberReduces());
      jobConf.setQueueName(getQueueName());
    }
    return jobConf;
  }
  
  @Override
  public InputSplit[] getInputSplits() {
    if (splits == null) {
      List<InputSplit> splitsList = new ArrayList<InputSplit>();
      Path emptyPath = new Path("/");
      int totalHosts = 0; // use to determine avg # of hosts per split.
      for (LoggedTask mapTask : job.getMapTasks()) {
        Pre21JobHistoryConstants.Values taskType = mapTask.getTaskType();
        if (taskType != Pre21JobHistoryConstants.Values.MAP) {
          LOG.warn("TaskType for a MapTask is not Map. task="
              + mapTask.getTaskID() + " type="
              + ((taskType == null) ? "null" : taskType.toString()));
          continue;
        }
        List<LoggedLocation> locations = mapTask.getPreferredLocations();
        List<String> hostList = new ArrayList<String>();
        if (locations != null) {
          for (LoggedLocation location : locations) {
            List<String> layers = location.getLayers();
            if (layers.size() == 0) {
              LOG.warn("Bad location layer format for task "+mapTask.getTaskID());
              continue;
            }
            String host = layers.get(layers.size() - 1);
            if (host == null) {
              LOG.warn("Bad location layer format for task "+mapTask.getTaskID() + ": " + layers);
              continue;
            }
            hostList.add(host);
          }
        }
        String[] hosts = hostList.toArray(new String[hostList.size()]);
        totalHosts += hosts.length;
        long mapInputBytes = getTaskInfo(mapTask).getInputBytes();
        if (mapInputBytes < 0) {
          LOG.warn("InputBytes for task "+mapTask.getTaskID()+" is not defined.");
          mapInputBytes = 0;
        }
       
        splitsList.add(new FileSplit(emptyPath, 0, mapInputBytes, hosts));
      }

      // If not all map tasks are in job trace, should make up some splits
      // for missing map tasks.
      int totalMaps = job.getTotalMaps();
      if (totalMaps < splitsList.size()) {
        LOG.warn("TotalMaps for job " + job.getJobID()
            + " is less than the total number of map task descriptions ("
            + totalMaps + "<" + splitsList.size() + ").");
      }

      int avgHostPerSplit;
      if (splitsList.size() == 0) {
        avgHostPerSplit = 3;
      } else {
        avgHostPerSplit = totalHosts / splitsList.size();
        if (avgHostPerSplit == 0) {
          avgHostPerSplit = 3;
        }
      }

      for (int i = splitsList.size(); i < totalMaps; i++) {
        if (cluster == null) {
          splitsList.add(new FileSplit(emptyPath, 0, 0, new String[0]));
        } else {
          MachineNode[] mNodes = cluster.getRandomMachines(avgHostPerSplit);
          String[] hosts = new String[mNodes.length];
          for (int j = 0; j < hosts.length; ++j) {
            hosts[j] = mNodes[j].getName();
          }
          // TODO set size of a split to 0 now.
          splitsList.add(new FileSplit(emptyPath, 0, 0, hosts));
        }
      }

      splits = splitsList.toArray(new InputSplit[splitsList.size()]);
    }
    return splits;
  }

  @Override
  public String getName() {
    String jobName = job.getJobName();
    if (jobName == null) {
      return "(name unknown)";
    } else {
      return jobName;
    }
  }

  @Override
  public JobID getJobID() {
    return JobID.forName(getLoggedJob().getJobID());
  }

  private int sanitizeValue(int oldVal, int defaultVal, String name, String id) {
    if (oldVal == -1) {
      LOG.warn(name +" not defined for "+id);
      return defaultVal;
    }
    return oldVal;
  }
  
  @Override
  public int getNumberMaps() {
    return sanitizeValue(job.getTotalMaps(), 0, "NumberMaps", job.getJobID());
  }

  @Override
  public int getNumberReduces() {
    return sanitizeValue(job.getTotalReduces(), 0, "NumberReduces", job.getJobID());
  }

  @Override
  public Values getOutcome() {
    return job.getOutcome();
  }

  @Override
  public long getSubmissionTime() {
    return job.getSubmitTime() - job.getRelativeTime();
  }

  @Override
  public String getQueueName() {
    String queue = job.getQueue();
    return (queue == null)? JobConf.DEFAULT_QUEUE_NAME : queue;
  }
  
  /**
   * Getting the number of map tasks that are actually logged in the trace.
   * @return The number of map tasks that are actually logged in the trace.
   */
  public int getNumLoggedMaps() {
    return job.getMapTasks().size();
  }


  /**
   * Getting the number of reduce tasks that are actually logged in the trace.
   * @return The number of map tasks that are actually logged in the trace.
   */
  public int getNumLoggedReduces() {
    return job.getReduceTasks().size();
  }
  
  /**
   * Mask the job ID part in a {@link TaskID}.
   * 
   * @param taskId
   *          raw {@link TaskID} read from trace
   * @return masked {@link TaskID} with empty {@link JobID}.
   */
  private TaskID maskTaskID(TaskID taskId) {
    JobID jobId = new JobID();
    return new TaskID(jobId, taskId.isMap(), taskId.getId());
  }

  /**
   * Mask the job ID part in a {@link TaskAttemptID}.
   * 
   * @param attemptId
   *          raw {@link TaskAttemptID} read from trace
   * @return masked {@link TaskAttemptID} with empty {@link JobID}.
   */
  private TaskAttemptID maskAttemptID(TaskAttemptID attemptId) {
    JobID jobId = new JobID();
    TaskID taskId = attemptId.getTaskID();
    return new TaskAttemptID(jobId.getJtIdentifier(), jobId.getId(),
        attemptId.isMap(), taskId.getId(), attemptId.getId());
  }

  private LoggedTask sanitizeLoggedTask(LoggedTask task) {
    if (task == null) {
      return null;
    }
    if (task.getTaskType() == null) {
      LOG.warn("Task " + task.getTaskID() + " has nulll TaskType");
      return null;
    }
    if (task.getTaskStatus() == null) {
      LOG.warn("Task " + task.getTaskID() + " has nulll TaskStatus");
      return null;
    }
    return task;
  }

  private LoggedTaskAttempt sanitizeLoggedTaskAttempt(LoggedTaskAttempt attempt) {
    if (attempt == null) {
      return null;
    }
    if (attempt.getResult() == null) {
      LOG.warn("TaskAttempt " + attempt.getResult() + " has nulll Result");
      return null;
    }

    return attempt;
  }

  /**
   * Build task mapping and task attempt mapping, to be later used to find
   * information of a particular {@link TaskID} or {@link TaskAttemptID}.
   */
  private synchronized void buildMaps() {
    if (loggedTaskMap == null) {
      loggedTaskMap = new HashMap<TaskID, LoggedTask>();
      loggedTaskAttemptMap = new HashMap<TaskAttemptID, LoggedTaskAttempt>();
      
      for (LoggedTask map : job.getMapTasks()) {
        map = sanitizeLoggedTask(map);
        if (map != null) {
          loggedTaskMap.put(maskTaskID(TaskID.forName(map.taskID)), map);

          for (LoggedTaskAttempt mapAttempt : map.getAttempts()) {
            mapAttempt = sanitizeLoggedTaskAttempt(mapAttempt);
            if (mapAttempt != null) {
              TaskAttemptID id = TaskAttemptID.forName(mapAttempt
                  .getAttemptID());
              loggedTaskAttemptMap.put(maskAttemptID(id), mapAttempt);
            }
          }
        }
      }
      for (LoggedTask reduce : job.getReduceTasks()) {
        reduce = sanitizeLoggedTask(reduce);
        if (reduce != null) {
          loggedTaskMap.put(maskTaskID(TaskID.forName(reduce.taskID)), reduce);

          for (LoggedTaskAttempt reduceAttempt : reduce.getAttempts()) {
            reduceAttempt = sanitizeLoggedTaskAttempt(reduceAttempt);
            if (reduceAttempt != null) {
              TaskAttemptID id = TaskAttemptID.forName(reduceAttempt
                  .getAttemptID());
              loggedTaskAttemptMap.put(maskAttemptID(id), reduceAttempt);
            }
          }
        }
      }

      // TODO: do not care about "other" tasks, "setup" or "clean"
    }
  }

  @Override
  public String getUser() {
    String retval = job.getUser();
    return (retval==null)?"(unknown)":retval;
  }

  /**
   * Get the underlining {@link LoggedJob} object read directly from the trace.
   * This is mainly for debugging.
   * 
   * @return the underlining {@link LoggedJob} object
   */
  public LoggedJob getLoggedJob() {
    return job;
  }

  /**
   * Get a {@link TaskAttemptInfo} with a {@link TaskAttemptID} associated with
   * taskType, taskNumber, and taskAttemptNumber. This function does not care
   * about locality, and follows the following decision logic: 1. Make up a
   * {@link TaskAttemptInfo} if the task attempt is missing in trace, 2. Make up
   * a {@link TaskAttemptInfo} if the task attempt has a KILLED final status in
   * trace, 3. Otherwise (final state is SUCCEEDED or FAILED), construct the
   * {@link TaskAttemptInfo} from the trace.
   */
  public TaskAttemptInfo getTaskAttemptInfo(TaskType taskType, int taskNumber,
      int taskAttemptNumber) {
    // does not care about locality. assume default locality is NODE_LOCAL.
    // But if both task and task attempt exist in trace, use logged locality.
    int locality = 0;
    LoggedTask loggedTask = getLoggedTask(taskType, taskNumber);
    if (loggedTask == null) {
      // TODO insert parameters
      TaskInfo taskInfo = new TaskInfo(0, 0, 0, 0, 0);
      return makeUpTaskAttemptInfo(taskType, taskInfo, taskAttemptNumber,
          taskNumber, locality);
    }

    LoggedTaskAttempt loggedAttempt = getLoggedTaskAttempt(taskType,
        taskNumber, taskAttemptNumber);
    if (loggedAttempt == null) {
      // Task exists, but attempt is missing.
      TaskInfo taskInfo = getTaskInfo(loggedTask);
      return makeUpTaskAttemptInfo(taskType, taskInfo, taskAttemptNumber,
          taskNumber, locality);
    } else {
      // TODO should we handle killed attempts later?
      if (loggedAttempt.getResult()== Values.KILLED) {
        TaskInfo taskInfo = getTaskInfo(loggedTask);
        return makeUpTaskAttemptInfo(taskType, taskInfo, taskAttemptNumber,
            taskNumber, locality);
      } else {
        return getTaskAttemptInfo(loggedTask, loggedAttempt);
      }
    }
  }

  @Override
  public TaskInfo getTaskInfo(TaskType taskType, int taskNumber) {
    return getTaskInfo(getLoggedTask(taskType, taskNumber));
  }

  /**
   * Get a {@link TaskAttemptInfo} with a {@link TaskAttemptID} associated with
   * taskType, taskNumber, and taskAttemptNumber. This function considers
   * locality, and follows the following decision logic: 1. Make up a
   * {@link TaskAttemptInfo} if the task attempt is missing in trace, 2. Make up
   * a {@link TaskAttemptInfo} if the task attempt has a KILLED final status in
   * trace, 3. If final state is FAILED, construct a {@link TaskAttemptInfo}
   * from the trace, without considering locality. 4. If final state is
   * SUCCEEDED, construct a {@link TaskAttemptInfo} from the trace, with runtime
   * scaled according to locality in simulation and locality in trace.
   */
  @Override
  public TaskAttemptInfo getMapTaskAttemptInfoAdjusted(int taskNumber,
      int taskAttemptNumber, int locality) {
    TaskType taskType = TaskType.MAP;
    LoggedTask loggedTask = getLoggedTask(taskType, taskNumber);
    if (loggedTask == null) {
      // TODO insert parameters
      TaskInfo taskInfo = new TaskInfo(0, 0, 0, 0, 0);
      return makeUpTaskAttemptInfo(taskType, taskInfo, taskAttemptNumber,
          taskNumber, locality);
    }
    LoggedTaskAttempt loggedAttempt = getLoggedTaskAttempt(taskType,
        taskNumber, taskAttemptNumber);
    if (loggedAttempt == null) {
      // Task exists, but attempt is missing.
      TaskInfo taskInfo = getTaskInfo(loggedTask);
      return makeUpTaskAttemptInfo(taskType, taskInfo, taskAttemptNumber,
          taskNumber, locality);
    } else {
      // Task and TaskAttempt both exist.
      if (loggedAttempt.getResult() == Values.KILLED) {
        TaskInfo taskInfo = getTaskInfo(loggedTask);
        return makeUpTaskAttemptInfo(taskType, taskInfo, taskAttemptNumber,
            taskNumber, locality);
      } else if (loggedAttempt.getResult() == Values.FAILED) {
        /**
         * FAILED attempt is not affected by locality however, made-up FAILED
         * attempts ARE affected by locality, since statistics are present for
         * attempts of different locality.
         */
        return getTaskAttemptInfo(loggedTask, loggedAttempt);
      } else if (loggedAttempt.getResult() == Values.SUCCESS) {
        int loggedLocality = getLocality(loggedTask, loggedAttempt);
        if (locality == loggedLocality) {
          return getTaskAttemptInfo(loggedTask, loggedAttempt);
        } else {
          // attempt succeeded in trace. It is scheduled in simulation with
          // a different locality.
          return scaleInfo(loggedTask, loggedAttempt, locality, loggedLocality,
              rackLocalOverNodeLocal, rackRemoteOverNodeLocal);
        }
      } else {
        throw new IllegalArgumentException(
            "attempt result is not SUCCEEDED, FAILED or KILLED: "
                + loggedAttempt.getResult());
      }
    }
  }

  private long sanitizeTaskRuntime(long time, String id) {
    if (time < 0) {
      LOG.warn("Negative running time for task "+id+": "+time);
      return 100L; // set default to 100ms.
    }
    return time;
  }
  
  @SuppressWarnings("hiding") 
  private TaskAttemptInfo scaleInfo(LoggedTask loggedTask,
      LoggedTaskAttempt loggedAttempt, int locality, int loggedLocality,
      double rackLocalOverNodeLocal, double rackRemoteOverNodeLocal) {
    TaskInfo taskInfo = getTaskInfo(loggedTask);
    double[] factors = new double[] { 1.0, rackLocalOverNodeLocal,
        rackRemoteOverNodeLocal };
    double scaleFactor = factors[locality] / factors[loggedLocality];
    State state = convertState(loggedAttempt.getResult());
    if (loggedTask.getTaskType() == Values.MAP) {
      long taskTime = 0;
      if (loggedAttempt.getStartTime() == 0) {
        taskTime = makeUpMapRuntime(state, locality);
      } else {
        taskTime = loggedAttempt.getFinishTime() - loggedAttempt.getStartTime();
      }
      taskTime = sanitizeTaskRuntime(taskTime, loggedAttempt.getAttemptID());
      taskTime *= scaleFactor;
      return new MapTaskAttemptInfo(state, taskInfo, taskTime);
    } else {
      throw new IllegalArgumentException("taskType can only be MAP: "
          + loggedTask.getTaskType());
    }
  }

  private int getLocality(LoggedTask loggedTask, LoggedTaskAttempt loggedAttempt) {
    int distance = cluster.getMaximumDistance();
    String rackHostName = loggedAttempt.getHostName();
    if (rackHostName == null) {
      return distance;
    }
    MachineNode mn = getMachineNode(rackHostName);
    if (mn == null) {
      return distance;
    }
    List<LoggedLocation> locations = loggedTask.getPreferredLocations();
    if (locations != null) {
      for (LoggedLocation location : locations) {
        List<String> layers = location.getLayers();
        if ((layers == null) || (layers.isEmpty())) {
          continue;
        }
        String dataNodeName = layers.get(layers.size()-1);
        MachineNode dataNode = cluster.getMachineByName(dataNodeName);
        if (dataNode != null) {
          distance = Math.min(distance, cluster.distance(mn, dataNode));
        }
      }
    }
    return distance;
  }

  private MachineNode getMachineNode(String rackHostName) {
    ParsedHost parsedHost = ParsedHost.parse(rackHostName);
    String hostName = (parsedHost == null) ? rackHostName 
                                           : parsedHost.getNodeName();
    if (hostName == null) {
      return null;
    }
    return (cluster == null) ? null : cluster.getMachineByName(hostName);
  }

  private TaskAttemptInfo getTaskAttemptInfo(LoggedTask loggedTask,
      LoggedTaskAttempt loggedAttempt) {
    TaskInfo taskInfo = getTaskInfo(loggedTask);
    State state = convertState(loggedAttempt.getResult());
    if (loggedTask.getTaskType() == Values.MAP) {
      long taskTime;
      if (loggedAttempt.getStartTime() == 0) {
        int locality = getLocality(loggedTask, loggedAttempt);
        taskTime = makeUpMapRuntime(state, locality);
      } else {
        taskTime = loggedAttempt.getFinishTime() - loggedAttempt.getStartTime();
      }
      taskTime = sanitizeTaskRuntime(taskTime, loggedAttempt.getAttemptID());
      return new MapTaskAttemptInfo(state, taskInfo, taskTime);
    } else if (loggedTask.getTaskType() == Values.REDUCE) {
      long startTime = loggedAttempt.getStartTime();
      long mergeDone = loggedAttempt.getSortFinished();
      long shuffleDone = loggedAttempt.getShuffleFinished();
      long finishTime = loggedAttempt.getFinishTime();
      if (startTime <= 0 || startTime >= finishTime) {
        // have seen startTime>finishTime.
        // haven't seen reduce task with startTime=0 ever. But if this happens,
        // make up a reduceTime with no shuffle/merge.
        long reduceTime = makeUpReduceRuntime(state);
        return new ReduceTaskAttemptInfo(state, taskInfo, 0, 0, reduceTime);
      } else {
        if (shuffleDone <= 0) {
          shuffleDone = startTime;
        }
        if (mergeDone <= 0) {
          mergeDone = finishTime;
        }
        long shuffleTime = shuffleDone - startTime;
        long mergeTime = mergeDone - shuffleDone;
        long reduceTime = finishTime - mergeDone;
        reduceTime = sanitizeTaskRuntime(reduceTime, loggedAttempt.getAttemptID());
        
        return new ReduceTaskAttemptInfo(state, taskInfo, shuffleTime,
            mergeTime, reduceTime);
      }
    } else {
      throw new IllegalArgumentException("taskType for "
          + loggedTask.getTaskID() + " is neither MAP nor REDUCE: "
          + loggedTask.getTaskType());
    }
  }

  private TaskInfo getTaskInfo(LoggedTask loggedTask) {
    List<LoggedTaskAttempt> attempts = loggedTask.getAttempts();

    long inputBytes = -1;
    long inputRecords = -1;
    long outputBytes = -1;
    long outputRecords = -1;
    long heapMegabytes = -1;

    Values type = loggedTask.getTaskType();
    if ((type != Values.MAP) && (type != Values.REDUCE)) {
      throw new IllegalArgumentException(
          "getTaskInfo only supports MAP or REDUCE tasks: " + type.toString()
              + " for task = " + loggedTask.getTaskID());
    }

    for (LoggedTaskAttempt attempt : attempts) {
      attempt = sanitizeLoggedTaskAttempt(attempt);
      // ignore bad attempts or unsuccessful attempts.
      if ((attempt == null) || (attempt.getResult() != Values.SUCCESS)) {
        continue;
      }

      if (type == Values.MAP) {
        inputBytes = attempt.getHdfsBytesRead();
        inputRecords = attempt.getMapInputRecords();
        outputBytes =
            (job.getTotalReduces() > 0) ? attempt.getMapOutputBytes() : attempt
                .getHdfsBytesWritten();
        outputRecords = attempt.getMapOutputRecords();
        heapMegabytes =
            (job.getJobMapMB() > 0) ? job.getJobMapMB() : job
                .getHeapMegabytes();
      } else {
        inputBytes = attempt.getReduceShuffleBytes();
        inputRecords = attempt.getReduceInputRecords();
        outputBytes = attempt.getHdfsBytesWritten();
        outputRecords = attempt.getReduceOutputRecords();
        heapMegabytes =
            (job.getJobReduceMB() > 0) ? job.getJobReduceMB() : job
                .getHeapMegabytes();
      }
      break;
    }

    TaskInfo taskInfo =
        new TaskInfo(inputBytes, (int) inputRecords, outputBytes,
            (int) outputRecords, (int) heapMegabytes);
    return taskInfo;
  }

  private TaskAttemptID makeTaskAttemptID(TaskType taskType, int taskNumber,
      int taskAttemptNumber) {
    return new TaskAttemptID(new TaskID(JobID.forName(job.getJobID()),
        TaskType.MAP == taskType, taskNumber), taskAttemptNumber);
  }
  
  private TaskAttemptInfo makeUpTaskAttemptInfo(TaskType taskType, TaskInfo taskInfo,
      int taskAttemptNumber, int taskNumber, int locality) {
    if (taskType == TaskType.MAP) {
      State state = State.SUCCEEDED;
      long runtime = 0;

      // make up state
      state = makeUpState(taskAttemptNumber, job.getMapperTriesToSucceed());
      runtime = makeUpMapRuntime(state, locality);
      runtime = sanitizeTaskRuntime(runtime, makeTaskAttemptID(taskType,
          taskNumber, taskAttemptNumber).toString());
      TaskAttemptInfo tai = new MapTaskAttemptInfo(state, taskInfo, runtime);
      return tai;
    } else if (taskType == TaskType.REDUCE) {
      State state = State.SUCCEEDED;
      long shuffleTime = 0;
      long sortTime = 0;
      long reduceTime = 0;

      // TODO make up state
      // state = makeUpState(taskAttemptNumber, job.getReducerTriesToSucceed());
      reduceTime = makeUpReduceRuntime(state);
      TaskAttemptInfo tai = new ReduceTaskAttemptInfo(state, taskInfo,
          shuffleTime, sortTime, reduceTime);
      return tai;
    }

    throw new IllegalArgumentException("taskType is neither MAP nor REDUCE: "
        + taskType);
  }

  private long makeUpReduceRuntime(State state) {
    long reduceTime = 0;
    for (int i = 0; i < 5; i++) {
      reduceTime = doMakeUpReduceRuntime(state);
      if (reduceTime >= 0) {
        return reduceTime;
      }
    }
    return 0;
  }

  private long doMakeUpReduceRuntime(State state) {
    long reduceTime;
    try {
      if (state == State.SUCCEEDED) {
        reduceTime = makeUpRuntime(job.getSuccessfulReduceAttemptCDF());
      } else if (state == State.FAILED) {
        reduceTime = makeUpRuntime(job.getFailedReduceAttemptCDF());
      } else {
        throw new IllegalArgumentException(
            "state is neither SUCCEEDED nor FAILED: " + state);
      }
      return reduceTime;
    } catch (NoValueToMakeUpRuntime e) {
      return 0;
    }
  }

  private long makeUpMapRuntime(State state, int locality) {
    long runtime;
    // make up runtime
    if (state == State.SUCCEEDED || state == State.FAILED) {
      List<LoggedDiscreteCDF> cdfList =
          state == State.SUCCEEDED ? job.getSuccessfulMapAttemptCDFs() : job
              .getFailedMapAttemptCDFs();
      // XXX MapCDFs is a ArrayList of 4 possible groups: distance=0, 1, 2, and
      // the last group is "distance cannot be determined". All pig jobs
      // would have only the 4th group, and pig tasks usually do not have
      // any locality, so this group should count as "distance=2".
      // However, setup/cleanup tasks are also counted in the 4th group.
      // These tasks do not make sense.
      try {
        runtime = makeUpRuntime(cdfList.get(locality));
      } catch (NoValueToMakeUpRuntime e) {
        runtime = makeUpRuntime(cdfList);
      }
    } else {
      throw new IllegalArgumentException(
          "state is neither SUCCEEDED nor FAILED: " + state);
    }
    return runtime;
  }

  /**
   * Perform a weighted random selection on a list of CDFs, and produce a random
   * variable using the selected CDF.
   * 
   * @param mapAttemptCDFs
   *          A list of CDFs for the distribution of runtime for the 1st, 2nd,
   *          ... map attempts for the job.
   */
  private long makeUpRuntime(List<LoggedDiscreteCDF> mapAttemptCDFs) {
    int total = 0;
    for (LoggedDiscreteCDF cdf : mapAttemptCDFs) {
      total += cdf.getNumberValues();
    }
    if (total == 0) {
      return -1;
    }
    int index = random.nextInt(total);
    for (LoggedDiscreteCDF cdf : mapAttemptCDFs) {
      if (index >= cdf.getNumberValues()) {
        index -= cdf.getNumberValues();
      } else {
        if (index < 0) {
          throw new IllegalStateException("application error");
        }
        return makeUpRuntime(cdf);
      }
    }
    throw new IllegalStateException("not possible to get here");
  }

  private long makeUpRuntime(LoggedDiscreteCDF loggedDiscreteCDF) {
    /*
     * We need this odd-looking code because if a seed exists we need to ensure
     * that only one interpolator is generated per LoggedDiscreteCDF, but if no
     * seed exists then the potentially lengthy process of making an
     * interpolator can happen outside the lock. makeUpRuntimeCore only locks
     * around the two hash map accesses.
     */
    if (hasRandomSeed) {
      synchronized (interpolatorMap) {
        return makeUpRuntimeCore(loggedDiscreteCDF);
      }
    }

    return makeUpRuntimeCore(loggedDiscreteCDF);
  }

  private long makeUpRuntimeCore(LoggedDiscreteCDF loggedDiscreteCDF) {
    CDFRandomGenerator interpolator;

    synchronized (interpolatorMap) {
      interpolator = interpolatorMap.get(loggedDiscreteCDF);
    }

    if (interpolator == null) {
      if (loggedDiscreteCDF.getNumberValues() == 0) {
        throw new NoValueToMakeUpRuntime("no value to use to make up runtime");
      }

      interpolator =
          hasRandomSeed ? new CDFPiecewiseLinearRandomGenerator(
              loggedDiscreteCDF, ++seed)
              : new CDFPiecewiseLinearRandomGenerator(loggedDiscreteCDF);

      /*
       * It doesn't matter if we compute and store an interpolator twice because
       * the two instances will be semantically identical and stateless, unless
       * we're seeded, in which case we're not stateless but this code will be
       * called synchronizedly.
       */
      synchronized (interpolatorMap) {
        interpolatorMap.put(loggedDiscreteCDF, interpolator);
      }
    }

    return interpolator.randomValue();
  }

  static private class NoValueToMakeUpRuntime extends IllegalArgumentException {
    static final long serialVersionUID = 1L;

    NoValueToMakeUpRuntime() {
      super();
    }

    NoValueToMakeUpRuntime(String detailMessage) {
      super(detailMessage);
    }

    NoValueToMakeUpRuntime(String detailMessage, Throwable cause) {
      super(detailMessage, cause);
    }

    NoValueToMakeUpRuntime(Throwable cause) {
      super(cause);
    }
  }

  private State makeUpState(int taskAttemptNumber, double[] numAttempts) {
    if (taskAttemptNumber >= numAttempts.length - 1) {
      // always succeed
      return State.SUCCEEDED;
    } else {
      double pSucceed = numAttempts[taskAttemptNumber];
      double pFail = 0;
      for (int i = taskAttemptNumber + 1; i < numAttempts.length; i++) {
        pFail += numAttempts[i];
      }
      return (random.nextDouble() < pSucceed / (pSucceed + pFail)) ? State.SUCCEEDED
          : State.FAILED;
    }
  }

  private TaskID getMaskedTaskID(TaskType taskType, int taskNumber) {
    return new TaskID(new JobID(), TaskType.MAP == taskType, taskNumber);
  }

  private LoggedTask getLoggedTask(TaskType taskType, int taskNumber) {
    buildMaps();
    return loggedTaskMap.get(getMaskedTaskID(taskType, taskNumber));
  }

  private LoggedTaskAttempt getLoggedTaskAttempt(TaskType taskType,
      int taskNumber, int taskAttemptNumber) {
    buildMaps();
    TaskAttemptID id =
        new TaskAttemptID(getMaskedTaskID(taskType, taskNumber),
            taskAttemptNumber);
    return loggedTaskAttemptMap.get(id);
  }

}
