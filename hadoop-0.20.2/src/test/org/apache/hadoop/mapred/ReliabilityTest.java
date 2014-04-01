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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * This class tests reliability of the framework in the face of failures of 
 * both tasks and tasktrackers. Steps:
 * 1) Get the cluster status
 * 2) Get the number of slots in the cluster
 * 3) Spawn a sleepjob that occupies the entire cluster (with two waves of maps)
 * 4) Get the list of running attempts for the job
 * 5) Fail a few of them
 * 6) Now fail a few trackers (ssh)
 * 7) Job should run to completion
 * 8) The above is repeated for the Sort suite of job (randomwriter, sort,
 *    validator). All jobs must complete, and finally, the sort validation
 *    should succeed.
 * To run the test:
 * ./bin/hadoop --config <config> jar
 *   build/hadoop-<version>-test.jar MRReliabilityTest -libjars
 *   build/hadoop-<version>-examples.jar [-scratchdir <dir>]"
 *   
 *   The scratchdir is optional and by default the current directory on the client
 *   will be used as the scratch space. Note that password-less SSH must be set up 
 *   between the client machine from where the test is submitted, and the cluster 
 *   nodes where the test runs.
 *   
 *   The test should be run on a <b>free</b> cluster where there is no other parallel
 *   job submission going on. Submission of other jobs while the test runs can cause
 *   the tests/jobs submitted to fail.
 */

public class ReliabilityTest extends Configured implements Tool {

  private String dir;
  private static final Log LOG = LogFactory.getLog(ReliabilityTest.class); 

  private void displayUsage() {
    LOG.info("This must be run in only the distributed mode " +
    		"(LocalJobRunner not supported).\n\tUsage: MRReliabilityTest " +
    		"-libjars <path to hadoop-examples.jar> [-scratchdir <dir>]" +
    		"\n[-scratchdir] points to a scratch space on this host where temp" +
    		" files for this test will be created. Defaults to current working" +
    		" dir. \nPasswordless SSH must be set up between this host and the" +
    		" nodes which the test is going to use.\n"+
        "The test should be run on a free cluster with no parallel job submission" +
        " going on, as the test requires to restart TaskTrackers and kill tasks" +
        " any job submission while the tests are running can cause jobs/tests to fail");
    System.exit(-1);
  }
  
  public int run(String[] args) throws Exception {
    Configuration conf = getConf();
    if ("local".equals(conf.get("mapred.job.tracker", "local"))) {
      displayUsage();
    }
    String[] otherArgs = 
      new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length == 2) {
      if (otherArgs[0].equals("-scratchdir")) {
        dir = otherArgs[1];
      } else {
        displayUsage();
      }
    }
    else if (otherArgs.length == 0) {
      dir = System.getProperty("user.dir");
    } else {
      displayUsage();
    }
    
    //to protect against the case of jobs failing even when multiple attempts
    //fail, set some high values for the max attempts
    conf.setInt("mapred.map.max.attempts", 10);
    conf.setInt("mapred.reduce.max.attempts", 10);
    runSleepJobTest(new JobClient(new JobConf(conf)), conf);
    runSortJobTests(new JobClient(new JobConf(conf)), conf);
    return 0;
  }
  
  private void runSleepJobTest(final JobClient jc, final Configuration conf) 
  throws Exception {
    ClusterStatus c = jc.getClusterStatus();
    int maxMaps = c.getMaxMapTasks() * 2;
    int maxReduces = maxMaps;
    int mapSleepTime = (int)c.getTTExpiryInterval();
    int reduceSleepTime = mapSleepTime;
    String[] sleepJobArgs = new String[] {     
        "-m", Integer.toString(maxMaps), 
        "-r", Integer.toString(maxReduces),
        "-mt", Integer.toString(mapSleepTime),
        "-rt", Integer.toString(reduceSleepTime)};
    runTest(jc, conf, "org.apache.hadoop.examples.SleepJob", sleepJobArgs, 
        new KillTaskThread(jc, 2, 0.2f, false, 2),
        new KillTrackerThread(jc, 2, 0.4f, false, 1));
    LOG.info("SleepJob done");
  }
  
  private void runSortJobTests(final JobClient jc, final Configuration conf) 
  throws Exception {
    String inputPath = "my_reliability_test_input";
    String outputPath = "my_reliability_test_output";
    FileSystem fs = jc.getFs();
    fs.delete(new Path(inputPath), true);
    fs.delete(new Path(outputPath), true);
    runRandomWriterTest(jc, conf, inputPath);
    runSortTest(jc, conf, inputPath, outputPath);
    runSortValidatorTest(jc, conf, inputPath, outputPath);
  }
  
  private void runRandomWriterTest(final JobClient jc, 
      final Configuration conf, final String inputPath) 
  throws Exception {
    runTest(jc, conf, "org.apache.hadoop.examples.RandomWriter", 
        new String[]{inputPath}, 
        null, new KillTrackerThread(jc, 0, 0.4f, false, 1));
    LOG.info("RandomWriter job done");
  }
  
  private void runSortTest(final JobClient jc, final Configuration conf,
      final String inputPath, final String outputPath) 
  throws Exception {
    runTest(jc, conf, "org.apache.hadoop.examples.Sort", 
        new String[]{inputPath, outputPath},
        new KillTaskThread(jc, 2, 0.2f, false, 2),
        new KillTrackerThread(jc, 2, 0.8f, false, 1));
    LOG.info("Sort job done");
  }
  
  private void runSortValidatorTest(final JobClient jc, 
      final Configuration conf, final String inputPath, final String outputPath)
  throws Exception {
    runTest(jc, conf, "org.apache.hadoop.mapred.SortValidator", new String[] {
        "-sortInput", inputPath, "-sortOutput", outputPath},
        new KillTaskThread(jc, 2, 0.2f, false, 1),
        new KillTrackerThread(jc, 2, 0.8f, false, 1));  
    LOG.info("SortValidator job done");    
  }
  
  private String normalizeCommandPath(String command) {
    final String hadoopHome;
    if ((hadoopHome = System.getenv("HADOOP_HOME")) != null) {
      command = hadoopHome + "/" + command;
    }
    return command;
  }
  
  private void checkJobExitStatus(int status, String jobName) {
    if (status != 0) {
      LOG.info(jobName + " job failed with status: " + status);
      System.exit(status);
    } else {
      LOG.info(jobName + " done.");
    }
  }

  //Starts the job in a thread. It also starts the taskKill/tasktrackerKill
  //threads.
  private void runTest(final JobClient jc, final Configuration conf,
      final String jobClass, final String[] args, KillTaskThread killTaskThread,
      KillTrackerThread killTrackerThread) throws Exception {
    Thread t = new Thread("Job Test") {
      public void run() {
        try {
          Class<?> jobClassObj = conf.getClassByName(jobClass);
          int status = ToolRunner.run(conf, (Tool)(jobClassObj.newInstance()), 
              args);
          checkJobExitStatus(status, jobClass);
        } catch (Exception e) {
          LOG.fatal("JOB " + jobClass + " failed to run");
          System.exit(-1);
        }
      }
    };
    t.setDaemon(true);
    t.start();
    JobStatus[] jobs;
    //get the job ID. This is the job that we just submitted
    while ((jobs = jc.jobsToComplete()).length == 0) {
      LOG.info("Waiting for the job " + jobClass +" to start");
      Thread.sleep(1000);
    }
    JobID jobId = jobs[jobs.length - 1].getJobID();
    RunningJob rJob = jc.getJob(jobId);
    if(rJob.isComplete()) {
      LOG.error("The last job returned by the querying JobTracker is complete :" + 
          rJob.getJobID() + " .Exiting the test");
      System.exit(-1);
    }
    while (rJob.getJobState() == JobStatus.PREP) {
      LOG.info("JobID : " + jobId + " not started RUNNING yet");
      Thread.sleep(1000);
      rJob = jc.getJob(jobId);
    }
    if (killTaskThread != null) {
      killTaskThread.setRunningJob(rJob);
      killTaskThread.start();
      killTaskThread.join();
      LOG.info("DONE WITH THE TASK KILL/FAIL TESTS");
    }
    if (killTrackerThread != null) {
      killTrackerThread.setRunningJob(rJob);
      killTrackerThread.start();
      killTrackerThread.join();
      LOG.info("DONE WITH THE TESTS TO DO WITH LOST TASKTRACKERS");
    }
    t.join();
  }
  
  private class KillTrackerThread extends Thread {
    private volatile boolean killed = false;
    private JobClient jc;
    private RunningJob rJob;
    final private int thresholdMultiplier;
    private float threshold = 0.2f;
    private boolean onlyMapsProgress;
    private int numIterations;
    final private String slavesFile = dir + "/_reliability_test_slaves_file_";
    final String shellCommand = normalizeCommandPath("bin/slaves.sh");
    final private String STOP_COMMAND = "ps uwwx | grep java | grep " + 
    "org.apache.hadoop.mapred.TaskTracker"+ " |" + 
    " grep -v grep | tr -s ' ' | cut -d ' ' -f2 | xargs kill -s STOP";
    final private String RESUME_COMMAND = "ps uwwx | grep java | grep " + 
    "org.apache.hadoop.mapred.TaskTracker"+ " |" + 
    " grep -v grep | tr -s ' ' | cut -d ' ' -f2 | xargs kill -s CONT";
    //Only one instance must be active at any point
    public KillTrackerThread(JobClient jc, int threshaldMultiplier,
        float threshold, boolean onlyMapsProgress, int numIterations) {
      this.jc = jc;
      this.thresholdMultiplier = threshaldMultiplier;
      this.threshold = threshold;
      this.onlyMapsProgress = onlyMapsProgress;
      this.numIterations = numIterations;
      setDaemon(true);
    }
    public void setRunningJob(RunningJob rJob) {
      this.rJob = rJob;
    }
    public void kill() {
      killed = true;
    }
    public void run() {
      stopStartTrackers(true);
      if (!onlyMapsProgress) {
        stopStartTrackers(false);
      }
    }
    private void stopStartTrackers(boolean considerMaps) {
      if (considerMaps) {
        LOG.info("Will STOP/RESUME tasktrackers based on Maps'" +
                " progress");
      } else {
        LOG.info("Will STOP/RESUME tasktrackers based on " +
                "Reduces' progress");
      }
      LOG.info("Initial progress threshold: " + threshold + 
          ". Threshold Multiplier: " + thresholdMultiplier + 
          ". Number of iterations: " + numIterations);
      float thresholdVal = threshold;
      int numIterationsDone = 0;
      while (!killed) {
        try {
          float progress;
          if (jc.getJob(rJob.getID()).isComplete() ||
              numIterationsDone == numIterations) {
            break;
          }

          if (considerMaps) {
            progress = jc.getJob(rJob.getID()).mapProgress();
          } else {
            progress = jc.getJob(rJob.getID()).reduceProgress();
          }
          if (progress >= thresholdVal) {
            numIterationsDone++;
            ClusterStatus c;
            stopTaskTrackers((c = jc.getClusterStatus(true)));
            Thread.sleep((int)Math.ceil(1.5 * c.getTTExpiryInterval()));
            startTaskTrackers();
            thresholdVal = thresholdVal * thresholdMultiplier;
          }
          Thread.sleep(5000);
        } catch (InterruptedException ie) {
          killed = true;
          return;
        } catch (Exception e) {
          LOG.fatal(StringUtils.stringifyException(e));
        }
      }
    }
    private void stopTaskTrackers(ClusterStatus c) throws Exception {

      Collection <String> trackerNames = c.getActiveTrackerNames();
      ArrayList<String> trackerNamesList = new ArrayList<String>(trackerNames);
      Collections.shuffle(trackerNamesList);

      int count = 0;

      FileOutputStream fos = new FileOutputStream(new File(slavesFile));
      LOG.info(new Date() + " Stopping a few trackers");

      for (String tracker : trackerNamesList) {
        String host = convertTrackerNameToHostName(tracker);
        LOG.info(new Date() + " Marking tracker on host: " + host);
        fos.write((host + "\n").getBytes());
        if (count++ >= trackerNamesList.size()/2) {
          break;
        }
      }
      fos.close();

      runOperationOnTT("suspend");
    }

    private void startTaskTrackers() throws Exception {
      LOG.info(new Date() + " Resuming the stopped trackers");
      runOperationOnTT("resume");
      new File(slavesFile).delete();
    }
    
    private void runOperationOnTT(String operation) throws IOException {
      Map<String,String> hMap = new HashMap<String,String>();
      hMap.put("HADOOP_SLAVES", slavesFile);
      StringTokenizer strToken;
      if (operation.equals("suspend")) {
        strToken = new StringTokenizer(STOP_COMMAND, " ");
      } else {
        strToken = new StringTokenizer(RESUME_COMMAND, " ");
      }
      String commandArgs[] = new String[strToken.countTokens() + 1];
      int i = 0;
      commandArgs[i++] = shellCommand;
      while (strToken.hasMoreTokens()) {
        commandArgs[i++] = strToken.nextToken();
      }
      String output = Shell.execCommand(hMap, commandArgs);
      if (output != null && !output.equals("")) {
        LOG.info(output);
      }
    }

    private String convertTrackerNameToHostName(String trackerName) {
      // Convert the trackerName to it's host name
      int indexOfColon = trackerName.indexOf(":");
      String trackerHostName = (indexOfColon == -1) ? 
          trackerName : 
            trackerName.substring(0, indexOfColon);
      return trackerHostName.substring("tracker_".length());
    }

  }
  
  private class KillTaskThread extends Thread {

    private volatile boolean killed = false;
    private RunningJob rJob;
    private JobClient jc;
    final private int thresholdMultiplier;
    private float threshold = 0.2f;
    private boolean onlyMapsProgress;
    private int numIterations;
    public KillTaskThread(JobClient jc, int thresholdMultiplier, 
        float threshold, boolean onlyMapsProgress, int numIterations) {
      this.jc = jc;
      this.thresholdMultiplier = thresholdMultiplier;
      this.threshold = threshold;
      this.onlyMapsProgress = onlyMapsProgress;
      this.numIterations = numIterations;
      setDaemon(true);
    }
    public void setRunningJob(RunningJob rJob) {
      this.rJob = rJob;
    }
    public void kill() {
      killed = true;
    }
    public void run() {
      killBasedOnProgress(true);
      if (!onlyMapsProgress) {
        killBasedOnProgress(false);
      }
    }
    private void killBasedOnProgress(boolean considerMaps) {
      boolean fail = false;
      if (considerMaps) {
        LOG.info("Will kill tasks based on Maps' progress");
      } else {
        LOG.info("Will kill tasks based on Reduces' progress");
      }
      LOG.info("Initial progress threshold: " + threshold + 
          ". Threshold Multiplier: " + thresholdMultiplier + 
          ". Number of iterations: " + numIterations);
      float thresholdVal = threshold;
      int numIterationsDone = 0;
      while (!killed) {
        try {
          float progress;
          if (jc.getJob(rJob.getID()).isComplete() || 
              numIterationsDone == numIterations) {
            break;
          }
          if (considerMaps) {
            progress = jc.getJob(rJob.getID()).mapProgress();
          } else {
            progress = jc.getJob(rJob.getID()).reduceProgress();
          }
          if (progress >= thresholdVal) {
            numIterationsDone++;
            if (numIterationsDone > 0 && numIterationsDone % 2 == 0) {
              fail = true; //fail tasks instead of kill
            }
            ClusterStatus c = jc.getClusterStatus();

            LOG.info(new Date() + " Killing a few tasks");

            Collection<TaskAttemptID> runningTasks =
              new ArrayList<TaskAttemptID>();
            TaskReport mapReports[] = jc.getMapTaskReports(rJob.getID());
            for (TaskReport mapReport : mapReports) {
              if (mapReport.getCurrentStatus() == TIPStatus.RUNNING) {
                runningTasks.addAll(mapReport.getRunningTaskAttempts());
              }
            }
            if (runningTasks.size() > c.getTaskTrackers()/2) {
              int count = 0;
              for (TaskAttemptID t : runningTasks) {
                LOG.info(new Date() + " Killed task : " + t);
                rJob.killTask(t, fail);
                if (count++ > runningTasks.size()/2) { //kill 50%
                  break;
                }
              }
            }
            runningTasks.clear();
            TaskReport reduceReports[] = jc.getReduceTaskReports(rJob.getID());
            for (TaskReport reduceReport : reduceReports) {
              if (reduceReport.getCurrentStatus() == TIPStatus.RUNNING) {
                runningTasks.addAll(reduceReport.getRunningTaskAttempts());
              }
            }
            if (runningTasks.size() > c.getTaskTrackers()/2) {
              int count = 0;
              for (TaskAttemptID t : runningTasks) {
                LOG.info(new Date() + " Killed task : " + t);
                rJob.killTask(t, fail);
                if (count++ > runningTasks.size()/2) { //kill 50%
                  break;
                }
              }
            }
            thresholdVal = thresholdVal * thresholdMultiplier;
          }
          Thread.sleep(5000);
        } catch (InterruptedException ie) {
          killed = true;
        } catch (Exception e) {
          LOG.fatal(StringUtils.stringifyException(e));
        }
      }
    }
  }
  
  public static void main(String args[]) throws Exception {
    int res = ToolRunner.run(new Configuration(), new ReliabilityTest(), args);
    System.exit(res);
  }
}