package org.apache.hadoop.mapred;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.examples.SleepJob;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.mapreduce.test.system.JTProtocol;
import org.apache.hadoop.mapreduce.test.system.JobInfo;
import org.apache.hadoop.mapreduce.test.system.MRCluster;
import org.apache.hadoop.mapreduce.test.system.JTClient; 
import org.apache.hadoop.mapreduce.test.system.TTClient;
import org.apache.hadoop.mapreduce.test.system.TTProtocol;
import org.apache.hadoop.mapreduce.test.system.TaskInfo;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.AfterClass;
import org.junit.Test;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Hashtable;

public class TestLostTaskTracker {
  private static final Log LOG = LogFactory
      .getLog(TestLostTaskTracker.class);
  private static MRCluster cluster;
  private static Configuration conf = new Configuration();
  private static Path inputDir = new Path("input");
  private static Path outputDir = new Path("output");
  private static String confFile = "mapred-site.xml";
  private JTProtocol wovenClient = null;
  private JobID jID = null;
  private JobInfo jInfo = null;
  private JTClient jtClient = null;

  @BeforeClass
  public static void before() throws Exception {
    String [] expExcludeList = {"java.net.ConnectException",
        "java.io.IOException"};
    cluster = MRCluster.createCluster(conf);
    cluster.setExcludeExpList(expExcludeList);
    cluster.setUp();
    Hashtable<String,Object> prop = new Hashtable<String,Object>();
    prop.put("mapred.tasktracker.expiry.interval",30000L);
    prop.put("mapreduce.job.complete.cancel.delegation.tokens",false);
    cluster.restartClusterWithNewConfig(prop, confFile);
    UtilsForTests.waitFor(1000);
    conf = cluster.getJTClient().getProxy().getDaemonConf();
    createInput(inputDir, conf);
  }

  @AfterClass
  public static void after() throws Exception {
    cleanup(inputDir, conf);
    cleanup(outputDir, conf);
    cluster.tearDown();
    cluster.restart();
  }
  /**
   * Verify the job status whether it is succeed or not when 
   * lost task tracker is alive before the timeout.
   * @throws IOException if an I/O error occurs.
   */
  @Test
  public void testJobStatusOfLostTaskTracker1() throws
      Exception{
    String testName = "LTT1";
    setupJobAndRun();
    JobStatus jStatus = verifyLostTaskTrackerJobStatus(testName);    
    Assert.assertEquals("Job has not been succeeded...", 
         JobStatus.SUCCEEDED, jStatus.getRunState());
  }
  
  /**
   * Verify the job status whether it is succeeded or not when 
   * the lost task trackers time out for all four attempts of a task. 
   * @throws IOException if an I/O error occurs.
   */
  @Test
  public void testJobStatusOfLostTracker2()  throws 
      Exception {
    String testName = "LTT2";
    setupJobAndRun();
    JobStatus jStatus = verifyLostTaskTrackerJobStatus(testName);
    Assert.assertEquals("Job has not been failed...", 
            JobStatus.SUCCEEDED, jStatus.getRunState());
  }

  private void setupJobAndRun() throws IOException { 
    SleepJob job = new SleepJob();
    job.setConf(conf);
    conf = job.setupJobConf(3, 1, 60000, 100, 60000, 100);
    JobConf jobConf = new JobConf(conf);
    cleanup(outputDir, conf);
    jtClient = cluster.getJTClient();
    JobClient client = jtClient.getClient();
    wovenClient = cluster.getJTClient().getProxy();
    RunningJob runJob = client.submitJob(jobConf);
    jID = runJob.getID();
    jInfo = wovenClient.getJobInfo(jID);
    Assert.assertNotNull("Job information is null",jInfo);
    Assert.assertTrue("Job has not been started for 1 min.", 
        jtClient.isJobStarted(jID));
    JobStatus jobStatus = jInfo.getStatus();
    // Make sure that job should run and completes 40%. 
    while (jobStatus.getRunState() != JobStatus.RUNNING && 
      jobStatus.mapProgress() < 0.4f) {
      UtilsForTests.waitFor(100);
      jobStatus = wovenClient.getJobInfo(jID).getStatus();
    }
  }
  
  private JobStatus verifyLostTaskTrackerJobStatus(String testName) 
      throws IOException{
    TaskInfo taskInfo = null;
    TaskID tID = null;
    String[] taskTrackers = null;
    TaskInfo[] taskInfos = wovenClient.getTaskInfo(jID);
    for (TaskInfo taskinfo : taskInfos) {
      if (!taskinfo.isSetupOrCleanup()) {
        taskInfo = taskinfo;
        break;
      }
    }
    Assert.assertTrue("Task has not been started for 1 min.",
            jtClient.isTaskStarted(taskInfo));
    tID = TaskID.downgrade(taskInfo.getTaskID());
    TTClient ttClient = getTTClientIns(taskInfo);
    int counter = 0;
    while (counter < 30) {
      if (ttClient != null) {
        break;
      }else{
         taskInfo = wovenClient.getTaskInfo(taskInfo.getTaskID());  
         ttClient = getTTClientIns(taskInfo); 
      }
      counter ++;
    }
    Assert.assertNotNull("TaskTracker has not been found",ttClient);
    if (testName.equals("LTT1")) {
        ttClient.kill();
        waitForTTStop(ttClient);
        UtilsForTests.waitFor(20000);
        ttClient.start();
        waitForTTStart(ttClient);
    } else {
       int index = 0 ;
       while(index++ < 4 ) {
           ttClient.kill();
           waitForTTStop(ttClient);
           UtilsForTests.waitFor(40000);
           ttClient.start();
           waitForTTStart(ttClient);
           taskInfo = wovenClient.getTaskInfo(taskInfo.getTaskID());
           ttClient = getTTClientIns(taskInfo);
           counter = 0;
           while (counter < 30) {
             if (ttClient != null) {
               break;
             }else{
                taskInfo = wovenClient.getTaskInfo(taskInfo.getTaskID());  
                ttClient = getTTClientIns(taskInfo); 
             }
             counter ++;
           }
           Assert.assertNotNull("TaskTracker has not been found",ttClient);
           LOG.info("Task killed attempts:" + 
               taskInfo.numKilledAttempts());
       }
       Assert.assertEquals("Task killed attempts are not matched ",
           4, taskInfo.numKilledAttempts());
    }
    LOG.info("Waiting till the job is completed...");
    while (!jInfo.getStatus().isJobComplete()) {
      UtilsForTests.waitFor(1000);
      jInfo = wovenClient.getJobInfo(jID);
    }
    return jInfo.getStatus();
  }

  private TTClient getTTClientIns(TaskInfo taskInfo) throws IOException{
    String [] taskTrackers = taskInfo.getTaskTrackers();
    int counter = 0;
    TTClient ttClient = null;
    while (counter < 60) {
      if (taskTrackers.length != 0) {
        break;
      }
      UtilsForTests.waitFor(100);
      taskInfo = wovenClient.getTaskInfo(taskInfo.getTaskID());
      taskTrackers = taskInfo.getTaskTrackers();
      counter ++;
    }
    if ( taskTrackers.length != 0) {
      String hostName = taskTrackers[0].split("_")[1];
      hostName = hostName.split(":")[0];
      ttClient = cluster.getTTClient(hostName);
    }
    return ttClient;
  }
  private void waitForTTStart(TTClient ttClient) throws 
     IOException {
    LOG.debug(ttClient.getHostName() + " is waiting to come up.");
    while (true) { 
      try {
        ttClient.ping();
        LOG.info("TaskTracker : " + ttClient.getHostName() + " is pinging...");
        break;
      } catch (Exception exp) {
        LOG.debug(ttClient.getHostName() + " is waiting to come up.");
        UtilsForTests.waitFor(10000);
      }
    }
  }
  
  private void waitForTTStop(TTClient ttClient) throws 
     IOException {
    LOG.info("Waiting for Tasktracker:" + ttClient.getHostName() 
        + " to stop.....");
    while (true) {
      try {
        ttClient.ping();
        LOG.debug(ttClient.getHostName() +" is waiting state to stop.");
        UtilsForTests.waitFor(10000);
      } catch (Exception exp) {
        LOG.info("TaskTracker : " + ttClient.getHostName() + " is stopped...");
        break;
      } 
    }
  }
  
  private static void cleanup(Path dir, Configuration conf) throws 
      IOException {
    FileSystem fs = dir.getFileSystem(conf);
    fs.delete(dir, true);
  }

  private static void createInput(Path inDir, Configuration conf) throws 
      IOException {
    String input = "Hadoop is framework for data intensive distributed " 
        + "applications.\nHadoop enables applications to" 
        + " work with thousands of nodes.";
    FileSystem fs = inDir.getFileSystem(conf);
    if (!fs.mkdirs(inDir)) {
      throw new IOException("Failed to create the input directory:" 
          + inDir.toString());
    }
    fs.setPermission(inDir, new FsPermission(FsAction.ALL, 
        FsAction.ALL, FsAction.ALL));
    DataOutputStream file = fs.create(new Path(inDir, "data.txt"));
    file.writeBytes(input);
    file.close();
  }
}
