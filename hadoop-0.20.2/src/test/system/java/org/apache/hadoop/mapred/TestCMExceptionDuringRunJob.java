package org.apache.hadoop.mapred;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.examples.SleepJob;
import org.apache.hadoop.mapreduce.test.system.FinishTaskControlAction;
import org.apache.hadoop.mapreduce.test.system.JTProtocol;
import org.apache.hadoop.mapreduce.test.system.JobInfo;
import org.apache.hadoop.mapreduce.test.system.MRCluster;
import org.apache.hadoop.mapreduce.test.system.MRCluster.Role;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.AfterClass;
import org.junit.Test;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;
import java.util.Hashtable;
import java.lang.Integer;

public class TestCMExceptionDuringRunJob {
  
  private static MRCluster cluster = null;
  static final Log LOG = LogFactory.getLog(TestCMExceptionDuringRunJob.class);
  private JTProtocol remoteJTClient=null;
  private Configuration conf =null;
  @BeforeClass
  public static void setUp() throws java.lang.Exception {
    String [] expExcludeList = new String[2];
    expExcludeList[0] = "java.net.ConnectException";
    expExcludeList[1] = "java.io.IOException";
    cluster = MRCluster.createCluster(new Configuration());
    cluster.setExcludeExpList(expExcludeList);
    cluster.setUp();
  }
  
  /**
   * The objective of the test is, when the user accesses the retired job data
   * or the running job data simultaneously in different threads there
   * is no concurrent modification exceptions gets thrown
   */
  @Test
  public void testNoCMExcepRunningJob() throws Exception {
    
    remoteJTClient = cluster.getJTClient().getProxy();
    
    conf = remoteJTClient.getDaemonConf();
    conf.setBoolean("mapreduce.job.complete.cancel.delegation.tokens", false);
    //set interval to 30 secs, check to 10 secs, check determines the frequency
    //of the jobtracker thread that will check for retired jobs, and interval
    //will determine how long it will take before a job retires.
    //conf.setInt("mapred.jobtracker.retirejob.interval",30*1000);
    //conf.setInt("mapred.jobtracker.retirejob.check",10*1000);
    //cluster.restartDaemonWithNewConfig(cluster.getJTClient(), "mapred-site.xml",
    //    conf, Role.JT);
    Hashtable<String,Long> props = new Hashtable<String,Long>();
    props.put("mapred.jobtracker.retirejob.interval",30000L);
    props.put("mapred.jobtracker.retirejob.check",10000L);
    cluster.restartClusterWithNewConfig(props,"mapred-site.xml");
    JobID jobid1 = runSleepJob(true);
    JobID jobid2 = runSleepJob(true);
    JobID jobid3 = runSleepJob(false);
    //Waiting for a minute for the job to retire
    UtilsForTests.waitFor(60*1000);
    RunAccessHistoryData access1 = new RunAccessHistoryData(jobid1);
    RunAccessHistoryData access2 = new RunAccessHistoryData(jobid2);
    new Thread(access1).start();
    new Thread(access2).start();
    remoteJTClient.getJobSummaryInfo(jobid3);
    cluster.signalAllTasks(jobid3);
    cluster.getJTClient().isJobStopped(jobid3);
    //cluster.restart(cluster.getJTClient(), Role.JT);
    cluster.restart();
  }
  
  
  public class RunAccessHistoryData implements Runnable {
    private  JobID jobId = null;
    
    public RunAccessHistoryData (JobID jobId) {
      this.jobId =jobId; 
    }
    
     public void run () {
       try {
         remoteJTClient.accessHistoryData(jobId);
       }
       catch (Exception ex) {
         ex.printStackTrace();
       }
     }
  }

  
  public JobID runSleepJob(boolean signalJob) throws Exception{
    SleepJob job = new SleepJob();
    job.setConf(conf);
    conf = job.setupJobConf(5, 1, 100, 5, 100, 5);
    JobConf jconf = new JobConf(conf);
    //Controls the job till all verification is done 
    FinishTaskControlAction.configureControlActionForJob(conf);
    //Submitting the job
    RunningJob rJob = cluster.getJTClient().getClient().submitJob(jconf);
    JobID jobId = rJob.getID();
    JobInfo jInfo = remoteJTClient.getJobInfo(jobId);
    LOG.info("jInfo is :" + jInfo);
    boolean jobStarted = cluster.getJTClient().isJobStarted(jobId);
    Assert.assertTrue("Job has not started even after a minute", 
        jobStarted );
      
    if(signalJob) {
      cluster.signalAllTasks(jobId);
      Assert.assertTrue("Job has not stopped yet",
          cluster.getJTClient().isJobStopped(jobId));
    }
    return jobId;
  }
  
  @AfterClass
  public static void tearDown() throws Exception {    
    cluster.tearDown();
  }

}
