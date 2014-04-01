package org.apache.hadoop.mapred;

import org.apache.commons.logging.Log;

import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.test.system.MRCluster;
import org.apache.hadoop.mapreduce.test.system.JTClient;
import org.apache.hadoop.mapreduce.test.system.JTProtocol;
import org.apache.hadoop.mapreduce.test.system.JobInfo;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.examples.SleepJob;
import org.junit.Before;
import org.junit.After;
import org.junit.Test;
import org.junit.Assert;

/**
 * Set the invalid configuration to task controller and verify the
 * job status.
 */
public class TestTaskController {
  private static final Log LOG = LogFactory.getLog(TestTaskController.class);
  private static Configuration conf = new Configuration();
  private static MRCluster cluster;
  private static JTProtocol remoteJTClient;
  private static JTClient jtClient;
  
  @Before
  public void before() throws Exception {
    String [] expExcludeList = {"java.net.ConnectException",
                                "java.io.IOException"};
    cluster = MRCluster.createCluster(conf);
    cluster.setExcludeExpList(expExcludeList);
    cluster.setUp();
    jtClient = cluster.getJTClient();
    remoteJTClient = jtClient.getProxy();
  }

  @After
  public void after() throws Exception {
    cluster.tearDown();
  }
  
  /**
   * Set the invalid mapred local directory location and run the job.
   * Verify the job status. 
   * @throws Exception - if an error occurs.
   */
  @Test
  public void testJobStatusForInvalidTaskControllerConf() 
      throws Exception {
    conf = remoteJTClient.getDaemonConf();
    if (conf.get("mapred.task.tracker.task-controller").
            equals("org.apache.hadoop.mapred.LinuxTaskController")) {
      StringBuffer mapredLocalDir = new StringBuffer();
      LOG.info("JobConf.MAPRED_LOCAL_DIR_PROPERTY:" + conf.get(JobConf.MAPRED_LOCAL_DIR_PROPERTY));
      mapredLocalDir.append(conf.get(JobConf.MAPRED_LOCAL_DIR_PROPERTY));
      mapredLocalDir.append(",");
      mapredLocalDir.append("/mapred/local");
      String jobArgs []= {"-D","mapred.local.dir=" + mapredLocalDir.toString(),
                         "-m", "1", 
                         "-r", "1", 
                         "-mt", "1000", 
                         "-rt", "1000",
                         "-recordt","100"};
      SleepJob job = new SleepJob();
      JobConf jobConf = new JobConf(conf); 
      int exitStatus = ToolRunner.run(jobConf, job, jobArgs);
      Assert.assertEquals("Exit Code:", 0, exitStatus);
      UtilsForTests.waitFor(100);
      JobClient jobClient = jtClient.getClient();
      JobID jobId =jobClient.getAllJobs()[0].getJobID();
      LOG.info("JobId:" + jobId);
      if (jobId != null) {
        JobInfo jInfo = remoteJTClient.getJobInfo(jobId);
        Assert.assertEquals("Job has not been succeeded", 
            jInfo.getStatus().getRunState(), JobStatus.SUCCEEDED);
       }
    } else {
       Assert.assertTrue("Linux Task controller not found.", false);
    }
  }
}
