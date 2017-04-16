package org.apache.hadoop.mapred;

import java.io.File;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.test.system.MRCluster;
import org.apache.hadoop.mapreduce.test.system.TTClient;
import org.apache.hadoop.mapreduce.test.system.MRCluster.Role;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestHealthScriptError {
  public static MRCluster cluster;
  public static HealthScriptHelper helper;
  public static String remotePath;
  public static String healthScriptError="healthScriptError";
  public static String remoteHSPath = "test.system.hdrc.healthscript.path";
  static final Log LOG = LogFactory.getLog(TestHealthScriptError.class);
  
  @BeforeClass
  public static void setUp() throws java.lang.Exception {
    String [] expExcludeList = new String[2];
    expExcludeList[0] = "java.net.ConnectException";
    expExcludeList[1] = "java.io.IOException";
    cluster = MRCluster.createCluster(new Configuration());
    cluster.setExcludeExpList(expExcludeList);
    cluster.setUp();
    remotePath = cluster.getConf().get(remoteHSPath);
    helper = new HealthScriptHelper();
  }
  
  /**
   * The test will induce the ERROR with health script, asserts the task tracker
   * is unhealthy and then revert backs to non error condition and verifies
   * the task tracker is healthy. When the task tracker is marked unhealthy
   * also verifies that is marked as blacklisted, and reverse is true when it is
   * healthy. Also this verifies the custom error message that is set when
   * the task tracker is marked unhealthy.
   * @throws Exception in case of test errors
   */
  @Test
  public void testInduceError() throws Exception { 
    LOG.info("running testInduceError");
    TTClient client = cluster.getTTClient();
    Configuration tConf= client.getProxy().getDaemonConf();    
    tConf.set("mapred.task.tracker.report.address",
        cluster.getConf().get("mapred.task.tracker.report.address"));
    String defaultHealthScript = tConf.get("mapred.healthChecker.script.path");
    Assert.assertTrue("Health script was not set", defaultHealthScript != null);        
    tConf.set("mapred.healthChecker.script.path", remotePath+File.separator+
        healthScriptError);
    tConf.setInt("mapred.healthChecker.interval", 1000);
    helper.copyFileToRemoteHost(healthScriptError, client.getHostName(), 
        remotePath, cluster);
    cluster.restartDaemonWithNewConfig(client, "mapred-site.xml", tConf, 
        Role.TT);
    //make sure the TT is blacklisted
    helper.verifyTTBlackList(tConf, client,
        "ERROR Task Tracker status is fatal", cluster);
    //Now put back the task tracker in a healthy state
    cluster.restart(client, Role.TT);
    //now do the opposite of blacklist verification
    tConf = client.getProxy().getDaemonConf();
    helper.deleteFileOnRemoteHost(remotePath+File.separator+healthScriptError,
        client.getHostName());
  } 
  
  @AfterClass
  public static void tearDown() throws Exception {    
    cluster.tearDown();
  }
}
