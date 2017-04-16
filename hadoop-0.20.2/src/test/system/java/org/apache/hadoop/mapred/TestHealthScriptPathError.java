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

public class TestHealthScriptPathError {
  public static MRCluster cluster;
  public static HealthScriptHelper helper;
  public static String remotePath;
  public String invalidHealthScript="invalidHealthScript";
  public static String remoteHSPath = "test.system.hdrc.healthscript.path";
  static final Log LOG = LogFactory.getLog(TestHealthScriptPathError.class);
  
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
   * Error in the test path and script will not run, the TT will not be marked
   * unhealthy
   * @throws Exception in case of test errors
   */
  @Test
  public void testHealthScriptPathError() throws Exception {
    LOG.info("running testHealthScriptPathError");
    TTClient client = cluster.getTTClient();
    Configuration tConf= client.getProxy().getDaemonConf();    
    tConf.set("mapred.task.tracker.report.address",
        cluster.getConf().get("mapred.task.tracker.report.address"));
    String defaultHealthScript = tConf.get("mapred.healthChecker.script.path");
    Assert.assertTrue("Health script was not set", defaultHealthScript != null);    
    tConf.set("mapred.healthChecker.script.path", remotePath+File.separator+
        invalidHealthScript);
    tConf.setInt("mapred.healthChecker.interval",1000);
    cluster.restartDaemonWithNewConfig(client, "mapred-site.xml", tConf, 
        Role.TT);
    //For a invalid health script the TT remains healthy
    helper.verifyTTNotBlackListed( client, tConf, cluster);
    cluster.restart(client, Role.TT);    
    tConf = client.getProxy().getDaemonConf();
  } 
  
  @AfterClass
  public static void tearDown() throws Exception {    
    cluster.tearDown();
  }
  
}
