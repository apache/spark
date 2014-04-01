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

public class TestHealthScriptTimeout {
  public static String remotePath;
  public static MRCluster cluster;
  public static HealthScriptHelper helper;
  public String healthScriptTimeout="healthScriptTimeout";
  public static String remoteHSPath = "test.system.hdrc.healthscript.path";
  static final Log LOG = LogFactory.getLog(TestHealthScriptTimeout.class);
  
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
   * In this case the test times out the task tracker will get blacklisted  . 
   * @throws Exception in case of test errors 
   */
  @Test
  public void testScriptTimeout() throws Exception {
    LOG.info("running testScriptTimeout");
    TTClient client = cluster.getTTClient();
    Configuration tConf= client.getProxy().getDaemonConf();
    int defaultTimeout = tConf.getInt("mapred.healthChecker.script.timeout", 0);
    tConf.set("mapred.task.tracker.report.address",
        cluster.getConf().get("mapred.task.tracker.report.address"));
    Assert.assertTrue("Health script timeout was not set",defaultTimeout != 0);     
    tConf.set("mapred.healthChecker.script.path", remotePath+File.separator+
        healthScriptTimeout);
    tConf.setInt("mapred.healthChecker.script.timeout", 100);
    tConf.setInt("mapred.healthChecker.interval",1000);    
    helper.copyFileToRemoteHost(healthScriptTimeout, client.getHostName(),
        remotePath, cluster);
    cluster.restartDaemonWithNewConfig(client, "mapred-site.xml", tConf, 
        Role.TT);
    //make sure the TT is blacklisted
    helper.verifyTTBlackList(tConf, client, "Node health script timed out",
        cluster);
    //Now put back the task tracker in a health state
    cluster.restart(client, Role.TT);
    tConf = client.getProxy().getDaemonConf();
    //now do the opposite of blacklist verification
    helper.deleteFileOnRemoteHost(remotePath+File.separator+healthScriptTimeout,
        client.getHostName());
    
  } 
  
  @AfterClass
  public static void tearDown() throws Exception {    
    cluster.tearDown();
  }
}
