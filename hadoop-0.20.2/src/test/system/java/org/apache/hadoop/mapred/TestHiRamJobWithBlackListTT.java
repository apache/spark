package org.apache.hadoop.mapred;

import java.io.File;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.test.system.JTProtocol;
import org.apache.hadoop.mapreduce.test.system.MRCluster;
import org.apache.hadoop.mapreduce.test.system.TTClient;
import org.apache.hadoop.mapreduce.test.system.MRCluster.Role;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import java.util.List;
import java.util.ArrayList;

public class TestHiRamJobWithBlackListTT {
  static final Log LOG = LogFactory.getLog(TestHiRamJobWithBlackListTT.class);
  private static HealthScriptHelper bListHelper = null;
  public static String remotePath;
  public static MRCluster cluster;
  
  @BeforeClass
  public static void setUp() throws java.lang.Exception {
    String [] expExcludeList = new String[2];
    expExcludeList[0] = "java.net.ConnectException";
    expExcludeList[1] = "java.io.IOException";
    cluster = MRCluster.createCluster(new Configuration());
    cluster.setExcludeExpList(expExcludeList);
    cluster.setUp();
    bListHelper = new HealthScriptHelper();
    remotePath = cluster.getConf().get(TestHealthScriptError.remoteHSPath);
  }
  
  /** Black List more than 25 % of task trackers , run the high ram
   * job and make sure that no exception is thrown. 
   * @throws Exception If fails to blacklist TT or run high ram high
   */
  @Test
  public void testHiRamJobBlackListedTaskTrackers() throws Exception {
    final HighRamJobHelper hRamHelper = new HighRamJobHelper();
    List<TTClient> bListedTT = new ArrayList<TTClient>();
    List<TTClient> tClient = cluster.getTTClients();
    int count = tClient.size();
    int moreThan25Per = count / 4 +1;
    LOG.info ("More than 25 % of TTclient is "+moreThan25Per);
    for (int i=0; i < moreThan25Per ; ++i) {
      TTClient client = tClient.get(i);
      bListedTT.add(client);
      blackListTT(client);
    }
    //Now run the high ram job
    JobClient jobClient = cluster.getJTClient().getClient();
    JTProtocol remoteJTClient = cluster.getJTClient().getProxy();
    Configuration conf = remoteJTClient.getDaemonConf();    
    hRamHelper.runHighRamJob(conf, jobClient, remoteJTClient,
        "Job did not succeed");
    //put the task tracker back in healthy state
    for( int i =0; i < bListedTT.size() ; ++i) {
      unBlackListTT(bListedTT.get(i));
    }
  }
  
  @AfterClass
  public static void tearDown() throws Exception {    
    cluster.tearDown();
  }
  
  private void unBlackListTT (TTClient client) throws Exception{
    //Now put back the task tracker in a healthy state
    cluster.restart(client, Role.TT);
    bListHelper.deleteFileOnRemoteHost(remotePath + File.separator +
        TestHealthScriptError.healthScriptError, client.getHostName());
  }
  
  private void blackListTT(TTClient client) throws Exception {
    Configuration tConf= client.getProxy().getDaemonConf();    
    tConf.set("mapred.task.tracker.report.address",
        cluster.getConf().get("mapred.task.tracker.report.address"));
    String defaultHealthScript = tConf.get("mapred.healthChecker.script.path");
    Assert.assertTrue("Health script was not set", defaultHealthScript != null);        
    tConf.set("mapred.healthChecker.script.path", remotePath+File.separator+
        TestHealthScriptError.healthScriptError);
    tConf.setInt("mapred.healthChecker.interval", 1000);
    bListHelper.copyFileToRemoteHost(TestHealthScriptError.healthScriptError, 
        client.getHostName(), remotePath, cluster);
    cluster.restartDaemonWithNewConfig(client, "mapred-site.xml", tConf, 
        Role.TT);
    //make sure the TT is blacklisted
    bListHelper.verifyTTBlackList(tConf, client,
        "ERROR Task Tracker status is fatal", cluster);
  }

}
