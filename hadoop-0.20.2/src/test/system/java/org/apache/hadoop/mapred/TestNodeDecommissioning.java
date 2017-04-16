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

import java.util.Hashtable;

import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;
import org.apache.hadoop.mapreduce.test.system.JTProtocol;
import org.apache.hadoop.mapreduce.test.system.JTClient;
import org.apache.hadoop.mapreduce.test.system.TTClient;
import org.apache.hadoop.mapreduce.test.system.MRCluster;
import org.apache.hadoop.test.system.process.HadoopDaemonRemoteCluster;
import java.util.List;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.common.RemoteExecution;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.examples.SleepJob;
import org.apache.hadoop.fs.Path;
import java.net.InetAddress;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.AfterClass;
import org.junit.Test;

/**
 * Verify the task tracker node Decommission.
 */

public class TestNodeDecommissioning {

  private static MRCluster cluster = null;
  private static FileSystem dfs = null;
  private static JobClient jobClient = null;
  private static Configuration conf = null;
  private static Path excludeHostPath = null;
 
  static final Log LOG = LogFactory.
      getLog(TestNodeDecommissioning.class);

  public TestNodeDecommissioning() throws Exception {
  }

  @BeforeClass
  public static void setUp() throws Exception {
    cluster = MRCluster.createCluster(new Configuration());
    String [] expExcludeList = {"java.net.ConnectException",
        "java.io.IOException"};
    cluster.setExcludeExpList(expExcludeList);
    cluster.setUp();
    jobClient = cluster.getJTClient().getClient();
    conf = cluster.getJTClient().getProxy().getDaemonConf();
    String confFile = "mapred-site.xml";
    Hashtable<String,String> prop = new Hashtable<String,String>();
    prop.put("mapred.hosts.exclude", "/tmp/mapred.exclude");
    prop.put("mapreduce.cluster.administrators", " gridadmin,hadoop,users");
    cluster.restartClusterWithNewConfig(prop, confFile);
    UtilsForTests.waitFor(1000);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    cluster.restart();
    cluster.tearDown();
  }

  /**
   * This tests whether a node is successfully able to be
   * decommissioned or not.
   * First a node is decommissioned and verified.
   * Then it is removed from decommissoned and verified again.
   * At last the node is started.
   * @param none
   * @return void
   */
  @Test
  public void TestNodeDecommissioning() throws Exception {

    JTProtocol remoteJTClientProxy = cluster.getJTClient().getProxy();

    JTClient remoteJTClient = cluster.getJTClient();
    String jtClientHostName = remoteJTClient.getHostName();
    InetAddress localMachine = java.net.InetAddress.getLocalHost();
    String testRunningHostName = localMachine.getHostName(); 
    LOG.info("Hostname of local machine: " + testRunningHostName);

    List<TTClient> ttClients = cluster.getTTClients();

    //One slave is got
    TTClient ttClient = (TTClient)ttClients.get(0);
    String ttClientHostName = ttClient.getHostName();

    //Hadoop Conf directory is got
    String hadoopConfDir = cluster.getConf().get(
        HadoopDaemonRemoteCluster.CONF_HADOOPCONFDIR);

    LOG.info("hadoopConfDir is:" + hadoopConfDir);

    //Hadoop Home is got
    String hadoopHomeDir = cluster.getConf().get(
        HadoopDaemonRemoteCluster.CONF_HADOOPHOME);

    LOG.info("hadoopHomeDir is:" + hadoopHomeDir);

    conf = cluster.getJTClient().getProxy().getDaemonConf();
    //"mapred.hosts.exclude" path is got
    String excludeHostPathString = (String) conf.get("mapred.hosts.exclude");
    String keytabForHadoopqaUser = 
        "/homes/hadoopqa/hadoopqa.dev.headless.keytab hadoopqa"; 
    excludeHostPath = new Path(excludeHostPathString);
    LOG.info("exclude Host pathString is :" + excludeHostPathString);

    //One sleep job is submitted
    SleepJob job = new SleepJob();
    job.setConf(conf);
    conf = job.setupJobConf(1, 0, 100, 100, 100, 100);
    JobConf jconf = new JobConf(conf);
    RunningJob rJob = cluster.getJTClient().getClient().submitJob(jconf);

    //username who submitted the job is got.
    String userName = null;
    try {
      JobStatus[] jobStatus = cluster.getJTClient().getClient().getAllJobs();
      userName = jobStatus[0].getUsername();
    } catch(Exception ex) {
      LOG.error("Failed to get user name");
      boolean status = false;
      Assert.assertTrue("Failed to get the userName", status);
    }

    //The client which needs to be decommissioned is put in the exclude path. 
    String command = "echo " + ttClientHostName + " > " + excludeHostPath;
 
    LOG.info("command is : " + command);
    RemoteExecution.executeCommand(jtClientHostName, userName, command);

    //The refreshNode command is created and execute in Job Tracker Client.
    String refreshNodeCommand = "export HADOOP_CONF_DIR=" + hadoopConfDir + 
        "; export HADOOP_HOME=" + hadoopHomeDir + ";cd " + hadoopHomeDir + 
        ";kinit -k -t " + keytabForHadoopqaUser + 
        ";bin/hadoop mradmin -refreshNodes;"; 
    LOG.info("refreshNodeCommand is : " + refreshNodeCommand);
    try {
      RemoteExecution.executeCommand(testRunningHostName, userName, 
          refreshNodeCommand);
    } catch (Exception e) { e.printStackTrace();}

    //Checked whether the node is really decommissioned.
    boolean nodeDecommissionedOrNot = false;
    nodeDecommissionedOrNot = remoteJTClientProxy.
        isNodeDecommissioned(ttClientHostName); 

    //The TTClient host is removed from the exclude path
    command = "rm " + excludeHostPath;

    LOG.info("command is : " + command);
    RemoteExecution.executeCommand(jtClientHostName, userName, command);

    Assert.assertTrue("Node should be decommissioned", nodeDecommissionedOrNot);

    //The refreshNode command is created and execute in Job Tracker Client.
    RemoteExecution.executeCommand(jtClientHostName, userName, 
        refreshNodeCommand);

    //Checked whether the node is out of decommission.
    nodeDecommissionedOrNot = false;
    nodeDecommissionedOrNot = remoteJTClientProxy.
        isNodeDecommissioned(ttClientHostName); 
    Assert.assertFalse("present of not is", nodeDecommissionedOrNot);

    //Starting that node
    String ttClientStart = "export HADOOP_CONF_DIR=" + hadoopConfDir +
        "; export HADOOP_HOME=" + hadoopHomeDir + ";cd " + hadoopHomeDir +
        ";kinit -k -t " + keytabForHadoopqaUser + 
        ";bin/hadoop-daemons.sh start tasktracker;";
    LOG.info("ttClientStart is : " + ttClientStart);
    RemoteExecution.executeCommand(jtClientHostName, userName,
        ttClientStart);
  }
}

