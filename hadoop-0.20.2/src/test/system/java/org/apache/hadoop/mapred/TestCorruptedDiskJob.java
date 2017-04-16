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

import org.apache.commons.logging.Log;
import com.jcraft.jsch.*;
import java.util.List;
import org.apache.hadoop.common.RemoteExecution;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.test.system.JTProtocol;
import org.apache.hadoop.mapreduce.test.system.JobInfo;
import org.apache.hadoop.mapreduce.test.system.MRCluster;
import org.apache.hadoop.mapreduce.test.system.TTClient;
import org.apache.hadoop.mapreduce.test.system.JTClient;
import org.apache.hadoop.examples.RandomWriter;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.Tool;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Assert;
import org.junit.Test;
import java.io.IOException;
import java.util.Hashtable;

/**
 * Submit a job. Corrupt some disks, when job is running 
 * job should continue running and pass successfully.
 */
public class TestCorruptedDiskJob {
  private static final Log LOG = LogFactory
      .getLog(TestCorruptedDiskJob.class);
  private static MRCluster cluster;
  private static Path inputDir = new Path("input");
  private static Path outputDir = new Path("output");
  private static Configuration conf = new Configuration();
  private static String confFile = "mapred-site.xml";
  private static FileSystem dfs = null;
  private static final int RW_BYTES_PER_MAP = 25 * 1024 * 1024;
  private static final int RW_MAPS_PER_HOST = 2;
  private static JobClient client = null;
  int count = 0;
  String userName = null;
  JobStatus[] jobStatus = null;
  private static List<TTClient> ttClients = null;

  @BeforeClass
  public static void before() throws Exception {
    cluster = MRCluster.createCluster(conf);
    String [] expExcludeList = {"java.net.ConnectException",
        "java.io.IOException"};
    cluster.setExcludeExpList(expExcludeList);
    cluster.setUp();
    conf.setBoolean("mapreduce.job.complete.cancel.delegation.tokens", false);
    String newConfDir = cluster.
        getConf().get("test.system.hdrc.hadoopnewconfdir");
    LOG.info("newConfDir is :" + newConfDir);
    String newMapredLocalDirPath = conf.get("mapred.local.dir");

    //One of the disk is made corrupted by making the path inaccessible.
    newMapredLocalDirPath  = newMapredLocalDirPath.replaceAll("1", "11");
    LOG.info("newMapredLocalDirPath is :" + newMapredLocalDirPath);

    Hashtable<String,String> prop = new Hashtable<String,String>();
    prop.put("mapred.local.dir", newMapredLocalDirPath);

    String userName = System.getProperty("user.name");
    LOG.info("user name is :" + userName);

    //Creating the string to modify taskcontroller.cfg
    String replaceTaskControllerCommand = "cat " + newConfDir +
        "/taskcontroller.cfg | grep -v mapred.local.dir  > " + 
        newConfDir + "/tmp1.cfg;echo mapred.local.dir=" + 
        newMapredLocalDirPath + " >> " + newConfDir + 
        "/tmp2.cfg;cat " + newConfDir + 
        "/tmp2.cfg  > " + newConfDir + 
        "/taskcontroller.cfg;cat " + newConfDir + 
        "/tmp1.cfg  >> " + newConfDir + "/taskcontroller.cfg;";
      
    ttClients = cluster.getTTClients();
    cluster.restartClusterWithNewConfig(prop, confFile);
    UtilsForTests.waitFor(1000);

    //Changing the taskcontroller.cfg file in all taktracker nodes.
    //This is required as mapred.local.dir should match 
    //in both mapred-site.xml and taskcontroller.cfg.
    //This change can be done after cluster is brought up as
    //Linux task controller will access taskcontroller.cfg
    //when a job's task starts.
    for ( int i = 0;i < ttClients.size();i++ ) {
      TTClient ttClient = (TTClient)ttClients.get(i);
      String ttClientHostName = ttClient.getHostName();
      try {
        RemoteExecution.executeCommand(ttClientHostName, userName,
          replaceTaskControllerCommand);
      } catch (Exception e) { e.printStackTrace(); };
    }

    conf = cluster.getJTClient().getProxy().getDaemonConf();
    client = cluster.getJTClient().getClient();
    dfs = client.getFs();
    dfs.delete(inputDir, true);
    dfs.delete(outputDir, true);
  }

  @AfterClass
  public static void after() throws Exception {
    cluster.tearDown();
    cluster.restart();
    UtilsForTests.waitFor(1000);
    dfs.delete(inputDir, true);
    dfs.delete(outputDir, true);
  }

  /**
   * This tests the corrupted disk. If a disk does not exist, still
   * the job should run successfully.
   */
  @Test
  public void testCorruptedDiskJob() throws 
      Exception {

    // Scale down the default settings for RandomWriter for the test-case
    // Generates NUM_HADOOP_SLAVES * RW_MAPS_PER_HOST * RW_BYTES_PER_MAP
    conf.setInt("test.randomwrite.bytes_per_map", RW_BYTES_PER_MAP);
    conf.setInt("test.randomwriter.maps_per_host", RW_MAPS_PER_HOST);
    String[] rwArgs = {inputDir.toString()};

    // JTProtocol remoteJTClient
    JTProtocol remoteJTClient = cluster.getJTClient().getProxy();

    // JobInfo jInfo;
    JobInfo jInfo = null;

    dfs.delete(inputDir, true);

    // Run RandomWriter
    Assert.assertEquals(ToolRunner.run(conf, new RandomWriter(), rwArgs),
        0);

    jobStatus = client.getAllJobs();
    JobID id = null;
    //Getting the jobId of the just submitted job
    id = jobStatus[0].getJobID();

    LOG.info("jobid is :" + id.toString());

    Assert.assertTrue("Failed to complete the job",
    cluster.getJTClient().isJobStopped(id));

    jInfo = remoteJTClient.getJobInfo(id);
    JobStatus jStatus = jInfo.getStatus();

    if (jStatus != null) {
      Assert.assertEquals("Job has not succeeded...",
        JobStatus.SUCCEEDED, jStatus.getRunState());
    }
  }
}
