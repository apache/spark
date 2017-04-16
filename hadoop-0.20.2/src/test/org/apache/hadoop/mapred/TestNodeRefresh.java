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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Shell.ShellCommandExecutor;

/**
 * Test node decommissioning and recommissioning via refresh. Also check if the 
 * nodes are decommissioned upon refresh. 
 */
public class TestNodeRefresh extends TestCase {
  private String namenode = null;
  private MiniDFSCluster dfs = null;
  private MiniMRCluster mr = null;
  private JobTracker jt = null;
  private String[] hosts = null;
  private String[] trackerHosts = null;
  private UserGroupInformation owner, user1, user2, user3, user4, user5;
  private static final Log LOG = 
    LogFactory.getLog(TestNodeRefresh.class);
  
  private String getHostname(int i) {
    return "host" + i + ".com";
  }

  private void startCluster(int numHosts, int numTrackerPerHost, 
                            int numExcluded, UserGroupInformation clusterUgi,
                            Configuration conf) 
  throws IOException {
    try {
      // create fake mapping for the groups
      owner = UserGroupInformation.getLoginUser();
      user1= UserGroupInformation.createUserForTesting("user1", 
                                                       new String[] {"user1"});
      user2= UserGroupInformation.createUserForTesting("user2", 
                                                       new String[] {"user2"});
      user3= UserGroupInformation.createUserForTesting("user3", 
                                                       new String[] {"abc"});
      user4= UserGroupInformation.createUserForTesting("user4", 
                                                   new String[] {"supergroup"});
      user5= UserGroupInformation.createUserForTesting("user5", 
          new String[] {"user5"});
      conf.setBoolean("dfs.replication.considerLoad", false);
      
      // prepare hosts info
      hosts = new String[numHosts];
      for (int i = 1; i <= numHosts; ++i) {
        hosts[i - 1] = getHostname(i);
      }
      
      // start dfs
      dfs = new MiniDFSCluster(conf, 1, true, null, hosts);
      dfs.waitActive();
      dfs.startDataNodes(conf, numHosts, true, null, null, hosts, null);
      dfs.waitActive();
      FileSystem.mkdirs(dfs.getFileSystem(), new Path("/"),
          new FsPermission((short) 0777));

      namenode = (dfs.getFileSystem()).getUri().getHost() + ":" + 
      (dfs.getFileSystem()).getUri().getPort(); 
      
      // create tracker hosts
      trackerHosts = new String[numHosts * numTrackerPerHost];
      for (int i = 1; i <= (numHosts * numTrackerPerHost); ++i) {
        trackerHosts[i - 1] = getHostname(i);
      }
      
      // start mini mr
      JobConf jtConf = new JobConf(conf);
      mr = new MiniMRCluster(0, 0, numHosts * numTrackerPerHost, namenode, 1, 
                             null, trackerHosts, clusterUgi, jtConf, 
                             numExcluded * numTrackerPerHost);
      jt = mr.getJobTrackerRunner().getJobTracker();
      
      // check if trackers from all the desired hosts have connected
      Set<String> hostsSeen = new HashSet<String>();
      for (TaskTrackerStatus status : jt.taskTrackers()) {
        hostsSeen.add(status.getHost());
      }
      assertEquals("Not all hosts are up", numHosts - numExcluded, 
                   hostsSeen.size());
    } catch (IOException ioe) {
      stopCluster();
    }
  }

  private void stopCluster() {
    hosts = null;
    trackerHosts = null;
    if (dfs != null) { 
      dfs.shutdown();
      dfs = null;
      namenode = null;
    }
    if (mr != null) { 
      mr.shutdown();
      mr = null;
      jt = null;
    }
  }

  private AdminOperationsProtocol getClient(Configuration conf, 
                                                   UserGroupInformation ugi) 
  throws IOException {
    return (AdminOperationsProtocol)RPC.getProxy(AdminOperationsProtocol.class,
        AdminOperationsProtocol.versionID, JobTracker.getAddress(conf), ugi, 
        conf, NetUtils.getSocketFactory(conf, AdminOperationsProtocol.class));
  }

  /**
   * Check default value of mapred.hosts.exclude. Also check if only 
   * owner is allowed to this command.
   */
  public void testMRRefreshDefault() throws IOException {  
    // start a cluster with 2 hosts and no exclude-hosts file
    Configuration conf = new Configuration();
    conf.set("mapred.hosts.exclude", "");
    startCluster(2, 1, 0, UserGroupInformation.getLoginUser(),conf);

    conf = mr.createJobConf(new JobConf(conf));

    // refresh with wrong user
    AdminOperationsProtocol client = getClient(conf, user1);
    boolean success = false;
    try {
      // Also try tool runner
      client.refreshNodes();
      success = true;
    } catch (IOException ioe) {}
    assertFalse("Invalid user performed privileged refresh operation", success);

    // refresh with correct user
    success = false;
    client = getClient(conf, owner);
    try {
      client.refreshNodes();
      success = true;
    } catch (IOException ioe){}
    assertTrue("Privileged user denied permission for refresh operation",
               success);

    // refresh with supergroup
    success = false;
    client = getClient(conf, user4);
    try {
      client.refreshNodes();
      success = true;
    } catch (IOException ioe){}
    assertFalse("Invalid user performed privileged refresh operation",
               success);

    // check the cluster status and tracker size
    assertEquals("Trackers are lost upon refresh with empty hosts.exclude",
                 2, jt.getClusterStatus(false).getTaskTrackers());
    assertEquals("Excluded node count is incorrect",
                 0, jt.getClusterStatus(false).getNumExcludedNodes());

    // check if the host is disallowed
    Set<String> hosts = new HashSet<String>();
    for (TaskTrackerStatus status : jt.taskTrackers()) {
      hosts.add(status.getHost());
    }
    assertEquals("Host is excluded upon refresh with empty hosts.exclude",
                 2, hosts.size());

    stopCluster();
  }

  /**
   * Check refresh with a specific user/group is set in the conf
   */
  public void testMRSuperUsers() throws IOException {  
    // start a cluster with 1 host and specified cluster administrators
    Configuration conf = new Configuration();
    // set the admin acl
    conf.set(JobConf.MR_ADMINS, "user5 abc");
    startCluster(2, 1, 0, UserGroupInformation.createRemoteUser("user1"), conf);
    
    conf = mr.createJobConf(new JobConf(conf));

    // refresh with wrong user
    AdminOperationsProtocol client = getClient(conf, user2);
    boolean success = false;
    try {
      // Also try tool runner
      client.refreshNodes();
      success = true;
    } catch (IOException ioe) {}
    assertFalse("Invalid user performed privileged refresh operation", success);

    // refresh with correct user
    success = false;
    client = getClient(conf, user1);
    try {
      client.refreshNodes();
      success = true;
    } catch (IOException ioe){}
    assertTrue("Privileged user denied permission for refresh operation",
               success);

    // refresh with admin group
    success = false;
    client = getClient(conf, user3);
    try {
      client.refreshNodes();
      success = true;
    } catch (IOException ioe){}
    assertTrue("Admin group member denied permission for refresh operation",
               success);

    // refresh with admin user
    success = false;
    client = getClient(conf, user5);
    try {
      client.refreshNodes();
      success = true;
    } catch (IOException ioe){}
    assertTrue("Admin user denied permission for refresh operation",
               success);

    stopCluster();
  }

  /**
   * Check node refresh for decommissioning. Check if an allowed host is 
   * disallowed upon refresh. Also check if only owner/cluster administrator is 
   * allowed to fire this command.
   */
  public void testMRRefreshDecommissioning() throws IOException {
    // start a cluster with 2 hosts and empty exclude-hosts file
    Configuration conf = new Configuration();
    File file = new File("hosts.exclude");
    file.delete();
    startCluster(2, 1, 0, UserGroupInformation.getLoginUser(), conf);
    String hostToDecommission = getHostname(1);
    conf = mr.createJobConf(new JobConf(conf));

    // change the exclude-hosts file to include one host
    FileOutputStream out = new FileOutputStream(file);
    LOG.info("Writing excluded nodes to log file " + file.toString());
    BufferedWriter writer = null;
    try {
      writer = new BufferedWriter(new OutputStreamWriter(out));
      writer.write( hostToDecommission + "\n"); // decommission first host
    } finally {
      if (writer != null) {
        writer.close();
      }
      out.close();
    }
    file.deleteOnExit();

    AdminOperationsProtocol client = getClient(conf, owner);
    try {
      client.refreshNodes();
    } catch (IOException ioe){}
 
    // check the cluster status and tracker size
    assertEquals("Tracker is not lost upon host decommissioning", 
                 1, jt.getClusterStatus(false).getTaskTrackers());
    assertEquals("Excluded node count is incorrect", 
                 1, jt.getClusterStatus(false).getNumExcludedNodes());
    
    // check if the host is disallowed
    for (TaskTrackerStatus status : jt.taskTrackers()) {
      assertFalse("Tracker from decommissioned host still exist", 
                  status.getHost().equals(hostToDecommission));
    }
    
    stopCluster();
  }

  /**
   * Check node refresh for recommissioning. Check if an disallowed host is 
   * allowed upon refresh.
   */
  public void testMRRefreshRecommissioning() throws IOException {
    String hostToInclude = getHostname(1);

    // start a cluster with 2 hosts and exclude-hosts file having one hostname
    Configuration conf = new Configuration();
    
    // create a exclude-hosts file to include one host
    File file = new File("hosts.exclude");
    file.delete();
    FileOutputStream out = new FileOutputStream(file);
    LOG.info("Writing excluded nodes to log file " + file.toString());
    BufferedWriter writer = null;
    try {
      writer = new BufferedWriter(new OutputStreamWriter(out));
      writer.write(hostToInclude + "\n"); // exclude first host
    } finally {
      if (writer != null) {
        writer.close();
      }
      out.close();
    }
    
    startCluster(2, 1, 1, UserGroupInformation.getLoginUser(), conf);
    
    file.delete();

    // change the exclude-hosts file to include no hosts
    // note that this will also test hosts file with no content
    out = new FileOutputStream(file);
    LOG.info("Clearing hosts.exclude file " + file.toString());
    writer = null;
    try {
      writer = new BufferedWriter(new OutputStreamWriter(out));
      writer.write("\n");
    } finally {
      if (writer != null) {
        writer.close();
      }
      out.close();
    }
    file.deleteOnExit();
    
    conf = mr.createJobConf(new JobConf(conf));

    AdminOperationsProtocol client = getClient(conf, owner);
    try {
      client.refreshNodes();
    } catch (IOException ioe){}

    // start a tracker
    mr.startTaskTracker(hostToInclude, null, 2, 1);

    // wait for the tracker to join the jt
    while  (jt.taskTrackers().size() < 2) {
      UtilsForTests.waitFor(100);
    }

    assertEquals("Excluded node count is incorrect", 
                 0, jt.getClusterStatus(false).getNumExcludedNodes());
    
    // check if the host is disallowed
    boolean seen = false;
    for (TaskTrackerStatus status : jt.taskTrackers()) {
      if(status.getHost().equals(hostToInclude)) {
        seen = true;
        break;
      }
    }
    assertTrue("Tracker from excluded host doesnt exist", seen);
    
    stopCluster();
  }
}
