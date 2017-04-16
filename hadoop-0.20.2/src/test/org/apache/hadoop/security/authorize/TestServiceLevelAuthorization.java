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
package org.apache.hadoop.security.authorize;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HDFSPolicyProvider;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.namenode.NameNodeAdapter;
import org.apache.hadoop.hdfs.tools.DFSAdmin;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MiniMRCluster;
import org.apache.hadoop.mapred.TestMiniMRWithDFS;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.StringUtils;

import junit.framework.TestCase;

public class TestServiceLevelAuthorization extends TestCase {
  public void testServiceLevelAuthorization() throws Exception {
    MiniDFSCluster dfs = null;
    MiniMRCluster mr = null;
    FileSystem fileSys = null;
    try {
      final int slaves = 4;

      // Turn on service-level authorization
      Configuration conf = new Configuration();
      conf.setClass(PolicyProvider.POLICY_PROVIDER_CONFIG, 
                    HadoopPolicyProvider.class, PolicyProvider.class);
      conf.setBoolean(ServiceAuthorizationManager.SERVICE_AUTHORIZATION_CONFIG, 
                      true);
      
      // Start the mini clusters
      dfs = new MiniDFSCluster(conf, slaves, true, null);

      // Ensure that the protocols authorized on the name node are only the HDFS protocols.
      Set<Class<?>> protocolsWithAcls = NameNodeAdapter.getRpcServer(dfs.getNameNode())
          .getServiceAuthorizationManager().getProtocolsWithAcls();
      Service[] hdfsServices = new HDFSPolicyProvider().getServices();
      for (Service service : hdfsServices) {
        if (!protocolsWithAcls.contains(service.getProtocol()))
          fail("service authorization manager has no entry for protocol " + service.getProtocol());
      }
      if (hdfsServices.length != protocolsWithAcls.size())
        fail("there should be an entry for every HDFS service in the protocols with ACLs map");

      fileSys = dfs.getFileSystem();
      JobConf mrConf = new JobConf(conf);
      mr = new MiniMRCluster(slaves, fileSys.getUri().toString(), 1, 
                             null, null, mrConf);

      // Ensure that the protocols configured for the name node did not change
      // when the MR cluster was started.
      protocolsWithAcls = NameNodeAdapter.getRpcServer(dfs.getNameNode())
          .getServiceAuthorizationManager().getProtocolsWithAcls();
      hdfsServices = new HDFSPolicyProvider().getServices();
      for (Service service : hdfsServices) {
        if (!protocolsWithAcls.contains(service.getProtocol()))
          fail("service authorization manager has no entry for protocol " + service.getProtocol());
      }
      if (hdfsServices.length != protocolsWithAcls.size())
        fail("there should be an entry for every HDFS service in the protocols with ACLs map");

      // make cleanup inline sothat validation of existence of these directories
      // can be done
      mr.setInlineCleanupThreads();
      
      // Run examples
      TestMiniMRWithDFS.runPI(mr, mr.createJobConf(mrConf));
      TestMiniMRWithDFS.runWordCount(mr, mr.createJobConf(mrConf));
    } finally {
      if (dfs != null) { dfs.shutdown(); }
      if (mr != null) { mr.shutdown();
      }
    }
  }
  
  private static final String DUMMY_ACL = "nouser nogroup";
  private static final String UNKNOWN_USER = "dev,null";
  
  private void rewriteHadoopPolicyFile(File policyFile) throws IOException {
    FileWriter fos = new FileWriter(policyFile);
    PolicyProvider policyProvider = new HDFSPolicyProvider();
    fos.write("<configuration>\n");
    for (Service service : policyProvider.getServices()) {
      String key = service.getServiceKey();
      String value ="*";
      if (key.equals("security.refresh.policy.protocol.acl")) {
        value = DUMMY_ACL;
      }
      fos.write("<property><name>"+ key + "</name><value>" + value + 
                "</value></property>\n");
      System.err.println("<property><name>"+ key + "</name><value>" + value + 
          "</value></property>\n");
    }
    fos.write("</configuration>\n");
    fos.close();
  }
  
  private void refreshPolicy(Configuration conf)  throws IOException {
    DFSAdmin dfsAdmin = new DFSAdmin(conf);
    dfsAdmin.refreshServiceAcl();
  }
  
  public void testRefresh() throws Exception {
    MiniDFSCluster dfs = null;
    try {
      final int slaves = 4;

      // Turn on service-level authorization
      final Configuration conf = new Configuration();
      conf.setClass(PolicyProvider.POLICY_PROVIDER_CONFIG, 
                    HDFSPolicyProvider.class, PolicyProvider.class);
      conf.setBoolean(ServiceAuthorizationManager.SERVICE_AUTHORIZATION_CONFIG, 
                      true);
      
      // Start the mini dfs cluster
      dfs = new MiniDFSCluster(conf, slaves, true, null);

      // Refresh the service level authorization policy
      refreshPolicy(conf);
      
      // Simulate an 'edit' of hadoop-policy.xml
      String confDir = System.getProperty("test.build.extraconf", 
                                          "build/test/extraconf");
      String HADOOP_POLICY_FILE = System.getProperty("hadoop.policy.file");
      File policyFile = new File(confDir, HADOOP_POLICY_FILE);
      String policyFileCopy = HADOOP_POLICY_FILE + ".orig";
      FileUtil.copy(policyFile, FileSystem.getLocal(conf),   // first save original 
                    new Path(confDir, policyFileCopy), false, conf);
      rewriteHadoopPolicyFile(                               // rewrite the file
          new File(confDir, HADOOP_POLICY_FILE));
      
      // Refresh the service level authorization policy
      refreshPolicy(conf);
      
      // Refresh the service level authorization policy once again, 
      // this time it should fail!
      try {
        // Note: hadoop-policy.xml for tests has 
        // security.refresh.policy.protocol.acl = ${user.name}
        UserGroupInformation unknownUser = 
          UserGroupInformation.createRemoteUser("unknown");
        unknownUser.doAs(new PrivilegedExceptionAction<Void>() {
          public Void run() throws IOException {
            refreshPolicy(conf);
            return null;
          }
        });
        fail("Refresh of NameNode's policy file cannot be successful!");
      } catch (Exception re) {
        System.out.println("Good, refresh worked... refresh failed with: " + 
                           StringUtils.stringifyException(re));
      } finally {
        // Reset to original hadoop-policy.xml
        FileUtil.fullyDelete(new File(confDir, 
            HADOOP_POLICY_FILE));
        FileUtil.replaceFile(new File(confDir, policyFileCopy), new File(confDir, HADOOP_POLICY_FILE));
      }
    } finally {
      if (dfs != null) { dfs.shutdown(); }
    }
  }

}
