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
package org.apache.hadoop.hdfs.server.namenode;

import junit.framework.TestCase;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.util.Daemon;

import java.io.IOException;

import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;

public class TestLeaseManager extends TestCase {
  public static final Log LOG = LogFactory.getLog(TestLeaseManager.class);

  /*
   * test case: two leases are added for a singler holder, should use
   * the internalReleaseOne method
   */
  public void testMultiPathLeaseRecovery()
    throws IOException, InterruptedException {
    Configuration conf = new Configuration();
    MiniDFSCluster cluster = new MiniDFSCluster(conf, 3, true, null);
    NameNode namenode = cluster.getNameNode();
    FSNamesystem spyNamesystem = spy(namenode.getNamesystem());
    LeaseManager leaseManager = new LeaseManager(spyNamesystem);
    
    spyNamesystem.leaseManager = leaseManager;
    spyNamesystem.lmthread.interrupt();
    
    String holder = "client-1";
    String path1 = "/file-1";
    String path2 = "/file-2";
    
    leaseManager.setLeasePeriod(1, 2);
    leaseManager.addLease(holder, path1);
    leaseManager.addLease(holder, path2);
    Thread.sleep(1000);

    synchronized (spyNamesystem) { // checkLeases is always called with FSN lock
      leaseManager.checkLeases();
    }
    verify(spyNamesystem).internalReleaseLeaseOne((LeaseManager.Lease)anyObject(), eq("/file-1"));
    verify(spyNamesystem).internalReleaseLeaseOne((LeaseManager.Lease)anyObject(), eq("/file-2"));
    verify(spyNamesystem, never()).internalReleaseLease((LeaseManager.Lease)anyObject(), anyString());
  }
}
