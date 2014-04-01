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
package org.apache.hadoop.hdfs;

import java.util.ConcurrentModificationException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;

/**
 * Test empty file creation.
 */
public class TestFileCreationEmpty extends junit.framework.TestCase {
  private boolean isConcurrentModificationException = false;

  /**
   * This test creates three empty files and lets their leases expire.
   * This triggers release of the leases. 
   * The empty files are supposed to be closed by that 
   * without causing ConcurrentModificationException.
   */
  public void testLeaseExpireEmptyFiles() throws Exception {
    final Thread.UncaughtExceptionHandler oldUEH = Thread.getDefaultUncaughtExceptionHandler();
    Thread.setDefaultUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
      public void uncaughtException(Thread t, Throwable e) {
        if (e instanceof ConcurrentModificationException) {
          FSNamesystem.LOG.error("t=" + t, e);
          isConcurrentModificationException = true;
        }
      }
    });

    System.out.println("testLeaseExpireEmptyFiles start");
    final long leasePeriod = 1000;
    final int DATANODE_NUM = 3;

    final Configuration conf = new Configuration();
    conf.setInt("heartbeat.recheck.interval", 1000);
    conf.setInt("dfs.heartbeat.interval", 1);

    // create cluster
    MiniDFSCluster cluster = new MiniDFSCluster(conf, DATANODE_NUM, true, null);
    try {
      cluster.waitActive();
      DistributedFileSystem dfs = (DistributedFileSystem)cluster.getFileSystem();

      // create a new file.
      TestFileCreation.createFile(dfs, new Path("/foo"), DATANODE_NUM);
      TestFileCreation.createFile(dfs, new Path("/foo2"), DATANODE_NUM);
      TestFileCreation.createFile(dfs, new Path("/foo3"), DATANODE_NUM);

      // set the soft and hard limit to be 1 second so that the
      // namenode triggers lease recovery
      cluster.setLeasePeriod(leasePeriod, leasePeriod);
      // wait for the lease to expire
      try {Thread.sleep(5 * leasePeriod);} catch (InterruptedException e) {}

      assertFalse(isConcurrentModificationException);
    } finally {
      Thread.setDefaultUncaughtExceptionHandler(oldUEH);
      cluster.shutdown();
    }
  }
}
