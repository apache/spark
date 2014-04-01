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
package org.apache.hadoop.ipc;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.mapred.MiniMRCluster;
import org.apache.hadoop.net.StandardSocketFactory;

/**
 * This class checks that RPCs can use specialized socket factories.
 */
public class TestSocketFactory extends TestCase {

  /**
   * Check that we can reach a NameNode or a JobTracker using a specific
   * socket factory
   */
  public void testSocketFactory() throws IOException {
    // Create a standard mini-cluster
    Configuration sconf = new Configuration();
    MiniDFSCluster cluster = new MiniDFSCluster(sconf, 1, true, null);
    final int nameNodePort = cluster.getNameNodePort();

    // Get a reference to its DFS directly
    FileSystem fs = cluster.getFileSystem();
    assertTrue(fs instanceof DistributedFileSystem);
    DistributedFileSystem directDfs = (DistributedFileSystem) fs;

    // Get another reference via network using a specific socket factory
    Configuration cconf = new Configuration();
    FileSystem.setDefaultUri(cconf, String.format("hdfs://localhost:%s/",
        nameNodePort + 10));
    cconf.set("hadoop.rpc.socket.factory.class.default",
        "org.apache.hadoop.ipc.DummySocketFactory");
    cconf.set("hadoop.rpc.socket.factory.class.ClientProtocol",
        "org.apache.hadoop.ipc.DummySocketFactory");
    cconf.set("hadoop.rpc.socket.factory.class.JobSubmissionProtocol",
        "org.apache.hadoop.ipc.DummySocketFactory");

    fs = FileSystem.get(cconf);
    assertTrue(fs instanceof DistributedFileSystem);
    DistributedFileSystem dfs = (DistributedFileSystem) fs;

    JobClient client = null;
    MiniMRCluster mr = null;
    try {
      // This will test RPC to the NameNode only.
      // could we test Client-DataNode connections?
      Path filePath = new Path("/dir");

      assertFalse(directDfs.exists(filePath));
      assertFalse(dfs.exists(filePath));

      directDfs.mkdirs(filePath);
      assertTrue(directDfs.exists(filePath));
      assertTrue(dfs.exists(filePath));

      // This will test TPC to a JobTracker
      fs = FileSystem.get(sconf);
      mr = new MiniMRCluster(1, fs.getUri().toString(), 1);
      final int jobTrackerPort = mr.getJobTrackerPort();

      JobConf jconf = new JobConf(cconf);
      jconf.set("mapred.job.tracker", String.format("localhost:%d",
          jobTrackerPort + 10));
      client = new JobClient(jconf);

      JobStatus[] jobs = client.jobsToComplete();
      assertTrue(jobs.length == 0);

    } finally {
      try {
        if (client != null)
          client.close();
      } catch (Exception ignored) {
        // nothing we can do
        ignored.printStackTrace();
      }
      try {
        if (dfs != null)
          dfs.close();

      } catch (Exception ignored) {
        // nothing we can do
        ignored.printStackTrace();
      }
      try {
        if (directDfs != null)
          directDfs.close();

      } catch (Exception ignored) {
        // nothing we can do
        ignored.printStackTrace();
      }
      try {
        if (cluster != null)
          cluster.shutdown();

      } catch (Exception ignored) {
        // nothing we can do
        ignored.printStackTrace();
      }
      if (mr != null) {
        try {
          mr.shutdown();
        } catch (Exception ignored) {
          ignored.printStackTrace();
        }
      }
    }
  }
}

/**
 * Dummy socket factory which shift TPC ports by subtracting 10 when
 * establishing a connection
 */
class DummySocketFactory extends StandardSocketFactory {
  /**
   * Default empty constructor (for use with the reflection API).
   */
  public DummySocketFactory() {
  }

  /* @inheritDoc */
  @Override
  public Socket createSocket() throws IOException {
    return new Socket() {
      @Override
      public void connect(SocketAddress addr, int timeout)
          throws IOException {

        assert (addr instanceof InetSocketAddress);
        InetSocketAddress iaddr = (InetSocketAddress) addr;
        SocketAddress newAddr = null;
        if (iaddr.isUnresolved())
          newAddr =
              new InetSocketAddress(iaddr.getHostName(),
                  iaddr.getPort() - 10);
        else
          newAddr =
              new InetSocketAddress(iaddr.getAddress(), iaddr.getPort() - 10);
        System.out.printf("Test socket: rerouting %s to %s\n", iaddr,
            newAddr);
        super.connect(newAddr, timeout);
      }
    };
  }

  /* @inheritDoc */
  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (!(obj instanceof DummySocketFactory))
      return false;
    return true;
  }

  /* @inheritDoc */
  @Override
  public int hashCode() {
    // Dummy hash code (to make find bugs happy)
    return 53;
  }
}
