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

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSClient.DFSOutputStream;
import org.apache.hadoop.hdfs.server.namenode.DatanodeDescriptor;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import junit.framework.TestCase;

/**
 * This class tests DatanodeDescriptor.getBlocksScheduled() at the
 * NameNode. This counter is supposed to keep track of blocks currently
 * scheduled to a datanode.
 */
public class TestBlocksScheduledCounter extends TestCase {

  public void testBlocksScheduledCounter() throws IOException {
    
    MiniDFSCluster cluster = new MiniDFSCluster(new Configuration(), 1, 
                                                true, null);
    cluster.waitActive();
    FileSystem fs = cluster.getFileSystem();
    
    //open a file an write a few bytes:
    FSDataOutputStream out = fs.create(new Path("/testBlockScheduledCounter"));
    for (int i=0; i<1024; i++) {
      out.write(i);
    }
    // flush to make sure a block is allocated.
    ((DFSOutputStream)(out.getWrappedStream())).sync();
    
    ArrayList<DatanodeDescriptor> dnList = new ArrayList<DatanodeDescriptor>();
    cluster.getNameNode().namesystem.DFSNodesStatus(dnList, dnList);
    DatanodeDescriptor dn = dnList.get(0);
    
    assertEquals(1, dn.getBlocksScheduled());
   
    // close the file and the counter should go to zero.
    out.close();   
    assertEquals(0, dn.getBlocksScheduled());
  }
}
