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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.util.StringUtils;

public class TestAbandonBlock extends junit.framework.TestCase {
  public static final Log LOG = LogFactory.getLog(TestAbandonBlock.class);
  
  private static final Configuration CONF = new Configuration();
  static final String FILE_NAME_PREFIX
      = "/" + TestAbandonBlock.class.getSimpleName() + "_"; 

  public void testAbandonBlock() throws IOException {
    MiniDFSCluster cluster = new MiniDFSCluster(CONF, 2, true, null);
    FileSystem fs = cluster.getFileSystem();

    String src = FILE_NAME_PREFIX + "foo";
    FSDataOutputStream fout = null;
    try {
      //start writing a a file but not close it
      fout = fs.create(new Path(src), true, 4096, (short)1, 512L);
      for(int i = 0; i < 1024; i++) {
        fout.write(123);
      }
      fout.sync();
  
      //try reading the block by someone
      final DFSClient dfsclient = new DFSClient(NameNode.getAddress(CONF), CONF);
      LocatedBlocks blocks = dfsclient.namenode.getBlockLocations(src, 0, 1);
      LocatedBlock b = blocks.get(0); 
      try {
        dfsclient.namenode.abandonBlock(b.getBlock(), src, "someone");
        //previous line should throw an exception.
        assertTrue(false);
      }
      catch(IOException ioe) {
        LOG.info("GREAT! " + StringUtils.stringifyException(ioe));
      }
    }
    finally {
      try{fout.close();} catch(Exception e) {}
      try{fs.close();} catch(Exception e) {}
      try{cluster.shutdown();} catch(Exception e) {}
    }
  }
}
