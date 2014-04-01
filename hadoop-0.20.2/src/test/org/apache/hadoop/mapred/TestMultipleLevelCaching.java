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

import java.io.IOException;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.mapred.TestRackAwareTaskPlacement;

/**
 * This test checks whether the task caches are created and used properly.
 */
public class TestMultipleLevelCaching extends TestCase {
  private static final int MAX_LEVEL = 5;
  final Path inDir = new Path("/cachetesting");
  final Path outputPath = new Path("/output");

  /**
   * Returns a string representing a rack with level + 1 nodes in the topology
   * for the rack.
   * For id = 2, level = 2 we get /a/b2/c2
   *     id = 1, level = 3 we get /a/b1/c1/d1
   * NOTE There should always be one shared node i.e /a 
   * @param id Unique Id for the rack
   * @param level The level in the topology where the separation starts
   */
  private static String getRack(int id, int level) {
    StringBuilder rack = new StringBuilder();
    char alpha = 'a';
    int length = level + 1;
    while (length > level) {
      rack.append("/");
      rack.append(alpha);
      ++alpha;
      --length;
    }
    while (length > 0) {
      rack.append("/");
      rack.append(alpha);
      rack.append(id);
      ++alpha;
      --length;
    }
    return rack.toString();
  }

  public void testMultiLevelCaching() throws IOException {
    for (int i = 1 ; i <= MAX_LEVEL; ++i) {
      testCachingAtLevel(i);
    }
  }

  private void testCachingAtLevel(int level) throws IOException {
    String namenode = null;
    MiniDFSCluster dfs = null;
    MiniMRCluster mr = null;
    FileSystem fileSys = null;
    String testName = "TestMultiLevelCaching";
    try {
      final int taskTrackers = 1;
      // generate the racks
      // use rack1 for data node
      String rack1 = getRack(0, level);
      // use rack2 for task tracker
      String rack2 = getRack(1, level);
      Configuration conf = new Configuration();
      // Run a datanode on host1 under /a/b/c/..../d1/e1/f1
      dfs = new MiniDFSCluster(conf, 1, true, new String[] {rack1}, 
                               new String[] {"host1.com"});
      dfs.waitActive();
      fileSys = dfs.getFileSystem();
      if (!fileSys.mkdirs(inDir)) {
        throw new IOException("Mkdirs failed to create " + inDir.toString());
      }
      UtilsForTests.writeFile(dfs.getNameNode(), conf, 
    		                        new Path(inDir + "/file"), (short)1);
      namenode = (dfs.getFileSystem()).getUri().getHost() + ":" + 
                 (dfs.getFileSystem()).getUri().getPort();

      // Run a job with the (only)tasktracker on host2 under diff topology
      // e.g /a/b/c/..../d2/e2/f2. 
      JobConf jc = new JobConf();
      // cache-level = level (unshared levels) + 1(topmost shared node i.e /a) 
      //               + 1 (for host)
      jc.setInt("mapred.task.cache.levels", level + 2);
      mr = new MiniMRCluster(taskTrackers, namenode, 1, new String[] {rack2}, 
    		                 new String[] {"host2.com"}, jc);

      /* The job is configured with 1 map for one (non-splittable) file. 
       * Since the datanode is running under different subtree, there is no
       * node-level data locality but there should be topological locality.
       */
      TestRackAwareTaskPlacement.launchJobAndTestCounters(
    		  testName, mr, fileSys, inDir, outputPath, 1, 1, 0, 0);
      mr.shutdown();
    } finally {
      fileSys.delete(inDir, true);
      fileSys.delete(outputPath, true);
      if (dfs != null) { 
        dfs.shutdown(); 
      }
    }
  }
}
