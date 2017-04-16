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
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.SortValidator.RecordStatsChecker.NonSplitableSequenceFileInputFormat;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.mapred.lib.IdentityReducer;

public class TestRackAwareTaskPlacement extends TestCase {
  private static final String rack1[] = new String[] {
    "/r1"
  };
  private static final String hosts1[] = new String[] {
    "host1.rack1.com"
  };
  private static final String rack2[] = new String[] {
    "/r2", "/r2"
  };
  private static final String hosts2[] = new String[] {
    "host1.rack2.com", "host2.rack2.com"
  };
  private static final String hosts3[] = new String[] {
    "host3.rack1.com"
  };
  private static final String hosts4[] = new String[] {
    "host1.rack2.com"
  };
  final Path inDir = new Path("/racktesting");
  final Path outputPath = new Path("/output");
  
  /**
   * Launches a MR job and tests the job counters against the expected values.
   * @param testName The name for the job
   * @param mr The MR cluster
   * @param fileSys The FileSystem
   * @param in Input path
   * @param out Output path
   * @param numMaps Number of maps
   * @param otherLocalMaps Expected value of other local maps
   * @param datalocalMaps Expected value of data(node) local maps
   * @param racklocalMaps Expected value of rack local maps
   */
  static void launchJobAndTestCounters(String jobName, MiniMRCluster mr, 
                                       FileSystem fileSys, Path in, Path out,
                                       int numMaps, int otherLocalMaps,
                                       int dataLocalMaps, int rackLocalMaps) 
  throws IOException {
    JobConf jobConf = mr.createJobConf();
    if (fileSys.exists(out)) {
        fileSys.delete(out, true);
    }
    RunningJob job = launchJob(jobConf, in, out, numMaps, jobName);
    Counters counters = job.getCounters();
    assertEquals("Number of local maps", 
            counters.getCounter(JobInProgress.Counter.OTHER_LOCAL_MAPS), otherLocalMaps);
    assertEquals("Number of Data-local maps", 
            counters.getCounter(JobInProgress.Counter.DATA_LOCAL_MAPS), 
                                dataLocalMaps);
    assertEquals("Number of Rack-local maps", 
            counters.getCounter(JobInProgress.Counter.RACK_LOCAL_MAPS), 
                                rackLocalMaps);
    mr.waitUntilIdle();
    mr.shutdown();
  }

  public void testTaskPlacement() throws IOException {
    String namenode = null;
    MiniDFSCluster dfs = null;
    MiniMRCluster mr = null;
    FileSystem fileSys = null;
    String testName = "TestForRackAwareness";
    try {
      final int taskTrackers = 1;

      /* Start 3 datanodes, one in rack r1, and two in r2. Create three
       * files (splits).
       * 1) file1, just after starting the datanode on r1, with 
       *    a repl factor of 1, and,
       * 2) file2 & file3 after starting the other two datanodes, with a repl 
       *    factor of 3.
       * At the end, file1 will be present on only datanode1, and, file2 and 
       * file3, will be present on all datanodes. 
       */
      Configuration conf = new Configuration();
      conf.setBoolean("dfs.replication.considerLoad", false);
      dfs = new MiniDFSCluster(conf, 1, true, rack1, hosts1);
      dfs.waitActive();
      fileSys = dfs.getFileSystem();
      if (!fileSys.mkdirs(inDir)) {
        throw new IOException("Mkdirs failed to create " + inDir.toString());
      }
      UtilsForTests.writeFile(dfs.getNameNode(), conf, new Path(inDir + "/file1"), (short)1);
      dfs.startDataNodes(conf, 2, true, null, rack2, hosts2, null);
      dfs.waitActive();

      UtilsForTests.writeFile(dfs.getNameNode(), conf, new Path(inDir + "/file2"), (short)3);
      UtilsForTests.writeFile(dfs.getNameNode(), conf, new Path(inDir + "/file3"), (short)3);
      
      namenode = (dfs.getFileSystem()).getUri().getHost() + ":" + 
                 (dfs.getFileSystem()).getUri().getPort(); 
      /* Run a job with the (only)tasktracker on rack2. The rack location
       * of the tasktracker will determine how many data/rack local maps it
       * runs. The hostname of the tasktracker is set to same as one of the 
       * datanodes.
       */
      mr = new MiniMRCluster(taskTrackers, namenode, 1, rack2, hosts4);

      /* The job is configured with three maps since there are three 
       * (non-splittable) files. On rack2, there are two files and both
       * have repl of three. The blocks for those files must therefore be
       * present on all the datanodes, in particular, the datanodes on rack2.
       * The third input file is pulled from rack1.
       */
      launchJobAndTestCounters(testName, mr, fileSys, inDir, outputPath, 3, 0,
                               2, 0);
      mr.shutdown();
      
      /* Run a job with the (only)tasktracker on rack1.
       */
      mr = new MiniMRCluster(taskTrackers, namenode, 1, rack1, hosts3);

      /* The job is configured with three maps since there are three 
       * (non-splittable) files. On rack1, because of the way in which repl
       * was setup while creating the files, we will have all the three files. 
       * Thus, a tasktracker will find all inputs in this rack.
       */
      launchJobAndTestCounters(testName, mr, fileSys, inDir, outputPath, 3, 0,
                               0, 3);
      mr.shutdown();
      
    } finally {
      if (dfs != null) { 
        dfs.shutdown(); 
      }
      if (mr != null) { 
        mr.shutdown();
      }
    }
  }
  static RunningJob launchJob(JobConf jobConf, Path inDir, Path outputPath, 
                              int numMaps, String jobName) throws IOException {
    jobConf.setJobName(jobName);
    jobConf.setInputFormat(NonSplitableSequenceFileInputFormat.class);
    jobConf.setOutputFormat(SequenceFileOutputFormat.class);
    FileInputFormat.setInputPaths(jobConf, inDir);
    FileOutputFormat.setOutputPath(jobConf, outputPath);
    jobConf.setMapperClass(IdentityMapper.class);
    jobConf.setReducerClass(IdentityReducer.class);
    jobConf.setOutputKeyClass(BytesWritable.class);
    jobConf.setOutputValueClass(BytesWritable.class);
    jobConf.setNumMapTasks(numMaps);
    jobConf.setNumReduceTasks(0);
    jobConf.setJar("build/test/testjar/testjob.jar");
    return JobClient.runJob(jobConf);
  }
}
