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
import java.io.PrintWriter;

import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.taglibs.standard.extra.spath.Predicate;

import org.mortbay.log.Log;

import junit.framework.TestCase;
import org.apache.hadoop.fs.Path;

/**
 * Test {@link JobTracker} w.r.t config parameters.
 */
public class TestJobHistoryConfig extends TestCase {
  // private MiniMRCluster mr = null;
  private MiniDFSCluster mdfs = null;
  private String namenode = null;
  FileSystem fileSys = null;
  final Path inDir = new Path("./input");
  final Path outDir = new Path("./output");

  private void setUpCluster(JobConf conf) throws IOException,
      InterruptedException {
    mdfs = new MiniDFSCluster(conf, 1, true, null);
    fileSys = mdfs.getFileSystem();
    namenode = fileSys.getUri().toString();
  }

  /**
   * Test case to make sure that JobTracker will start and JobHistory enabled
   * <ol>
   * <li>Run a job with valid jobhistory configuration</li>
   * <li>Check if JobTracker can start</li>
   * </ol>
   * 
   * @throws Exception
   */

  public void testJobHistoryWithValidConfiguration() throws Exception {
    try {
      JobConf conf = new JobConf();
      setUpCluster(conf);
      conf.set("hadoop.job.history.location", "/hadoop/history");
      conf = MiniMRCluster.configureJobConf(conf, namenode, 0, 0, null);
      boolean started = canStartJobTracker(conf);
      assertTrue(started);
    } finally {
      if (mdfs != null) {
        try {
          mdfs.shutdown();
        } catch (Exception e) {
        }
      }
    }
  }

  public static class MapperClass extends MapReduceBase implements
      Mapper<LongWritable, Text, Text, IntWritable> {
    public void configure(JobConf job) {
    }

    public void map(LongWritable key, Text value,
        OutputCollector<Text, IntWritable> output, Reporter reporter)
        throws IOException {
      throw new IOException();
    }
  }

  public void testJobHistoryLogging() throws Exception {
    JobConf conf = new JobConf();
    setUpCluster(conf);
    conf.setMapperClass(MapperClass.class);
    conf.setReducerClass(IdentityReducer.class);
    conf.setNumReduceTasks(0);
    JobClient jc = new JobClient(conf);
    conf.set("hadoop.job.history.location", "/hadoop/history");
    conf = MiniMRCluster.configureJobConf(conf, namenode, 0, 0, null);
    FileSystem inFs = inDir.getFileSystem(conf);
    if (!inFs.mkdirs(inDir)) {
      throw new IOException("Mkdirs failed to create " + inDir.toString());
    }
    FileInputFormat.setInputPaths(conf, inDir);
    FileOutputFormat.setOutputPath(conf, outDir);
    conf.setSpeculativeExecution(false);
    conf.setJobName("test");
    conf.setUser("testuser");
    conf.setQueueName("testQueue");
    String TEST_ROOT_DIR = new Path(System.getProperty("test.build.data",
        "/tmp")).toString().replace(' ', '+');
    JobTracker jt = JobTracker.startTracker(conf);
    assertTrue(jt != null);
    JobInProgress jip = new JobInProgress(new JobID("jt", 1),
        new JobConf(conf), jt);
    assertTrue(jip != null);
    jip.jobFile = "testfile";
    String historyFile = JobHistory.getHistoryFilePath(jip.getJobID());
    JobHistory.JobInfo.logSubmitted(jip.getJobID(), jip.getJobConf(),
        jip.jobFile, jip.startTime);
  }

  /**
   * Check whether the JobTracker can be started.
   * 
   * @throws IOException
   */
  private boolean canStartJobTracker(JobConf conf) throws InterruptedException,
      IOException {
    JobTracker jt = null;
    try {
      jt = JobTracker.startTracker(conf);
      Log.info("Started JobTracker");
    } catch (IOException e) {
      Log.info("Can not Start JobTracker", e.getLocalizedMessage());
      return false;
    }
    if (jt != null) {
      jt.fs.close();
      jt.stopTracker();
    }
    return true;
  }
}
