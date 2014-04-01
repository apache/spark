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

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;

import static org.junit.Assert.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestFairSchedulerPoolNames {

  final static String TEST_DIR = new File(System.getProperty("test.build.data",
      "build/contrib/streaming/test/data")).getAbsolutePath();
  final static String ALLOC_FILE = new File(TEST_DIR, "test-pools")
      .getAbsolutePath();

  private static final String POOL_PROPERTY = "pool";
  private String namenode;
  private MiniDFSCluster miniDFSCluster = null;
  private MiniMRCluster miniMRCluster = null;

  /**
   * Note that The PoolManager.ALLOW_UNDECLARED_POOLS_KEY property is set to
   * false. So, the default pool is not added, and only pool names in the
   * scheduler allocation file are considered valid.
   */
  @Before
  public void setUp() throws Exception {
    new File(TEST_DIR).mkdirs(); // Make sure data directory exists
    // Create an allocation file with only one pool defined.
    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    out.println("<pool name=\"poolA\">");
    out.println("<minMaps>1</minMaps>");
    out.println("<minReduces>1</minReduces>");
    out.println("</pool>");
    out.println("</allocations>");
    out.close();

    Configuration conf = new Configuration();
    miniDFSCluster = new MiniDFSCluster(conf, 1, true, null);
    namenode = miniDFSCluster.getFileSystem().getUri().toString();

    JobConf clusterConf = new JobConf();
    clusterConf.set("mapred.jobtracker.taskScheduler", FairScheduler.class
        .getName());
    clusterConf.set("mapred.fairscheduler.allocation.file", ALLOC_FILE);
    clusterConf.set("mapred.fairscheduler.poolnameproperty", POOL_PROPERTY);
    clusterConf.setBoolean(FairScheduler.ALLOW_UNDECLARED_POOLS_KEY, false);
    miniMRCluster = new MiniMRCluster(1, namenode, 1, null, null, clusterConf);
  }

  @After
  public void tearDown() throws Exception {
    if (miniDFSCluster != null) {
      miniDFSCluster.shutdown();
    }
    if (miniMRCluster != null) {
      miniMRCluster.shutdown();
    }
  }

  private void submitJob(String pool) throws IOException {
    JobConf conf = new JobConf();
    final Path inDir = new Path("/tmp/testing/wc/input");
    final Path outDir = new Path("/tmp/testing/wc/output");
    FileSystem fs = FileSystem.get(URI.create(namenode), conf);
    fs.delete(outDir, true);
    if (!fs.mkdirs(inDir)) {
      throw new IOException("Mkdirs failed to create " + inDir.toString());
    }
    DataOutputStream file = fs.create(new Path(inDir, "part-00000"));
    file.writeBytes("Sample text");
    file.close();

    FileSystem.setDefaultUri(conf, namenode);
    conf.set("mapred.job.tracker", "localhost:"
        + miniMRCluster.getJobTrackerPort());
    conf.setJobName("wordcount");
    conf.setInputFormat(TextInputFormat.class);

    // the keys are words (strings)
    conf.setOutputKeyClass(Text.class);
    // the values are counts (ints)
    conf.setOutputValueClass(IntWritable.class);

    conf.setMapperClass(WordCount.MapClass.class);
    conf.setCombinerClass(WordCount.Reduce.class);
    conf.setReducerClass(WordCount.Reduce.class);
    FileInputFormat.setInputPaths(conf, inDir);
    FileOutputFormat.setOutputPath(conf, outDir);
    conf.setNumMapTasks(1);
    conf.setNumReduceTasks(0);
    if (pool != null) {
      conf.set(POOL_PROPERTY, pool);
    }
    JobClient.runJob(conf);
  }

  /**
   * Tests job submission using the default pool name.
   */
  @Test
  public void testDefaultPoolName() {
    Throwable t = null;
    try {
      submitJob(null);
    } catch (Exception e) {
      t = e;
    }
    assertNotNull("No exception during submission", t);
    assertTrue("Incorrect exception message", t.getMessage().contains(
        "Add pool name to the fair scheduler allocation file"));
  }

  /**
   * Tests job submission using a valid pool name (i.e., name exists in the fair
   * scheduler allocation file).
   */
  @Test
  public void testValidPoolName() {
    Throwable t = null;
    try {
      submitJob("poolA");
    } catch (Exception e) {
      t = e;
    }
    assertNull("Exception during submission", t);
  }

  /**
   * Tests job submission using an invalid pool name (i.e., name doesn't exist
   * in the fair scheduler allocation file).
   */
  @Test
  public void testInvalidPoolName() {
    Throwable t = null;
    try {
      submitJob("poolB");
    } catch (Exception e) {
      t = e;
    }
    assertNotNull("No exception during submission", t);
    assertTrue("Incorrect exception message", t.getMessage().contains(
        "Add pool name to the fair scheduler allocation file"));
  }

}
