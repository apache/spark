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

import junit.framework.TestCase;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.IOException;

/**
 * Abstract Test case class to run MR in local or cluster mode and in local FS
 * or DFS.
 *
 * The Hadoop instance is started and stopped on each test method.
 *
 * If using DFS the filesystem is reformated at each start (test method).
 *
 * Job Configurations should be created using a configuration returned by the
 * 'createJobConf()' method.
 */
public abstract class HadoopTestCase extends TestCase {
  public static final int LOCAL_MR = 1;
  public static final int CLUSTER_MR = 2;
  public static final int LOCAL_FS = 4;
  public static final int DFS_FS = 8;

  private boolean localMR;
  private boolean localFS;

  private int taskTrackers;
  private int dataNodes;

  /**
   * Creates a testcase for local or cluster MR using DFS.
   *
   * The DFS will be formatted regardless if there was one or not before in the
   * given location.
   *
   * @param mrMode indicates if the MR should be local (LOCAL_MR) or cluster
   * (CLUSTER_MR)
   * @param fsMode indicates if the FS should be local (LOCAL_FS) or DFS (DFS_FS)
   *
   * local FS when using relative PATHs)
   *
   * @param taskTrackers number of task trackers to start when using cluster
   *
   * @param dataNodes number of data nodes to start when using DFS
   *
   * @throws IOException thrown if the base directory cannot be set.
   */
  public HadoopTestCase(int mrMode, int fsMode, int taskTrackers, int dataNodes)
    throws IOException {
    if (mrMode != LOCAL_MR && mrMode != CLUSTER_MR) {
      throw new IllegalArgumentException(
                                         "Invalid MapRed mode, must be LOCAL_MR or CLUSTER_MR");
    }
    if (fsMode != LOCAL_FS && fsMode != DFS_FS) {
      throw new IllegalArgumentException(
                                         "Invalid FileSystem mode, must be LOCAL_FS or DFS_FS");
    }
    if (taskTrackers < 1) {
      throw new IllegalArgumentException(
                                         "Invalid taskTrackers value, must be greater than 0");
    }
    if (dataNodes < 1) {
      throw new IllegalArgumentException(
                                         "Invalid dataNodes value, must be greater than 0");
    }
    localMR = (mrMode == LOCAL_MR);
    localFS = (fsMode == LOCAL_FS);
    /*
      JobConf conf = new JobConf();
      fsRoot = conf.get("hadoop.tmp.dir");

      if (fsRoot == null) {
      throw new IllegalArgumentException(
      "hadoop.tmp.dir is not defined");
      }

      fsRoot = fsRoot.replace(' ', '+') + "/fs";

      File file = new File(fsRoot);
      if (!file.exists()) {
      if (!file.mkdirs()) {
      throw new RuntimeException("Could not create FS base path: " + file);
      }
      }
    */
    this.taskTrackers = taskTrackers;
    this.dataNodes = dataNodes;
  }

  /**
   * Indicates if the MR is running in local or cluster mode.
   *
   * @return returns TRUE if the MR is running locally, FALSE if running in
   * cluster mode.
   */
  public boolean isLocalMR() {
    return localMR;
  }

  /**
   * Indicates if the filesystem is local or DFS.
   *
   * @return returns TRUE if the filesystem is local, FALSE if it is DFS.
   */
  public boolean isLocalFS() {
    return localFS;
  }


  private MiniDFSCluster dfsCluster = null;
  private MiniMRCluster mrCluster = null;
  private FileSystem fileSystem = null;

  /**
   * Creates Hadoop instance based on constructor configuration before
   * a test case is run.
   *
   * @throws Exception
   */
  protected void setUp() throws Exception {
    super.setUp();
    if (localFS) {
      fileSystem = FileSystem.getLocal(new JobConf());
    }
    else {
      dfsCluster = new MiniDFSCluster(new JobConf(), dataNodes, true, null);
      fileSystem = dfsCluster.getFileSystem();
    }
    if (localMR) {
    }
    else {
      //noinspection deprecation
      mrCluster = new MiniMRCluster(taskTrackers, fileSystem.getName(), 1);
    }
  }

  /**
   * Destroys Hadoop instance based on constructor configuration after
   * a test case is run.
   *
   * @throws Exception
   */
  protected void tearDown() throws Exception {
    try {
      if (mrCluster != null) {
        mrCluster.shutdown();
      }
    }
    catch (Exception ex) {
      System.out.println(ex);
    }
    try {
      if (dfsCluster != null) {
        dfsCluster.shutdown();
      }
    }
    catch (Exception ex) {
      System.out.println(ex);
    }
    super.tearDown();
  }

  /**
   * Returns the Filesystem in use.
   *
   * TestCases should use this Filesystem as it
   * is properly configured with the workingDir for relative PATHs.
   *
   * @return the filesystem used by Hadoop.
   */
  protected FileSystem getFileSystem() {
    return fileSystem;
  }

  /**
   * Returns a job configuration preconfigured to run against the Hadoop
   * managed by the testcase.
   * @return configuration that works on the testcase Hadoop instance
   */
  protected JobConf createJobConf() {
    return (localMR) ? new JobConf() : mrCluster.createJobConf();
  }

}
