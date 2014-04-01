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

package org.apache.hadoop.mapred.pipes;

import java.security.PrivilegedExceptionAction;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.ClusterWithLinuxTaskController;
import org.apache.hadoop.mapred.JobConf;

/**
 * Test Pipes jobs with LinuxTaskController running the jobs as a user different
 * from the user running the cluster. See {@link ClusterWithLinuxTaskController}
 */
public class TestPipesAsDifferentUser extends ClusterWithLinuxTaskController {

  private static final Log LOG =
      LogFactory.getLog(TestPipesAsDifferentUser.class);

  public void testPipes()
      throws Exception {
    if (System.getProperty("compile.c++") == null) {
      LOG.info("compile.c++ is not defined, so skipping TestPipes");
      return;
    }

    if (!shouldRun()) {
      return;
    }

    super.startCluster();
    jobOwner.doAs(new PrivilegedExceptionAction<Object>() {
      public Object run() throws Exception {
        JobConf clusterConf = getClusterConf();
        Path inputPath = new Path(homeDirectory, "in");
        Path outputPath = new Path(homeDirectory, "out");

        TestPipes.writeInputFile(FileSystem.get(clusterConf), inputPath);
        TestPipes.runProgram(mrCluster, dfsCluster, TestPipes.wordCountSimple,
            inputPath, outputPath, 3, 2, TestPipes.twoSplitOutput, clusterConf);
        assertOwnerShip(outputPath);
        TestPipes.cleanup(dfsCluster.getFileSystem(), outputPath);

        TestPipes.runProgram(mrCluster, dfsCluster, TestPipes.wordCountSimple,
            inputPath, outputPath, 3, 0, TestPipes.noSortOutput, clusterConf);
        assertOwnerShip(outputPath);
        TestPipes.cleanup(dfsCluster.getFileSystem(), outputPath);

        TestPipes.runProgram(mrCluster, dfsCluster, TestPipes.wordCountPart,
            inputPath, outputPath, 3, 2, TestPipes.fixedPartitionOutput,
            clusterConf);
        assertOwnerShip(outputPath);
        TestPipes.cleanup(dfsCluster.getFileSystem(), outputPath);

        TestPipes.runNonPipedProgram(mrCluster, dfsCluster,
            TestPipes.wordCountNoPipes, clusterConf);
        assertOwnerShip(TestPipes.nonPipedOutDir, FileSystem
            .getLocal(clusterConf));
        return null;
      }
    });
  }
}
