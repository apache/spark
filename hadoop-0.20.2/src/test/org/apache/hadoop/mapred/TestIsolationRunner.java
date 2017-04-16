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
import java.io.IOException;
import java.util.UUID;

import javax.security.auth.login.LoginException;

import junit.framework.TestCase;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.security.UserGroupInformation;

/** 
 * Re-runs a map task using the IsolationRunner. 
 *
 * The task included here is an identity mapper that touches
 * a file in a side-effect directory.  This is used
 * to verify that the task in fact ran.
 */
public class TestIsolationRunner extends TestCase {

  private static final String SIDE_EFFECT_DIR_PROPERTY =
    "test.isolationrunner.sideeffectdir";
  private static String TEST_ROOT_DIR = new File(System.getProperty(
      "test.build.data", "/tmp")).toURI().toString().replace(' ', '+');
  
  /** Identity mapper that also creates a side effect file. */
  static class SideEffectMapper<K, V> extends IdentityMapper<K, V> {
    private JobConf conf;
    @Override
    public void configure(JobConf conf) {
      this.conf = conf;
    }
    @Override
    public void close() throws IOException {
      writeSideEffectFile(conf, "map");
    }
  }

  static class SideEffectReducer<K, V> extends IdentityReducer<K, V> {
    private JobConf conf;
    @Override
    public void configure(JobConf conf) {
      this.conf = conf;
    }
    @Override
    public void close() throws IOException {
      writeSideEffectFile(conf, "reduce");
    }
  }

  private static void deleteSideEffectFiles(JobConf conf) throws IOException {
    FileSystem localFs = FileSystem.getLocal(conf);
    localFs.delete(new Path(conf.get(SIDE_EFFECT_DIR_PROPERTY)), true);
    assertEquals(0, countSideEffectFiles(conf, ""));
  }
  
  private static void writeSideEffectFile(JobConf conf, String prefix)
      throws IOException {
    FileSystem localFs = FileSystem.getLocal(conf);
    Path sideEffectFile = new Path(conf.get(SIDE_EFFECT_DIR_PROPERTY),
        prefix + "-" + UUID.randomUUID().toString());
    localFs.create(sideEffectFile).close();
  }
  
  private static int countSideEffectFiles(JobConf conf, final String prefix)
      throws IOException {
    FileSystem localFs = FileSystem.getLocal(conf);
    FileStatus[] files = localFs.listStatus(
        new Path(conf.get(SIDE_EFFECT_DIR_PROPERTY)), new PathFilter() {
      @Override public boolean accept(Path path) {
        return path.getName().startsWith(prefix + "-");
      }
    });
    return files.length;
  }
  
  private Path getAttemptJobXml(JobConf conf, JobID jobId, boolean isMap)
      throws IOException, LoginException {
    String taskid =
        new TaskAttemptID(new TaskID(jobId, isMap, 0), 0).toString();
    return new LocalDirAllocator("mapred.local.dir").getLocalPathToRead(
        TaskTracker.getTaskConfFile(UserGroupInformation.getCurrentUser()
            .getUserName(), jobId.toString(), taskid, false), conf);
  }

  public void testIsolationRunOfMapTask()
      throws IOException,
      InterruptedException,
      ClassNotFoundException,
      LoginException {
    MiniMRCluster mr = null;
    try {
      mr = new MiniMRCluster(1, "file:///", 4);

      // Run a job succesfully; keep task files.
      JobConf conf = mr.createJobConf();
      conf.setKeepTaskFilesPattern(".*");
      conf.set(SIDE_EFFECT_DIR_PROPERTY, TEST_ROOT_DIR +
          "/isolationrunnerjob/sideeffect");
      // Delete previous runs' data.
      deleteSideEffectFiles(conf);
      JobID jobId = runJobNormally(conf);
      assertEquals(1, countSideEffectFiles(conf, "map"));
      assertEquals(1, countSideEffectFiles(conf, "reduce"));
      
      deleteSideEffectFiles(conf);

      // Retrieve succesful job's configuration and 
      // run IsolationRunner against the map task.
      FileSystem localFs = FileSystem.getLocal(conf);
      Path mapJobXml =
          getAttemptJobXml(
              mr.getTaskTrackerRunner(0).getTaskTracker().getJobConf(),
              jobId, true).makeQualified(localFs);
      assertTrue(localFs.exists(mapJobXml));
      
      new IsolationRunner().run(new String[] {
          new File(mapJobXml.toUri()).getCanonicalPath() });
      
      assertEquals(1, countSideEffectFiles(conf, "map"));
      assertEquals(0, countSideEffectFiles(conf, "reduce"));

      // Clean up
      deleteSideEffectFiles(conf);
    } finally {
      if (mr != null) {
        mr.shutdown();
      }
    }
  }

  static JobID runJobNormally(JobConf conf) throws IOException {
    final Path inDir = new Path(TEST_ROOT_DIR + "/isolationrunnerjob/input");
    final Path outDir = new Path(TEST_ROOT_DIR + "/isolationrunnerjob/output");

    FileSystem fs = FileSystem.get(conf);
    fs.delete(outDir, true);
    if (!fs.exists(inDir)) {
      fs.mkdirs(inDir);
    }
    String input = "The quick brown fox jumps over lazy dog\n";
    DataOutputStream file = fs.create(new Path(inDir, "file"));
    file.writeBytes(input);
    file.close();

    conf.setInputFormat(TextInputFormat.class);
    conf.setMapperClass(SideEffectMapper.class);
    conf.setReducerClass(SideEffectReducer.class);

    FileInputFormat.setInputPaths(conf, inDir);
    FileOutputFormat.setOutputPath(conf, outDir);
    conf.setNumMapTasks(1);
    conf.setNumReduceTasks(1);

    JobClient jobClient = new JobClient(conf);
    RunningJob job = jobClient.submitJob(conf);
    job.waitForCompletion();
    return job.getID();
  }
}
