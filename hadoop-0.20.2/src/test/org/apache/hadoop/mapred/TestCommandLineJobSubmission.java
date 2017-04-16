/* Licensed to the Apache Software Foundation (ASF) under one
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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.hdfs.MiniDFSCluster;

/**
 * check for the job submission  options of 
 * -libjars -files -archives
 */

public class TestCommandLineJobSubmission extends TestCase {
  // Input output paths for this.. 
  // these are all dummy and does not test
  // much in map reduce except for the command line
  // params 
  static final Path input = new Path("/test/input/");
  static final Path output = new Path("/test/output");
  File buildDir = new File(System.getProperty("test.build.data", "/tmp"));
  public void testJobShell() throws Exception {
    MiniDFSCluster dfs = null;
    MiniMRCluster mr = null;
    FileSystem fs = null;
    Path testFile = new Path(input, "testfile");
    try {
      Configuration conf = new Configuration();
      //start the mini mr and dfs cluster.
      dfs = new MiniDFSCluster(conf, 2 , true, null);
      fs = dfs.getFileSystem();
      FSDataOutputStream stream = fs.create(testFile);
      stream.write("teststring".getBytes());
      stream.close();
      mr = new MiniMRCluster(2, fs.getUri().toString(), 1);
      File thisbuildDir = new File(buildDir, "jobCommand");
      assertTrue("create build dir", thisbuildDir.mkdirs()); 
      File f = new File(thisbuildDir, "files_tmp");
      FileOutputStream fstream = new FileOutputStream(f);
      fstream.write("somestrings".getBytes());
      fstream.close();
      File f1 = new File(thisbuildDir, "files_tmp1");
      fstream = new FileOutputStream(f1);
      fstream.write("somestrings".getBytes());
      fstream.close();
      
      // copy files to dfs
      Path cachePath = new Path("/cacheDir");
      if (!fs.mkdirs(cachePath)) {
        throw new IOException(
            "Mkdirs failed to create " + cachePath.toString());
      }
      Path localCachePath = new Path(System.getProperty("test.cache.data"));
      Path txtPath = new Path(localCachePath, new Path("test.txt"));
      Path jarPath = new Path(localCachePath, new Path("test.jar"));
      Path zipPath = new Path(localCachePath, new Path("test.zip"));
      Path tarPath = new Path(localCachePath, new Path("test.tar"));
      Path tgzPath = new Path(localCachePath, new Path("test.tgz"));
      fs.copyFromLocalFile(txtPath, cachePath);
      fs.copyFromLocalFile(jarPath, cachePath);
      fs.copyFromLocalFile(zipPath, cachePath);

      // construct options for -files
      String[] files = new String[3];
      files[0] = f.toString();
      files[1] = f1.toString() + "#localfilelink";
      files[2] = 
        fs.getUri().resolve(cachePath + "/test.txt#dfsfilelink").toString();

      // construct options for -libjars
      String[] libjars = new String[2];
      libjars[0] = "build/test/testjar/testjob.jar";
      libjars[1] = fs.getUri().resolve(cachePath + "/test.jar").toString();
      
      // construct options for -archives
      String[] archives = new String[3];
      archives[0] = tgzPath.toString();
      archives[1] = tarPath + "#tarlink";
      archives[2] = 
        fs.getUri().resolve(cachePath + "/test.zip#ziplink").toString();
      
      String[] args = new String[8];
      args[0] = "-files";
      args[1] = StringUtils.arrayToString(files);
      args[2] = "-libjars";
      // the testjob.jar as a temporary jar file 
      // rather than creating its own
      args[3] = StringUtils.arrayToString(libjars);
      args[4] = "-archives";
      args[5] = StringUtils.arrayToString(archives);
      args[6] = input.toString();
      args[7] = output.toString();
      
      JobConf jobConf = mr.createJobConf();
      //before running the job, verify that libjar is not in client classpath
      assertTrue("libjar not in client classpath", loadLibJar(jobConf)==null);
      int ret = ToolRunner.run(jobConf,
                               new testshell.ExternalMapReduce(), args);
      //after running the job, verify that libjar is in the client classpath
      assertTrue("libjar added to client classpath", loadLibJar(jobConf)!=null);
      
      assertTrue("not failed ", ret != -1);
      f.delete();
      thisbuildDir.delete();
      
      // test duplicate uris for options -files and -archives
      testDuplicateURI(mr, dfs);
    } finally {
      if (dfs != null) {dfs.shutdown();};
      if (mr != null) {mr.shutdown();};
    }
  }
  
  
  private void testDuplicateURI(MiniMRCluster mr, MiniDFSCluster dfs)
      throws Exception {
    Configuration jobConf = mr.createJobConf();
    FileSystem fs = dfs.getFileSystem();
    Path dfsPath = new Path("/test/testjob.jar");
    fs.copyFromLocalFile(new Path("build/test/testjar/testjob.jar"), dfsPath);
    String url = fs.getDefaultUri(jobConf).toString() + dfsPath.toString();
    String[] args = new String[6];
    args[0] = "-files";
    args[1] = url;
    args[2] = "-archives";
    args[3] = url;
    args[4] = input.toString();
    args[5] = output.toString();

    Exception ex = null;
    try {
      int ret = ToolRunner
          .run(jobConf, new testshell.ExternalMapReduce(), args);
    } catch (Exception e) {
      ex = e;
    }
    assertNotNull("No exception thrown", ex);
    assertTrue("Exception is not InvalidJobConfException.",
        ex instanceof InvalidJobConfException);
    assertEquals("Wrong message for the exception", "The core URI, \"" + url
        + "\" is listed both in " + DistributedCache.CACHE_FILES + " and in "
        + DistributedCache.CACHE_ARCHIVES + " .", ex.getMessage());
  }

  @SuppressWarnings("unchecked")
  private Class loadLibJar(JobConf jobConf) {
    try {
      return jobConf.getClassByName("testjar.ClassWordCount");
    } catch (ClassNotFoundException e) {
      return null;
    }
  }
}
