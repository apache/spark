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

package org.apache.hadoop.streaming;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.StringWriter;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MiniMRCluster;
import org.apache.hadoop.mapred.Utils;
/**
 * This test case tests the symlink creation
 * utility provided by distributed caching 
 */
public class TestMultipleCachefiles extends TestCase
{
  String INPUT_FILE = "/testing-streaming/input.txt";
  String OUTPUT_DIR = "/testing-streaming/out";
  String CACHE_FILE = "/testing-streaming/cache.txt";
  String CACHE_FILE_2 = "/testing-streaming/cache2.txt";
  String input = "check to see if we can read this none reduce";
  String map = "xargs cat ";
  String reduce = "cat";
  String mapString = "testlink";
  String mapString2 = "testlink2";
  String cacheString = "This is just the cache string";
  String cacheString2 = "This is just the second cache string";
  StreamJob job;

  public TestMultipleCachefiles() throws IOException
  {
  }

  public void testMultipleCachefiles() throws Exception
  {
    boolean mayExit = false;
    MiniMRCluster mr = null;
    MiniDFSCluster dfs = null; 
    try{
      Configuration conf = new Configuration();
      dfs = new MiniDFSCluster(conf, 1, true, null);
      FileSystem fileSys = dfs.getFileSystem();
      String namenode = fileSys.getName();
      mr  = new MiniMRCluster(1, namenode, 3);
      // During tests, the default Configuration will use a local mapred
      // So don't specify -config or -cluster
      String strJobtracker = "mapred.job.tracker=" + "localhost:" + mr.getJobTrackerPort();
      String strNamenode = "fs.default.name=" + namenode;
      String argv[] = new String[] {
        "-input", INPUT_FILE,
        "-output", OUTPUT_DIR,
        "-mapper", map,
        "-reducer", reduce,
        //"-verbose",
        //"-jobconf", "stream.debug=set"
        "-jobconf", strNamenode,
        "-jobconf", strJobtracker,
        "-jobconf", "stream.tmpdir="+System.getProperty("test.build.data","/tmp"),
        "-jobconf", 
          JobConf.MAPRED_MAP_TASK_JAVA_OPTS + "=" +
            "-Dcontrib.name=" + System.getProperty("contrib.name") + " " +
            "-Dbuild.test=" + System.getProperty("build.test") + " " +
            conf.get(JobConf.MAPRED_MAP_TASK_JAVA_OPTS, 
                     conf.get(JobConf.MAPRED_TASK_JAVA_OPTS, "")),
        "-jobconf", 
          JobConf.MAPRED_REDUCE_TASK_JAVA_OPTS + "=" +
            "-Dcontrib.name=" + System.getProperty("contrib.name") + " " +
            "-Dbuild.test=" + System.getProperty("build.test") + " " +
            conf.get(JobConf.MAPRED_REDUCE_TASK_JAVA_OPTS, 
                     conf.get(JobConf.MAPRED_TASK_JAVA_OPTS, "")),
        "-cacheFile", "hdfs://"+fileSys.getName()+CACHE_FILE + "#" + mapString,
        "-cacheFile", "hdfs://"+fileSys.getName()+CACHE_FILE_2 + "#" + mapString2
      };

      fileSys.delete(new Path(OUTPUT_DIR));
      
      DataOutputStream file = fileSys.create(new Path(INPUT_FILE));
      file.writeBytes(mapString + "\n");
      file.writeBytes(mapString2 + "\n");
      file.close();
      file = fileSys.create(new Path(CACHE_FILE));
      file.writeBytes(cacheString);
      file.close();
      file = fileSys.create(new Path(CACHE_FILE_2));
      file.writeBytes(cacheString2);
      file.close();
        
      job = new StreamJob(argv, mayExit);     
      job.go();

      fileSys = dfs.getFileSystem();
      String line = null;
      String line2 = null;
      Path[] fileList = FileUtil.stat2Paths(fileSys.listStatus(
                                   new Path(OUTPUT_DIR),
                                   new Utils.OutputFileUtils
                                     .OutputFilesFilter()));
      for (int i = 0; i < fileList.length; i++){
        System.out.println(fileList[i].toString());
        BufferedReader bread =
          new BufferedReader(new InputStreamReader(fileSys.open(fileList[i])));
        line = bread.readLine();
        System.out.println(line);
        line2 = bread.readLine();
        System.out.println(line2);
      }
      assertEquals(cacheString + "\t", line);
      assertEquals(cacheString2 + "\t", line2);
    } finally{
      if (dfs != null) { dfs.shutdown(); }
      if (mr != null) { mr.shutdown();}
    }
    
  }

  void failTrace(Exception e)
  {
    StringWriter sw = new StringWriter();
    e.printStackTrace(new PrintWriter(sw));
    fail(sw.toString());
  }

  public static void main(String[]args) throws Exception
  {
    new TestMultipleCachefiles().testMultipleCachefiles();
  }

}
