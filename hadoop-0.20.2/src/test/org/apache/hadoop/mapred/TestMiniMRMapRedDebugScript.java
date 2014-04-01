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
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

import junit.framework.TestCase;
import org.junit.Ignore;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.IdentityReducer;

/**
 * Class to test mapred debug Script
 */
@Ignore //disabled until we fix the issues in running debug scripts
public class TestMiniMRMapRedDebugScript extends TestCase {
  private static final Log LOG =
    LogFactory.getLog(TestMiniMRMapRedDebugScript.class.getName());

  private MiniMRCluster mr;
  private MiniDFSCluster dfs;
  private FileSystem fileSys;
  
  /**
   * Fail map class 
   */
  public static class MapClass extends MapReduceBase
  implements Mapper<LongWritable, Text, Text, IntWritable> {
     public void map (LongWritable key, Text value, 
                     OutputCollector<Text, IntWritable> output, 
                     Reporter reporter) throws IOException {
       System.err.println("Bailing out");
       throw new IOException();
     }
  }

  /**
   * Reads tasklog and returns it as string after trimming it.
   * @param filter Task log filter; can be STDOUT, STDERR,
   *                SYSLOG, DEBUGOUT, DEBUGERR
   * @param taskId The task id for which the log has to collected
   * @param isCleanup whether the task is a cleanup attempt or not.
   * @return task log as string
   * @throws IOException
   */
  public static String readTaskLog(TaskLog.LogName  filter, 
                                   TaskAttemptID taskId, 
                                   boolean isCleanup)
  throws IOException {
    // string buffer to store task log
    StringBuffer result = new StringBuffer();
    int res;

    // reads the whole tasklog into inputstream
    InputStream taskLogReader = new TaskLog.Reader(taskId, filter, 0, -1, isCleanup);
    // construct string log from inputstream.
    byte[] b = new byte[65536];
    while (true) {
      res = taskLogReader.read(b);
      if (res > 0) {
        result.append(new String(b));
      } else {
        break;
      }
    }
    taskLogReader.close();
    
    // trim the string and return it
    String str = result.toString();
    str = str.trim();
    return str;
  }

  /**
   * Launches failed map task and debugs the failed task
   * @param conf configuration for the mapred job
   * @param inDir input path
   * @param outDir output path
   * @param debugDir debug directory where script is present
   * @param debugCommand The command to execute script
   * @param input Input text
   * @return the output of debug script 
   * @throws IOException
   */
  public String launchFailMapAndDebug(JobConf conf,
                                      Path inDir,
                                      Path outDir,
                                      Path debugDir,
                                      String debugScript,
                                      String input)
  throws IOException {

    // set up the input file system and write input text.
    FileSystem inFs = inDir.getFileSystem(conf);
    FileSystem outFs = outDir.getFileSystem(conf);
    outFs.delete(outDir, true);
    if (!inFs.mkdirs(inDir)) {
      throw new IOException("Mkdirs failed to create " + inDir.toString());
    }
    {
      // write input into input file
      DataOutputStream file = inFs.create(new Path(inDir, "part-0"));
      file.writeBytes(input);
      file.close();
    }

    // configure the mapred Job for failing map task.
    conf.setJobName("failmap");
    conf.setMapperClass(MapClass.class);        
    conf.setReducerClass(IdentityReducer.class);
    conf.setNumMapTasks(1);
    conf.setNumReduceTasks(0);
    conf.setMapDebugScript(debugScript);
    FileInputFormat.setInputPaths(conf, inDir);
    FileOutputFormat.setOutputPath(conf, outDir);
    String TEST_ROOT_DIR = new Path(System.getProperty("test.build.data",
                                      "/tmp")).toString().replace(' ', '+');
    conf.set("test.build.data", TEST_ROOT_DIR);

    // copy debug script to cache from local file system.
    FileSystem debugFs = debugDir.getFileSystem(conf);
    Path scriptPath = new Path(debugDir,"testscript.txt");
    Path cachePath = new Path("/cacheDir");
    if (!debugFs.mkdirs(cachePath)) {
      throw new IOException("Mkdirs failed to create " + cachePath.toString());
    }
    debugFs.copyFromLocalFile(scriptPath,cachePath);
    
    URI uri = debugFs.getUri().resolve(cachePath+"/testscript.txt#testscript");
    DistributedCache.createSymlink(conf);
    DistributedCache.addCacheFile(uri, conf);

    RunningJob job =null;
    // run the job. It will fail with IOException.
    try {
      job = new JobClient(conf).submitJob(conf);
    } catch (IOException e) {
    	LOG.info("Running Job failed", e);
    }

    JobID jobId = job.getID();
    // construct the task id of first map task of failmap
    TaskAttemptID taskId = new TaskAttemptID(new TaskID(jobId,true, 0), 0);
    // wait for the job to finish.
    while (!job.isComplete()) ;
    
    // return the output of debugout log.
    return readTaskLog(TaskLog.LogName.DEBUGOUT,taskId, false);
  }

  /**
   * Tests Map task's debug script
   * 
   * In this test, we launch a mapreduce program which 
   * writes 'Bailing out' to stderr and throws an exception.
   * We will run the script when tsk fails and validate 
   * the output of debug out log. 
   *
   */
  public void testMapDebugScript() throws Exception {
    try {
      
      // create configuration, dfs, file system and mapred cluster 
      Configuration cnf = new Configuration();
      dfs = new MiniDFSCluster(cnf, 1, true, null);
      fileSys = dfs.getFileSystem();
      mr = new MiniMRCluster(2, fileSys.getName(), 1);
      JobConf conf = mr.createJobConf();
      
      // intialize input, output and debug directories
      final Path debugDir = new Path("build/test/debug");
      Path inDir = new Path("testing/wc/input");
      Path outDir = new Path("testing/wc/output");
      
      // initialize debug command and input text
      String debugScript = "./testscript";
      String input = "The input";
      
      // Launch failed map task and run debug script
      String result = launchFailMapAndDebug(conf,inDir, 
                               outDir,debugDir, debugScript, input);
      
      // Assert the output of debug script.
      assertTrue(result.contains("Test Script\nBailing out"));

    } finally {  
      // close file system and shut down dfs and mapred cluster
      try {
        if (fileSys != null) {
          fileSys.close();
        }
        if (dfs != null) {
          dfs.shutdown();
        }
        if (mr != null) {
          mr.shutdown();
        }
      } catch (IOException ioe) {
        LOG.info("IO exception in closing file system:"+ioe.getMessage(), ioe);
      }
    }
  }

  public static void main(String args[]){
    TestMiniMRMapRedDebugScript tmds = new TestMiniMRMapRedDebugScript();
    try {
      tmds.testMapDebugScript();
    } catch (Exception e) {
      LOG.error("Exception in test: "+e.getMessage(), e);
    }
  }
  
}

