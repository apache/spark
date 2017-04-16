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

import java.io.*;
import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;

/**
 * Class to test mapred task's temp directory
 */
public class TestMiniMRTaskTempDir extends TestCase {
  private static final Log LOG =
    LogFactory.getLog(TestMiniMRTaskTempDir.class.getName());

  private MiniMRCluster mr;
  private MiniDFSCluster dfs;
  private FileSystem fileSys;
  
  /**
   * Map class which checks whether temp directory exists
   * and check the value of java.io.tmpdir
   * Creates a tempfile and checks whether that is created in 
   * temp directory specified.
   */
  public static class MapClass extends MapReduceBase
  implements Mapper<LongWritable, Text, Text, IntWritable> {
	 Path tmpDir;
	 FileSystem localFs;
     public void map (LongWritable key, Text value, 
                     OutputCollector<Text, IntWritable> output, 
                     Reporter reporter) throws IOException {
       String tmp = null;
       if (localFs.exists(tmpDir)) {
         tmp = tmpDir.makeQualified(localFs).toString();

         assertEquals(tmp, new Path(System.getProperty("java.io.tmpdir")).
                                           makeQualified(localFs).toString());
       } else {
         fail("Temp directory "+tmpDir +" doesnt exist.");
       }
       File tmpFile = File.createTempFile("test", ".tmp");
       assertEquals(tmp, new Path(tmpFile.getParent()).
                                           makeQualified(localFs).toString());
     }
     public void configure(JobConf job) {
       tmpDir = new Path(job.get("mapred.child.tmp", "./tmp"));
       try {
         localFs = FileSystem.getLocal(job);
       } catch (IOException ioe) {
         ioe.printStackTrace();
         fail("IOException in getting localFS");
       }
     }
  }

  /**
   * Launch tests 
   * @param conf Configuration of the mapreduce job.
   * @param inDir input path
   * @param outDir output path
   * @param input Input text
   * @throws IOException
   */
  public void launchTest(JobConf conf,
                         Path inDir,
                         Path outDir,
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

    // configure the mapred Job which creates a tempfile in map.
    conf.setJobName("testmap");
    conf.setMapperClass(MapClass.class);        
    conf.setReducerClass(IdentityReducer.class);
    conf.setNumMapTasks(1);
    conf.setNumReduceTasks(0);
    FileInputFormat.setInputPaths(conf, inDir);
    FileOutputFormat.setOutputPath(conf, outDir);
    String TEST_ROOT_DIR = new Path(System.getProperty("test.build.data",
                                      "/tmp")).toString().replace(' ', '+');
    conf.set("test.build.data", TEST_ROOT_DIR);

    // Launch job with default option for temp dir. 
    // i.e. temp dir is ./tmp 
    JobClient.runJob(conf);
    outFs.delete(outDir, true);

    // Launch job by giving relative path to temp dir.
    conf.set("mapred.child.tmp", "../temp");
    JobClient.runJob(conf);
    outFs.delete(outDir, true);

    // Launch job by giving absolute path to temp dir
    conf.set("mapred.child.tmp", "/tmp");
    JobClient.runJob(conf);
    outFs.delete(outDir, true);
  }

  /**
   * Tests task's temp directory.
   * 
   * In this test, we give different values to mapred.child.tmp
   * both relative and absolute. And check whether the temp directory 
   * is created. We also check whether java.io.tmpdir value is same as 
   * the directory specified. We create a temp file and check if is is 
   * created in the directory specified.
   */
  public void testTaskTempDir(){
    try {
      
      // create configuration, dfs, file system and mapred cluster 
      dfs = new MiniDFSCluster(new Configuration(), 1, true, null);
      fileSys = dfs.getFileSystem();
      mr = new MiniMRCluster(2, fileSys.getUri().toString(), 1);
      JobConf conf = mr.createJobConf();
      
      // intialize input, output directories
      Path inDir = new Path("testing/wc/input");
      Path outDir = new Path("testing/wc/output");
      String input = "The input";
      
      launchTest(conf, inDir, outDir, input);
      
    } catch(Exception e) {
      e.printStackTrace();
      fail("Exception in testing temp dir");
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
        LOG.info("IO exception in closing file system)" );
        ioe.printStackTrace();        			
      }
    }
  }

  public static void main(String args[]){
    TestMiniMRTaskTempDir test = new TestMiniMRTaskTempDir();
    test.testTaskTempDir();
  }
}
