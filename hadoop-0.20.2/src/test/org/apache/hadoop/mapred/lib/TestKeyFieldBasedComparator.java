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

package org.apache.hadoop.mapred.lib;

import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.HadoopTestCase;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.Utils;


public class TestKeyFieldBasedComparator extends HadoopTestCase {
  JobConf conf;
  JobConf localConf;
  
  String line1 = "123 -123 005120 123.9 0.01 0.18 010 10.0 4444.1 011 011 234";
  String line2 = "134 -12 005100 123.10 -1.01 0.19 02 10.1 4444";

  public TestKeyFieldBasedComparator() throws IOException {
    super(HadoopTestCase.LOCAL_MR, HadoopTestCase.LOCAL_FS, 1, 1);
    conf = createJobConf();
    localConf = createJobConf();
    localConf.set("map.output.key.field.separator", " ");
  }
  public void configure(String keySpec, int expect) throws Exception {
    Path testdir = new Path("build/test/test.mapred.spill");
    Path inDir = new Path(testdir, "in");
    Path outDir = new Path(testdir, "out");
    FileSystem fs = getFileSystem();
    fs.delete(testdir, true);
    conf.setInputFormat(TextInputFormat.class);
    FileInputFormat.setInputPaths(conf, inDir);
    FileOutputFormat.setOutputPath(conf, outDir);
    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(LongWritable.class);

    conf.setNumMapTasks(1);
    conf.setNumReduceTasks(2);

    conf.setOutputFormat(TextOutputFormat.class);
    conf.setOutputKeyComparatorClass(KeyFieldBasedComparator.class);
    conf.setKeyFieldComparatorOptions(keySpec);
    conf.setKeyFieldPartitionerOptions("-k1.1,1.1");
    conf.set("map.output.key.field.separator", " ");
    conf.setMapperClass(InverseMapper.class);
    conf.setReducerClass(IdentityReducer.class);
    if (!fs.mkdirs(testdir)) {
      throw new IOException("Mkdirs failed to create " + testdir.toString());
    }
    if (!fs.mkdirs(inDir)) {
      throw new IOException("Mkdirs failed to create " + inDir.toString());
    }
    // set up input data in 2 files 
    Path inFile = new Path(inDir, "part0");
    FileOutputStream fos = new FileOutputStream(inFile.toString());
    fos.write((line1 + "\n").getBytes());
    fos.write((line2 + "\n").getBytes());
    fos.close();
    JobClient jc = new JobClient(conf);
    RunningJob r_job = jc.submitJob(conf);
    while (!r_job.isComplete()) {
      Thread.sleep(1000);
    }
    
    if (!r_job.isSuccessful()) {
      fail("Oops! The job broke due to an unexpected error");
    }
    Path[] outputFiles = FileUtil.stat2Paths(
        getFileSystem().listStatus(outDir,
        new Utils.OutputFileUtils.OutputFilesFilter()));
    if (outputFiles.length > 0) {
      InputStream is = getFileSystem().open(outputFiles[0]);
      BufferedReader reader = new BufferedReader(new InputStreamReader(is));
      String line = reader.readLine();
      //make sure we get what we expect as the first line, and also
      //that we have two lines (both the lines must end up in the same
      //reducer since the partitioner takes the same key spec for all
      //lines
      if (expect == 1) {
        assertTrue(line.startsWith(line1));
      } else if (expect == 2) {
        assertTrue(line.startsWith(line2));
      }
      line = reader.readLine();
      if (expect == 1) {
        assertTrue(line.startsWith(line2));
      } else if (expect == 2) {
        assertTrue(line.startsWith(line1));
      }
      reader.close();
    }
  }
  public void testBasicUnixComparator() throws Exception {
    configure("-k1,1n", 1);
    configure("-k2,2n", 1);
    configure("-k2.2,2n", 2);
    configure("-k3.4,3n", 2);
    configure("-k3.2,3.3n -k4,4n", 2);
    configure("-k3.2,3.3n -k4,4nr", 1);
    configure("-k2.4,2.4n", 2);
    configure("-k7,7", 1);
    configure("-k7,7n", 2);
    configure("-k8,8n", 1);
    configure("-k9,9", 2);
    configure("-k11,11",2);
    configure("-k10,10",2);
    
    localTestWithoutMRJob("-k9,9", 1);

    localTestWithoutMRJob("-k9n", 1);
  }
  
  byte[] line1_bytes = line1.getBytes();
  byte[] line2_bytes = line2.getBytes();

  public void localTestWithoutMRJob(String keySpec, int expect) throws Exception {
    KeyFieldBasedComparator<Void, Void> keyFieldCmp = new KeyFieldBasedComparator<Void, Void>();
    localConf.setKeyFieldComparatorOptions(keySpec);
    keyFieldCmp.configure(localConf);
    int result = keyFieldCmp.compare(line1_bytes, 0, line1_bytes.length,
        line2_bytes, 0, line2_bytes.length);
    if ((expect >= 0 && result < 0) || (expect < 0 && result >= 0))
      fail();
  }
}
