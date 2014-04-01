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

import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;

import org.apache.hadoop.mapred.lib.*;

public class TestMultipleTextOutputFormat extends TestCase {
  private static JobConf defaultConf = new JobConf();

  private static FileSystem localFs = null;
  static {
    try {
      localFs = FileSystem.getLocal(defaultConf);
    } catch (IOException e) {
      throw new RuntimeException("init failure", e);
    }
  }

  // A random task attempt id for testing.
  private static String attempt = "attempt_200707121733_0001_m_000000_0";

  private static Path workDir = 
    new Path(new Path(
                      new Path(System.getProperty("test.build.data", "."), 
                               "data"), 
                      FileOutputCommitter.TEMP_DIR_NAME), "_" + attempt);

  private static void writeData(RecordWriter<Text, Text> rw) throws IOException {
    for (int i = 10; i < 40; i++) {
      String k = "" + i;
      String v = "" + i;
      rw.write(new Text(k), new Text(v));
    }
  }
  
  static class KeyBasedMultipleTextOutputFormat extends MultipleTextOutputFormat<Text, Text> {
    protected String generateFileNameForKeyValue(Text key, Text v, String name) {
      
      return key.toString().substring(0, 1) + "-" + name;
    }
  }
  
  private static void test1(JobConf job) throws IOException {
    FileSystem fs = FileSystem.getLocal(job);
    String name = "part-00000";
    KeyBasedMultipleTextOutputFormat theOutputFormat = new KeyBasedMultipleTextOutputFormat();
    RecordWriter<Text, Text> rw = theOutputFormat.getRecordWriter(fs, job, name, null);
    writeData(rw);
    rw.close(null);
  }
  
  private static void test2(JobConf job) throws IOException {
    FileSystem fs = FileSystem.getLocal(job);
    String name = "part-00000";
    //pretend that we have input file with 1/2/3 as the suffix
    job.set("map.input.file", "1/2/3");
    // we use the last two legs of the input file as the output file
    job.set("mapred.outputformat.numOfTrailingLegs", "2");
    MultipleTextOutputFormat<Text, Text> theOutputFormat = new MultipleTextOutputFormat<Text, Text>();
    RecordWriter<Text, Text> rw = theOutputFormat.getRecordWriter(fs, job, name, null);
    writeData(rw);
    rw.close(null);
  }
  
  public void testFormat() throws Exception {
    JobConf job = new JobConf();
    job.set("mapred.task.id", attempt);
    FileOutputFormat.setOutputPath(job, workDir.getParent().getParent());
    FileOutputFormat.setWorkOutputPath(job, workDir);
    FileSystem fs = workDir.getFileSystem(job);
    if (!fs.mkdirs(workDir)) {
      fail("Failed to create output directory");
    }
    //System.out.printf("workdir: %s\n", workDir.toString());
    TestMultipleTextOutputFormat.test1(job);
    TestMultipleTextOutputFormat.test2(job);
    String file_11 = "1-part-00000";
    
    File expectedFile_11 = new File(new Path(workDir, file_11).toString()); 

    //System.out.printf("expectedFile_11: %s\n", new Path(workDir, file_11).toString());
    StringBuffer expectedOutput = new StringBuffer();
    for (int i = 10; i < 20; i++) {
      expectedOutput.append(""+i).append('\t').append(""+i).append("\n");
    }
    String output = UtilsForTests.slurp(expectedFile_11);
    //System.out.printf("File_2 output: %s\n", output);
    assertEquals(output, expectedOutput.toString());
    
    String file_12 = "2-part-00000";
    
    File expectedFile_12 = new File(new Path(workDir, file_12).toString()); 
    //System.out.printf("expectedFile_12: %s\n", new Path(workDir, file_12).toString());
    expectedOutput = new StringBuffer();
    for (int i = 20; i < 30; i++) {
      expectedOutput.append(""+i).append('\t').append(""+i).append("\n");
    }
    output = UtilsForTests.slurp(expectedFile_12);
    //System.out.printf("File_2 output: %s\n", output);
    assertEquals(output, expectedOutput.toString());
    
    String file_13 = "3-part-00000";
    
    File expectedFile_13 = new File(new Path(workDir, file_13).toString()); 
    //System.out.printf("expectedFile_13: %s\n", new Path(workDir, file_13).toString());
    expectedOutput = new StringBuffer();
    for (int i = 30; i < 40; i++) {
      expectedOutput.append(""+i).append('\t').append(""+i).append("\n");
    }
    output = UtilsForTests.slurp(expectedFile_13);
    //System.out.printf("File_2 output: %s\n", output);
    assertEquals(output, expectedOutput.toString());
    
    String file_2 = "2/3";
    
    File expectedFile_2 = new File(new Path(workDir, file_2).toString()); 
    //System.out.printf("expectedFile_2: %s\n", new Path(workDir, file_2).toString());
    expectedOutput = new StringBuffer();
    for (int i = 10; i < 40; i++) {
      expectedOutput.append(""+i).append('\t').append(""+i).append("\n");
    }
    output = UtilsForTests.slurp(expectedFile_2);
    //System.out.printf("File_2 output: %s\n", output);
    assertEquals(output, expectedOutput.toString());
  }

  public static void main(String[] args) throws Exception {
    new TestMultipleTextOutputFormat().testFormat();
  }
}
