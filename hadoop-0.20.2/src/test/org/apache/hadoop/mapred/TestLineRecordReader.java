/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.mapred;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.tools.ant.util.FileUtils;
import org.junit.Test;

public class TestLineRecordReader extends TestCase {

  private static Path workDir = new Path(new Path(System.getProperty(
      "test.build.data", "."), "data"), "TestTextInputFormat");
  private static Path inputDir = new Path(workDir, "input");
  private static Path outputDir = new Path(workDir, "output");

  /**
   * Writes the input test file
   * 
   * @param conf
   * @throws IOException
   */
  public void createInputFile(Configuration conf) throws IOException {
    FileSystem localFs = FileSystem.getLocal(conf);
    Path file = new Path(inputDir, "test.txt");
    Writer writer = new OutputStreamWriter(localFs.create(file));
    writer.write("abc\ndef\t\nghi\njkl");
    writer.close();
  }

  /**
   * Reads the output file into a string
   * 
   * @param conf
   * @return
   * @throws IOException
   */
  public String readOutputFile(Configuration conf) throws IOException {
    FileSystem localFs = FileSystem.getLocal(conf);
    Path file = new Path(outputDir, "part-00000");
    Reader reader = new InputStreamReader(localFs.open(file));
    String r = FileUtils.readFully(reader);
    reader.close();
    return r;
  }

  /**
   * Creates and runs an MR job
   * 
   * @param conf
   * @throws IOException
   * @throws InterruptedException
   * @throws ClassNotFoundException
   */
  public void createAndRunJob(Configuration conf) throws IOException,
      InterruptedException, ClassNotFoundException {
    JobConf job = new JobConf(conf);
    job.setJarByClass(TestLineRecordReader.class);
    job.setMapperClass(IdentityMapper.class);
    job.setReducerClass(IdentityReducer.class);
    FileInputFormat.addInputPath(job, inputDir);
    FileOutputFormat.setOutputPath(job, outputDir);
    JobClient.runJob(job);
  }

  /**
   * Test the case when a custom record delimiter is specified using the
   * textinputformat.record.delimiter configuration property
   * 
   * @throws IOException
   * @throws InterruptedException
   * @throws ClassNotFoundException
   */
  @Test
  public void testCustomRecordDelimiters() throws IOException,
      InterruptedException, ClassNotFoundException {
    Configuration conf = new Configuration();
    conf.set("textinputformat.record.delimiter", "\t\n");
    FileSystem localFs = FileSystem.getLocal(conf);
    // cleanup
    localFs.delete(workDir, true);
    // creating input test file
    createInputFile(conf);
    createAndRunJob(conf);
    String expected = "0\tabc\ndef\n9\tghi\njkl\n";
    this.assertEquals(expected, readOutputFile(conf));
  }

  /**
   * Test the default behavior when the textinputformat.record.delimiter
   * configuration property is not specified
   * 
   * @throws IOException
   * @throws InterruptedException
   * @throws ClassNotFoundException
   */
  @Test
  public void testDefaultRecordDelimiters() throws IOException,
      InterruptedException, ClassNotFoundException {
    Configuration conf = new Configuration();
    FileSystem localFs = FileSystem.getLocal(conf);
    // cleanup
    localFs.delete(workDir, true);
    // creating input test file
    createInputFile(conf);
    createAndRunJob(conf);
    String expected = "0\tabc\n4\tdef\t\n9\tghi\n13\tjkl\n";
    this.assertEquals(expected, readOutputFile(conf));
  }

}