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

import java.io.*;
import java.util.*;
import junit.framework.TestCase;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class TestLineInputFormat extends TestCase {
  private static int MAX_LENGTH = 200;
  
  private static JobConf defaultConf = new JobConf();
  private static FileSystem localFs = null; 

  static {
    try {
      localFs = FileSystem.getLocal(defaultConf);
    } catch (IOException e) {
      throw new RuntimeException("init failure", e);
    }
  }

  private static Path workDir = 
    new Path(new Path(System.getProperty("test.build.data", "."), "data"),
             "TestLineInputFormat");
  
  public void testFormat() throws Exception {
    JobConf job = new JobConf();
    Path file = new Path(workDir, "test.txt");

    int seed = new Random().nextInt();
    Random random = new Random(seed);

    localFs.delete(workDir, true);
    FileInputFormat.setInputPaths(job, workDir);
    int numLinesPerMap = 5;
    job.setInt("mapred.line.input.format.linespermap", numLinesPerMap);

    // for a variety of lengths
    for (int length = 0; length < MAX_LENGTH;
         length += random.nextInt(MAX_LENGTH/10) + 1) {
      // create a file with length entries
      Writer writer = new OutputStreamWriter(localFs.create(file));
      try {
        for (int i = 0; i < length; i++) {
          writer.write(Integer.toString(i));
          writer.write("\n");
        }
      } finally {
        writer.close();
      }
      checkFormat(job, numLinesPerMap);
    }
  }

  // A reporter that does nothing
  private static final Reporter voidReporter = Reporter.NULL;
  
  void checkFormat(JobConf job, int expectedN) throws IOException{
    NLineInputFormat format = new NLineInputFormat();
    format.configure(job);
    int ignoredNumSplits = 1;
    InputSplit[] splits = format.getSplits(job, ignoredNumSplits);

    // check all splits except last one
    int count = 0;
    for (int j = 0; j < splits.length -1; j++) {
      assertEquals("There are no split locations", 0,
                   splits[j].getLocations().length);
      RecordReader<LongWritable, Text> reader =
        format.getRecordReader(splits[j], job, voidReporter);
      Class readerClass = reader.getClass();
      assertEquals("reader class is LineRecordReader.",
                   LineRecordReader.class, readerClass);        
      LongWritable key = reader.createKey();
      Class keyClass = key.getClass();
      assertEquals("Key class is LongWritable.", LongWritable.class, keyClass);
      Text value = reader.createValue();
      Class valueClass = value.getClass();
      assertEquals("Value class is Text.", Text.class, valueClass);
         
      try {
        count = 0;
        while (reader.next(key, value)) {
          count++;
        }
      } finally {
        reader.close();
      }
      assertEquals("number of lines in split is " + expectedN ,
                   expectedN, count);
    }
  }
  
  public static void main(String[] args) throws Exception {
    new TestLineInputFormat().testFormat();
  }
}
