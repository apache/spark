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
import java.util.*;
import junit.framework.TestCase;

import org.apache.commons.logging.*;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.conf.*;

public class TestSequenceFileInputFilter extends TestCase {
  private static final Log LOG = FileInputFormat.LOG;

  private static final int MAX_LENGTH = 15000;
  private static final Configuration conf = new Configuration();
  private static final JobConf job = new JobConf(conf);
  private static final FileSystem fs;
  private static final Path inDir = new Path(System.getProperty("test.build.data",".") + "/mapred");
  private static final Path inFile = new Path(inDir, "test.seq");
  private static final Random random = new Random(1);
  private static final Reporter reporter = Reporter.NULL;
  
  static {
    FileInputFormat.setInputPaths(job, inDir);
    try {
      fs = FileSystem.getLocal(conf);
    } catch (IOException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  private static void createSequenceFile(int numRecords) throws Exception {
    // create a file with length entries
    SequenceFile.Writer writer =
      SequenceFile.createWriter(fs, conf, inFile,
                                Text.class, BytesWritable.class);
    try {
      for (int i = 1; i <= numRecords; i++) {
        Text key = new Text(Integer.toString(i));
        byte[] data = new byte[random.nextInt(10)];
        random.nextBytes(data);
        BytesWritable value = new BytesWritable(data);
        writer.append(key, value);
      }
    } finally {
      writer.close();
    }
  }


  private int countRecords(int numSplits) throws IOException {
    InputFormat<Text, BytesWritable> format =
      new SequenceFileInputFilter<Text, BytesWritable>();
    Text key = new Text();
    BytesWritable value = new BytesWritable();
    if (numSplits==0) {
      numSplits =
        random.nextInt(MAX_LENGTH/(SequenceFile.SYNC_INTERVAL/20))+1;
    }
    InputSplit[] splits = format.getSplits(job, numSplits);
      
    // check each split
    int count = 0;
    LOG.info("Generated " + splits.length + " splits.");
    for (int j = 0; j < splits.length; j++) {
      RecordReader<Text, BytesWritable> reader =
        format.getRecordReader(splits[j], job, reporter);
      try {
        while (reader.next(key, value)) {
          LOG.info("Accept record "+key.toString());
          count++;
        }
      } finally {
        reader.close();
      }
    }
    return count;
  }
  
  public void testRegexFilter() throws Exception {
    // set the filter class
    LOG.info("Testing Regex Filter with patter: \\A10*");
    SequenceFileInputFilter.setFilterClass(job, 
                                           SequenceFileInputFilter.RegexFilter.class);
    SequenceFileInputFilter.RegexFilter.setPattern(job, "\\A10*");
    
    // clean input dir
    fs.delete(inDir, true);
  
    // for a variety of lengths
    for (int length = 1; length < MAX_LENGTH;
         length+= random.nextInt(MAX_LENGTH/10)+1) {
      LOG.info("******Number of records: "+length);
      createSequenceFile(length);
      int count = countRecords(0);
      assertEquals(count, length==0?0:(int)Math.log10(length)+1);
    }
    
    // clean up
    fs.delete(inDir, true);
  }

  public void testPercentFilter() throws Exception {
    LOG.info("Testing Percent Filter with frequency: 1000");
    // set the filter class
    SequenceFileInputFilter.setFilterClass(job, 
                                           SequenceFileInputFilter.PercentFilter.class);
    SequenceFileInputFilter.PercentFilter.setFrequency(job, 1000);
      
    // clean input dir
    fs.delete(inDir, true);
    
    // for a variety of lengths
    for (int length = 0; length < MAX_LENGTH;
         length+= random.nextInt(MAX_LENGTH/10)+1) {
      LOG.info("******Number of records: "+length);
      createSequenceFile(length);
      int count = countRecords(1);
      LOG.info("Accepted "+count+" records");
      int expectedCount = length/1000;
      if (expectedCount*1000!=length)
        expectedCount++;
      assertEquals(count, expectedCount);
    }
      
    // clean up
    fs.delete(inDir, true);
  }
  
  public void testMD5Filter() throws Exception {
    // set the filter class
    LOG.info("Testing MD5 Filter with frequency: 1000");
    SequenceFileInputFilter.setFilterClass(job, 
                                           SequenceFileInputFilter.MD5Filter.class);
    SequenceFileInputFilter.MD5Filter.setFrequency(job, 1000);
      
    // clean input dir
    fs.delete(inDir, true);
    
    // for a variety of lengths
    for (int length = 0; length < MAX_LENGTH;
         length+= random.nextInt(MAX_LENGTH/10)+1) {
      LOG.info("******Number of records: "+length);
      createSequenceFile(length);
      LOG.info("Accepted "+countRecords(0)+" records");
    }
    // clean up
    fs.delete(inDir, true);
  }

  public static void main(String[] args) throws Exception {
    TestSequenceFileInputFilter filter = new TestSequenceFileInputFilter();
    filter.testRegexFilter();
  }
}
