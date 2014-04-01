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

package org.apache.hadoop.io;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.SequenceFile.Sorter.RawKeyValueIterator;
import org.apache.hadoop.io.SequenceFile.Sorter.SegmentDescriptor;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapred.*;

import junit.framework.TestCase;
import org.apache.commons.logging.*;

public class TestSequenceFileMergeProgress extends TestCase {
  private static final Log LOG = FileInputFormat.LOG;
  private static final int RECORDS = 10000;
  
  public void testMergeProgressWithNoCompression() throws IOException {
    runTest(SequenceFile.CompressionType.NONE);
  }

  public void testMergeProgressWithRecordCompression() throws IOException {
    runTest(SequenceFile.CompressionType.RECORD);
  }

  public void testMergeProgressWithBlockCompression() throws IOException {
    runTest(SequenceFile.CompressionType.BLOCK);
  }

  public void runTest(CompressionType compressionType) throws IOException {
    JobConf job = new JobConf();
    FileSystem fs = FileSystem.getLocal(job);
    Path dir = new Path(System.getProperty("test.build.data",".") + "/mapred");
    Path file = new Path(dir, "test.seq");
    Path tempDir = new Path(dir, "tmp");

    fs.delete(dir, true);
    FileInputFormat.setInputPaths(job, dir);
    fs.mkdirs(tempDir);

    LongWritable tkey = new LongWritable();
    Text tval = new Text();

    SequenceFile.Writer writer =
      SequenceFile.createWriter(fs, job, file, LongWritable.class, Text.class,
        compressionType, new DefaultCodec());
    try {
      for (int i = 0; i < RECORDS; ++i) {
        tkey.set(1234);
        tval.set("valuevaluevaluevaluevaluevaluevaluevaluevaluevaluevalue");
        writer.append(tkey, tval);
      }
    } finally {
      writer.close();
    }
    
    long fileLength = fs.getFileStatus(file).getLen();
    LOG.info("With compression = " + compressionType + ": "
        + "compressed length = " + fileLength);
    
    SequenceFile.Sorter sorter = new SequenceFile.Sorter(fs, 
        job.getOutputKeyComparator(), job.getMapOutputKeyClass(),
        job.getMapOutputValueClass(), job);
    Path[] paths = new Path[] {file};
    RawKeyValueIterator rIter = sorter.merge(paths, tempDir, false);
    int count = 0;
    while (rIter.next()) {
      count++;
    }
    assertEquals(RECORDS, count);
    assertEquals(1.0f, rIter.getProgress().get());
  }

}
