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

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.streaming.AutoInputFormat;

import junit.framework.TestCase;

public class TestAutoInputFormat extends TestCase {

  private static Configuration conf = new Configuration();

  private static final int LINES_COUNT = 3;

  private static final int RECORDS_COUNT = 3;

  private static final int SPLITS_COUNT = 2;

  @SuppressWarnings( { "unchecked", "deprecation" })
  public void testFormat() throws IOException {
    JobConf job = new JobConf(conf);
    FileSystem fs = FileSystem.getLocal(conf);
    Path dir = new Path(System.getProperty("test.build.data", ".") + "/mapred");
    Path txtFile = new Path(dir, "auto.txt");
    Path seqFile = new Path(dir, "auto.seq");

    fs.delete(dir, true);

    FileInputFormat.setInputPaths(job, dir);

    Writer txtWriter = new OutputStreamWriter(fs.create(txtFile));
    try {
      for (int i = 0; i < LINES_COUNT; i++) {
        txtWriter.write("" + (10 * i));
        txtWriter.write("\n");
      }
    } finally {
      txtWriter.close();
    }

    SequenceFile.Writer seqWriter = SequenceFile.createWriter(fs, conf,
      seqFile, IntWritable.class, LongWritable.class);
    try {
      for (int i = 0; i < RECORDS_COUNT; i++) {
        IntWritable key = new IntWritable(11 * i);
        LongWritable value = new LongWritable(12 * i);
        seqWriter.append(key, value);
      }
    } finally {
      seqWriter.close();
    }

    AutoInputFormat format = new AutoInputFormat();
    InputSplit[] splits = format.getSplits(job, SPLITS_COUNT);
    for (InputSplit split : splits) {
      RecordReader reader = format.getRecordReader(split, job, Reporter.NULL);
      Object key = reader.createKey();
      Object value = reader.createValue();
      try {
        while (reader.next(key, value)) {
          if (key instanceof LongWritable) {
            assertEquals("Wrong value class.", Text.class, value.getClass());
            assertTrue("Invalid value", Integer.parseInt(((Text) value)
              .toString()) % 10 == 0);
          } else {
            assertEquals("Wrong key class.", IntWritable.class, key.getClass());
            assertEquals("Wrong value class.", LongWritable.class, value
              .getClass());
            assertTrue("Invalid key.", ((IntWritable) key).get() % 11 == 0);
            assertTrue("Invalid value.", ((LongWritable) value).get() % 12 == 0);
          }
        }
      } finally {
        reader.close();
      }
    }
  }

}
