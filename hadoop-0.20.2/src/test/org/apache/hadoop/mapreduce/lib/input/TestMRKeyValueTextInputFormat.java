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

package org.apache.hadoop.mapreduce.lib.input;

import java.io.*;
import java.util.*;
import junit.framework.TestCase;

import org.apache.commons.logging.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.compress.*;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MapContext;
import org.apache.hadoop.mapreduce.MapReduceTestUtil;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.LineReader;
import org.apache.hadoop.util.ReflectionUtils;

public class TestMRKeyValueTextInputFormat extends TestCase {
  private static final Log LOG =
    LogFactory.getLog(TestMRKeyValueTextInputFormat.class.getName());

  private static int MAX_LENGTH = 10000;
  
  private static Configuration defaultConf = new Configuration();
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
             "TestKeyValueTextInputFormat");
  
  public void testFormat() throws Exception {
    Job job = new Job(new Configuration());
    Path file = new Path(workDir, "test.txt");

    int seed = new Random().nextInt();
    LOG.info("seed = " + seed);
    Random random = new Random(seed);

    localFs.delete(workDir, true);
    FileInputFormat.setInputPaths(job, workDir);

    // for a variety of lengths
    for (int length = 0; length < MAX_LENGTH;
         length += random.nextInt(MAX_LENGTH / 10) + 1) {

      LOG.debug("creating; entries = " + length);

      // create a file with length entries
      Writer writer = new OutputStreamWriter(localFs.create(file));
      try {
        for (int i = 0; i < length; i++) {
          writer.write(Integer.toString(i * 2));
          writer.write("\t");
          writer.write(Integer.toString(i));
          writer.write("\n");
        }
      } finally {
        writer.close();
      }

      // try splitting the file in a variety of sizes
      KeyValueTextInputFormat format = new KeyValueTextInputFormat();
      for (int i = 0; i < 3; i++) {
        int numSplits = random.nextInt(MAX_LENGTH / 20) + 1;
        LOG.debug("splitting: requesting = " + numSplits);
        List<InputSplit> splits = format.getSplits(job);
        LOG.debug("splitting: got =        " + splits.size());

        // check each split
        BitSet bits = new BitSet(length);
        for (int j = 0; j < splits.size(); j++) {
          LOG.debug("split["+j+"]= " + splits.get(j));
          TaskAttemptContext context = MapReduceTestUtil.
            createDummyMapTaskAttemptContext(job.getConfiguration());
          RecordReader<Text, Text> reader = format.createRecordReader(
            splits.get(j), context);
          Class<?> clazz = reader.getClass();
          assertEquals("reader class is KeyValueLineRecordReader.", 
            KeyValueLineRecordReader.class, clazz);
          MapContext<Text, Text, Text, Text> mcontext = 
            new MapContext<Text, Text, Text, Text>(job.getConfiguration(), 
            context.getTaskAttemptID(), reader, null, null, 
            MapReduceTestUtil.createDummyReporter(), splits.get(j));
          reader.initialize(splits.get(j), mcontext);

          Text key = null;
          Text value = null;
          try {
            int count = 0;
            while (reader.nextKeyValue()) {
              key = reader.getCurrentKey();
              clazz = key.getClass();
              assertEquals("Key class is Text.", Text.class, clazz);
              value = reader.getCurrentValue();
              clazz = value.getClass();
              assertEquals("Value class is Text.", Text.class, clazz);
              int v = Integer.parseInt(value.toString());
              LOG.debug("read " + v);
              assertFalse("Key in multiple partitions.", bits.get(v));
              bits.set(v);
              count++;
            }
            LOG.debug("splits[" + j + "]=" + splits.get(j) +" count=" + count);
          } finally {
            reader.close();
          }
        }
        assertEquals("Some keys in no partition.", length, bits.cardinality());
      }

    }
  }
  private LineReader makeStream(String str) throws IOException {
    return new LineReader(new ByteArrayInputStream
                                           (str.getBytes("UTF-8")), 
                                           defaultConf);
  }
  
  public void testUTF8() throws Exception {
    LineReader in = makeStream("abcd\u20acbdcd\u20ac");
    Text line = new Text();
    in.readLine(line);
    assertEquals("readLine changed utf8 characters", 
                 "abcd\u20acbdcd\u20ac", line.toString());
    in = makeStream("abc\u200axyz");
    in.readLine(line);
    assertEquals("split on fake newline", "abc\u200axyz", line.toString());
  }

  public void testNewLines() throws Exception {
    LineReader in = makeStream("a\nbb\n\nccc\rdddd\r\neeeee");
    Text out = new Text();
    in.readLine(out);
    assertEquals("line1 length", 1, out.getLength());
    in.readLine(out);
    assertEquals("line2 length", 2, out.getLength());
    in.readLine(out);
    assertEquals("line3 length", 0, out.getLength());
    in.readLine(out);
    assertEquals("line4 length", 3, out.getLength());
    in.readLine(out);
    assertEquals("line5 length", 4, out.getLength());
    in.readLine(out);
    assertEquals("line5 length", 5, out.getLength());
    assertEquals("end of file", 0, in.readLine(out));
  }
  
  private static void writeFile(FileSystem fs, Path name, 
                                CompressionCodec codec,
                                String contents) throws IOException {
    OutputStream stm;
    if (codec == null) {
      stm = fs.create(name);
    } else {
      stm = codec.createOutputStream(fs.create(name));
    }
    stm.write(contents.getBytes());
    stm.close();
  }
  
  private static List<Text> readSplit(KeyValueTextInputFormat format, 
      InputSplit split, Job job) throws IOException, InterruptedException {
    List<Text> result = new ArrayList<Text>();
    Configuration conf = job.getConfiguration();
    TaskAttemptContext context = MapReduceTestUtil.
      createDummyMapTaskAttemptContext(conf);
    RecordReader<Text, Text> reader = format.createRecordReader(split, 
      MapReduceTestUtil.createDummyMapTaskAttemptContext(conf));
    MapContext<Text, Text, Text, Text> mcontext = 
      new MapContext<Text, Text, Text, Text>(conf, 
      context.getTaskAttemptID(), reader, null, null,
      MapReduceTestUtil.createDummyReporter(), 
      split);
    reader.initialize(split, mcontext);
    while (reader.nextKeyValue()) {
      result.add(new Text(reader.getCurrentValue()));
    }
    return result;
  }
  
  /**
   * Test using the gzip codec for reading
   */
  public static void testGzip() throws IOException, InterruptedException {
    Configuration conf = new Configuration();
    CompressionCodec gzip = new GzipCodec();
    ReflectionUtils.setConf(gzip, conf);
    localFs.delete(workDir, true);
    writeFile(localFs, new Path(workDir, "part1.txt.gz"), gzip, 
              "line-1\tthe quick\nline-2\tbrown\nline-3\t" +
              "fox jumped\nline-4\tover\nline-5\t the lazy\nline-6\t dog\n");
    writeFile(localFs, new Path(workDir, "part2.txt.gz"), gzip,
              "line-1\tthis is a test\nline-1\tof gzip\n");
    Job job = new Job(conf);
    FileInputFormat.setInputPaths(job, workDir);
    KeyValueTextInputFormat format = new KeyValueTextInputFormat();
    List<InputSplit> splits = format.getSplits(job);
    assertEquals("compressed splits == 2", 2, splits.size());
    FileSplit tmp = (FileSplit) splits.get(0);
    if (tmp.getPath().getName().equals("part2.txt.gz")) {
      splits.set(0, splits.get(1));
      splits.set(1, tmp);
    }
    List<Text> results = readSplit(format, splits.get(0), job);
    assertEquals("splits[0] length", 6, results.size());
    assertEquals("splits[0][0]", "the quick", results.get(0).toString());
    assertEquals("splits[0][1]", "brown", results.get(1).toString());
    assertEquals("splits[0][2]", "fox jumped", results.get(2).toString());
    assertEquals("splits[0][3]", "over", results.get(3).toString());
    assertEquals("splits[0][4]", " the lazy", results.get(4).toString());
    assertEquals("splits[0][5]", " dog", results.get(5).toString());
    results = readSplit(format, splits.get(1), job);
    assertEquals("splits[1] length", 2, results.size());
    assertEquals("splits[1][0]", "this is a test", 
                 results.get(0).toString());    
    assertEquals("splits[1][1]", "of gzip", 
                 results.get(1).toString());    
  }
  
  public static void main(String[] args) throws Exception {
    new TestMRKeyValueTextInputFormat().testFormat();
  }
}
