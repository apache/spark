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
import org.apache.hadoop.io.compress.*;
import org.apache.hadoop.util.LineReader;
import org.apache.hadoop.util.ReflectionUtils;

public class TestKeyValueTextInputFormat extends TestCase {
  private static final Log LOG =
    LogFactory.getLog(TestKeyValueTextInputFormat.class.getName());

  private static int MAX_LENGTH = 10000;
  
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
             "TestKeyValueTextInputFormat");
  
  public void testFormat() throws Exception {
    JobConf job = new JobConf();
    Path file = new Path(workDir, "test.txt");

    // A reporter that does nothing
    Reporter reporter = Reporter.NULL;
    
    int seed = new Random().nextInt();
    LOG.info("seed = "+seed);
    Random random = new Random(seed);

    localFs.delete(workDir, true);
    FileInputFormat.setInputPaths(job, workDir);

    // for a variety of lengths
    for (int length = 0; length < MAX_LENGTH;
         length+= random.nextInt(MAX_LENGTH/10)+1) {

      LOG.debug("creating; entries = " + length);

      // create a file with length entries
      Writer writer = new OutputStreamWriter(localFs.create(file));
      try {
        for (int i = 0; i < length; i++) {
          writer.write(Integer.toString(i*2));
          writer.write("\t");
          writer.write(Integer.toString(i));
          writer.write("\n");
        }
      } finally {
        writer.close();
      }

      // try splitting the file in a variety of sizes
      KeyValueTextInputFormat format = new KeyValueTextInputFormat();
      format.configure(job);
      for (int i = 0; i < 3; i++) {
        int numSplits = random.nextInt(MAX_LENGTH/20)+1;
        LOG.debug("splitting: requesting = " + numSplits);
        InputSplit[] splits = format.getSplits(job, numSplits);
        LOG.debug("splitting: got =        " + splits.length);

        // check each split
        BitSet bits = new BitSet(length);
        for (int j = 0; j < splits.length; j++) {
          LOG.debug("split["+j+"]= " + splits[j]);
          RecordReader<Text, Text> reader =
            format.getRecordReader(splits[j], job, reporter);
          Class readerClass = reader.getClass();
          assertEquals("reader class is KeyValueLineRecordReader.", KeyValueLineRecordReader.class, readerClass);        

          Text key = reader.createKey();
          Class keyClass = key.getClass();
          Text value = reader.createValue();
          Class valueClass = value.getClass();
          assertEquals("Key class is Text.", Text.class, keyClass);
          assertEquals("Value class is Text.", Text.class, valueClass);
          try {
            int count = 0;
            while (reader.next(key, value)) {
              int v = Integer.parseInt(value.toString());
              LOG.debug("read " + v);
              if (bits.get(v)) {
                LOG.warn("conflict with " + v + 
                         " in split " + j +
                         " at position "+reader.getPos());
              }
              assertFalse("Key in multiple partitions.", bits.get(v));
              bits.set(v);
              count++;
            }
            LOG.debug("splits["+j+"]="+splits[j]+" count=" + count);
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
  
  private static final Reporter voidReporter = Reporter.NULL;
  
  private static List<Text> readSplit(KeyValueTextInputFormat format, 
                                      InputSplit split, 
                                      JobConf job) throws IOException {
    List<Text> result = new ArrayList<Text>();
    RecordReader<Text, Text> reader = format.getRecordReader(split, job,
                                                 voidReporter);
    Text key = reader.createKey();
    Text value = reader.createValue();
    while (reader.next(key, value)) {
      result.add(value);
      value = (Text) reader.createValue();
    }
    return result;
  }
  
  /**
   * Test using the gzip codec for reading
   */
  public static void testGzip() throws IOException {
    JobConf job = new JobConf();
    CompressionCodec gzip = new GzipCodec();
    ReflectionUtils.setConf(gzip, job);
    localFs.delete(workDir, true);
    writeFile(localFs, new Path(workDir, "part1.txt.gz"), gzip, 
              "line-1\tthe quick\nline-2\tbrown\nline-3\tfox jumped\nline-4\tover\nline-5\t the lazy\nline-6\t dog\n");
    writeFile(localFs, new Path(workDir, "part2.txt.gz"), gzip,
              "line-1\tthis is a test\nline-1\tof gzip\n");
    FileInputFormat.setInputPaths(job, workDir);
    KeyValueTextInputFormat format = new KeyValueTextInputFormat();
    format.configure(job);
    InputSplit[] splits = format.getSplits(job, 100);
    assertEquals("compressed splits == 2", 2, splits.length);
    FileSplit tmp = (FileSplit) splits[0];
    if (tmp.getPath().getName().equals("part2.txt.gz")) {
      splits[0] = splits[1];
      splits[1] = tmp;
    }
    List<Text> results = readSplit(format, splits[0], job);
    assertEquals("splits[0] length", 6, results.size());
    assertEquals("splits[0][5]", " dog", results.get(5).toString());
    results = readSplit(format, splits[1], job);
    assertEquals("splits[1] length", 2, results.size());
    assertEquals("splits[1][0]", "this is a test", 
                 results.get(0).toString());    
    assertEquals("splits[1][1]", "of gzip", 
                 results.get(1).toString());    
  }
  
  public static void main(String[] args) throws Exception {
    new TestKeyValueTextInputFormat().testFormat();
  }
}
