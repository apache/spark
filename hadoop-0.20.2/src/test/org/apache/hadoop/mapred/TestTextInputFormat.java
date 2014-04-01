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

public class TestTextInputFormat extends TestCase {
  private static final Log LOG =
    LogFactory.getLog(TestTextInputFormat.class.getName());

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
             "TestTextInputFormat");
  
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
          writer.write(Integer.toString(i));
          writer.write("\n");
        }
      } finally {
        writer.close();
      }

      // try splitting the file in a variety of sizes
      TextInputFormat format = new TextInputFormat();
      format.configure(job);
      LongWritable key = new LongWritable();
      Text value = new Text();
      for (int i = 0; i < 3; i++) {
        int numSplits = random.nextInt(MAX_LENGTH/20)+1;
        LOG.debug("splitting: requesting = " + numSplits);
        InputSplit[] splits = format.getSplits(job, numSplits);
        LOG.debug("splitting: got =        " + splits.length);

        if (length == 0) {
           assertEquals("Files of length 0 are not returned from FileInputFormat.getSplits().", 
                        1, splits.length);
           assertEquals("Empty file length == 0", 0, splits[0].getLength());
        }

        // check each split
        BitSet bits = new BitSet(length);
        for (int j = 0; j < splits.length; j++) {
          LOG.debug("split["+j+"]= " + splits[j]);
          RecordReader<LongWritable, Text> reader =
            format.getRecordReader(splits[j], job, reporter);
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

  private static LineReader makeStream(String str) throws IOException {
    return new LineReader(new ByteArrayInputStream
                                             (str.getBytes("UTF-8")), 
                                           defaultConf);
  }
  private static LineReader makeStream(String str, int bufsz) throws IOException {
    return new LineReader(new ByteArrayInputStream
                                             (str.getBytes("UTF-8")), 
                                           bufsz);
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

  /**
   * Test readLine for various kinds of line termination sequneces.
   * Varies buffer size to stress test.  Also check that returned
   * value matches the string length.
   *
   * @throws Exception
   */
  public void testNewLines() throws Exception {
    final String STR = "a\nbb\n\nccc\rdddd\r\r\r\n\r\neeeee";
    final int STRLENBYTES = STR.getBytes().length;
    Text out = new Text();
    for (int bufsz = 1; bufsz < STRLENBYTES+1; ++bufsz) {
      LineReader in = makeStream(STR, bufsz);
      int c = 0;
      c += in.readLine(out); //"a"\n
      assertEquals("line1 length, bufsz:"+bufsz, 1, out.getLength());
      c += in.readLine(out); //"bb"\n
      assertEquals("line2 length, bufsz:"+bufsz, 2, out.getLength());
      c += in.readLine(out); //""\n
      assertEquals("line3 length, bufsz:"+bufsz, 0, out.getLength());
      c += in.readLine(out); //"ccc"\r
      assertEquals("line4 length, bufsz:"+bufsz, 3, out.getLength());
      c += in.readLine(out); //dddd\r
      assertEquals("line5 length, bufsz:"+bufsz, 4, out.getLength());
      c += in.readLine(out); //""\r
      assertEquals("line6 length, bufsz:"+bufsz, 0, out.getLength());
      c += in.readLine(out); //""\r\n
      assertEquals("line7 length, bufsz:"+bufsz, 0, out.getLength());
      c += in.readLine(out); //""\r\n
      assertEquals("line8 length, bufsz:"+bufsz, 0, out.getLength());
      c += in.readLine(out); //"eeeee"EOF
      assertEquals("line9 length, bufsz:"+bufsz, 5, out.getLength());
      assertEquals("end of file, bufsz: "+bufsz, 0, in.readLine(out));
      assertEquals("total bytes, bufsz: "+bufsz, c, STRLENBYTES);
    }
  }

  /**
   * Test readLine for correct interpretation of maxLineLength
   * (returned string should be clipped at maxLineLength, and the
   * remaining bytes on the same line should be thrown out).
   * Also check that returned value matches the string length.
   * Varies buffer size to stress test.
   *
   * @throws Exception
   */
  public void testMaxLineLength() throws Exception {
    final String STR = "a\nbb\n\nccc\rdddd\r\neeeee";
    final int STRLENBYTES = STR.getBytes().length;
    Text out = new Text();
    for (int bufsz = 1; bufsz < STRLENBYTES+1; ++bufsz) {
      LineReader in = makeStream(STR, bufsz);
      int c = 0;
      c += in.readLine(out, 1);
      assertEquals("line1 length, bufsz: "+bufsz, 1, out.getLength());
      c += in.readLine(out, 1);
      assertEquals("line2 length, bufsz: "+bufsz, 1, out.getLength());
      c += in.readLine(out, 1);
      assertEquals("line3 length, bufsz: "+bufsz, 0, out.getLength());
      c += in.readLine(out, 3);
      assertEquals("line4 length, bufsz: "+bufsz, 3, out.getLength());
      c += in.readLine(out, 10);
      assertEquals("line5 length, bufsz: "+bufsz, 4, out.getLength());
      c += in.readLine(out, 8);
      assertEquals("line5 length, bufsz: "+bufsz, 5, out.getLength());
      assertEquals("end of file, bufsz: " +bufsz, 0, in.readLine(out));
      assertEquals("total bytes, bufsz: "+bufsz, c, STRLENBYTES);
    }
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
  
  private static List<Text> readSplit(TextInputFormat format, 
                                      InputSplit split, 
                                      JobConf job) throws IOException {
    List<Text> result = new ArrayList<Text>();
    RecordReader<LongWritable, Text> reader =
      format.getRecordReader(split, job, voidReporter);
    LongWritable key = reader.createKey();
    Text value = reader.createValue();
    while (reader.next(key, value)) {
      result.add(value);
      value = (Text) reader.createValue();
    }
    reader.close();
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
              "the quick\nbrown\nfox jumped\nover\n the lazy\n dog\n");
    writeFile(localFs, new Path(workDir, "part2.txt.gz"), gzip,
              "this is a test\nof gzip\n");
    FileInputFormat.setInputPaths(job, workDir);
    TextInputFormat format = new TextInputFormat();
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

  /**
   * Test using the gzip codec and an empty input file
   */
  public static void testGzipEmpty() throws IOException {
    JobConf job = new JobConf();
    CompressionCodec gzip = new GzipCodec();
    ReflectionUtils.setConf(gzip, job);
    localFs.delete(workDir, true);
    writeFile(localFs, new Path(workDir, "empty.gz"), gzip, "");
    FileInputFormat.setInputPaths(job, workDir);
    TextInputFormat format = new TextInputFormat();
    format.configure(job);
    InputSplit[] splits = format.getSplits(job, 100);
    assertEquals("Compressed files of length 0 are not returned from FileInputFormat.getSplits().",
                 1, splits.length);
    List<Text> results = readSplit(format, splits[0], job);
    assertEquals("Compressed empty file length == 0", 0, results.size());
  }
  
  private static String unquote(String in) {
    StringBuffer result = new StringBuffer();
    for(int i=0; i < in.length(); ++i) {
      char ch = in.charAt(i);
      if (ch == '\\') {
        ch = in.charAt(++i);
        switch (ch) {
        case 'n':
          result.append('\n');
          break;
        case 'r':
          result.append('\r');
          break;
        default:
          result.append(ch);
          break;
        }
      } else {
        result.append(ch);
      }
    }
    return result.toString();
  }

  /**
   * Parse the command line arguments into lines and display the result.
   * @param args
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {
    for(String arg: args) {
      System.out.println("Working on " + arg);
      LineReader reader = makeStream(unquote(arg));
      Text line = new Text();
      int size = reader.readLine(line);
      while (size > 0) {
        System.out.println("Got: " + line.toString());
        size = reader.readLine(line);
      }
      reader.close();
    }
  }
}
