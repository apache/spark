/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.io.file.tfile;

import java.io.IOException;
import java.util.Random;

import junit.framework.Assert;
import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.file.tfile.TFile.Reader;
import org.apache.hadoop.io.file.tfile.TFile.Writer;
import org.apache.hadoop.io.file.tfile.TFile.Reader.Scanner;

public class TestTFileSplit extends TestCase {
  private static String ROOT =
      System.getProperty("test.build.data", "/tmp/tfile-test");

  private final static int BLOCK_SIZE = 64 * 1024;

  private static final String KEY = "key";
  private static final String VALUE = "value";

  private FileSystem fs;
  private Configuration conf;
  private Path path;
  private Random random = new Random();

  private String comparator = "memcmp";
  private String outputFile = "TestTFileSplit";

  void createFile(int count, String compress) throws IOException {
    conf = new Configuration();
    path = new Path(ROOT, outputFile + "." + compress);
    fs = path.getFileSystem(conf);
    FSDataOutputStream out = fs.create(path);
    Writer writer = new Writer(out, BLOCK_SIZE, compress, comparator, conf);

    int nx;
    for (nx = 0; nx < count; nx++) {
      byte[] key = composeSortedKey(KEY, count, nx).getBytes();
      byte[] value = (VALUE + nx).getBytes();
      writer.append(key, value);
    }
    writer.close();
    out.close();
  }

  void readFile() throws IOException {
    long fileLength = fs.getFileStatus(path).getLen();
    int numSplit = 10;
    long splitSize = fileLength / numSplit + 1;

    Reader reader =
        new Reader(fs.open(path), fs.getFileStatus(path).getLen(), conf);
    long offset = 0;
    long rowCount = 0;
    BytesWritable key, value;
    for (int i = 0; i < numSplit; ++i, offset += splitSize) {
      Scanner scanner = reader.createScannerByByteRange(offset, splitSize);
      int count = 0;
      key = new BytesWritable();
      value = new BytesWritable();
      while (!scanner.atEnd()) {
        scanner.entry().get(key, value);
        ++count;
        scanner.advance();
      }
      scanner.close();
      Assert.assertTrue(count > 0);
      rowCount += count;
    }
    Assert.assertEquals(rowCount, reader.getEntryCount());
    reader.close();
  }

  /* Similar to readFile(), tests the scanner created 
   * by record numbers rather than the offsets.
   */
  void readRowSplits(int numSplits) throws IOException {

    Reader reader =
      new Reader(fs.open(path), fs.getFileStatus(path).getLen(), conf);
    
    long totalRecords = reader.getEntryCount();
    for (int i=0; i<numSplits; i++) {
      long startRec = i*totalRecords/numSplits;
      long endRec = (i+1)*totalRecords/numSplits;
      if (i == numSplits-1) {
        endRec = totalRecords;
      }
      Scanner scanner = reader.createScannerByRecordNum(startRec, endRec);
      int count = 0;
      BytesWritable key = new BytesWritable();
      BytesWritable value = new BytesWritable();
      long x=startRec;
      while (!scanner.atEnd()) {
        assertEquals("Incorrect RecNum returned by scanner", scanner.getRecordNum(), x);
        scanner.entry().get(key, value);
        ++count;
        assertEquals("Incorrect RecNum returned by scanner", scanner.getRecordNum(), x);
        scanner.advance();
        ++x;
      }
      scanner.close();
      Assert.assertTrue(count == (endRec - startRec));
    }
    // make sure specifying range at the end gives zero records.
    Scanner scanner = reader.createScannerByRecordNum(totalRecords, -1);
    Assert.assertTrue(scanner.atEnd());
  }
  
  static String composeSortedKey(String prefix, int total, int value) {
    return String.format("%s%010d", prefix, value);
  }
  
  void checkRecNums() throws IOException {
    long fileLen = fs.getFileStatus(path).getLen();
    Reader reader = new Reader(fs.open(path), fileLen, conf);
    long totalRecs = reader.getEntryCount();
    long begin = random.nextLong() % (totalRecs / 2);
    if (begin < 0)
      begin += (totalRecs / 2);
    long end = random.nextLong() % (totalRecs / 2);
    if (end < 0)
      end += (totalRecs / 2);
    end += (totalRecs / 2) + 1;

    assertEquals("RecNum for offset=0 should be 0", 0, reader
        .getRecordNumNear(0));
    for (long x : new long[] { fileLen, fileLen + 1, 2 * fileLen }) {
      assertEquals("RecNum for offset>=fileLen should be total entries",
          totalRecs, reader.getRecordNumNear(x));
    }

    for (long i = 0; i < 100; ++i) {
      assertEquals("Locaton to RecNum conversion not symmetric", i, reader
          .getRecordNumByLocation(reader.getLocationByRecordNum(i)));
    }

    for (long i = 1; i < 100; ++i) {
      long x = totalRecs - i;
      assertEquals("Locaton to RecNum conversion not symmetric", x, reader
          .getRecordNumByLocation(reader.getLocationByRecordNum(x)));
    }

    for (long i = begin; i < end; ++i) {
      assertEquals("Locaton to RecNum conversion not symmetric", i, reader
          .getRecordNumByLocation(reader.getLocationByRecordNum(i)));
    }

    for (int i = 0; i < 1000; ++i) {
      long x = random.nextLong() % totalRecs;
      if (x < 0) x += totalRecs;
      assertEquals("Locaton to RecNum conversion not symmetric", x, reader
          .getRecordNumByLocation(reader.getLocationByRecordNum(x)));
    }
  }
  
  public void testSplit() throws IOException {
    System.out.println("testSplit");
    createFile(100000, Compression.Algorithm.NONE.getName());
    checkRecNums();   
    readFile();
    readRowSplits(10);
    fs.delete(path, true);
    createFile(500000, Compression.Algorithm.GZ.getName());
    checkRecNums();
    readFile();
    readRowSplits(83);
    fs.delete(path, true);
  }
}
