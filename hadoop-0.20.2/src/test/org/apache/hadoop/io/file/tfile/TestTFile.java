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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.file.tfile.TFile.Reader;
import org.apache.hadoop.io.file.tfile.TFile.Writer;
import org.apache.hadoop.io.file.tfile.TFile.Reader.Scanner;

/**
 * test tfile features.
 * 
 */
public class TestTFile extends TestCase {
  private static String ROOT =
      System.getProperty("test.build.data", "/tmp/tfile-test");
  private FileSystem fs;
  private Configuration conf;
  private final int minBlockSize = 512;
  private final int largeVal = 3 * 1024 * 1024;
  private static String localFormatter = "%010d";

  @Override
  public void setUp() throws IOException {
    conf = new Configuration();
    fs = FileSystem.get(conf);
  }

  @Override
  public void tearDown() throws IOException {
    // do nothing
  }

  // read a key from the scanner
  public byte[] readKey(Scanner scanner) throws IOException {
    int keylen = scanner.entry().getKeyLength();
    byte[] read = new byte[keylen];
    scanner.entry().getKey(read);
    return read;
  }

  // read a value from the scanner
  public byte[] readValue(Scanner scanner) throws IOException {
    int valueLen = scanner.entry().getValueLength();
    byte[] read = new byte[valueLen];
    scanner.entry().getValue(read);
    return read;
  }

  // read a long value from the scanner
  public byte[] readLongValue(Scanner scanner, int len) throws IOException {
    DataInputStream din = scanner.entry().getValueStream();
    byte[] b = new byte[len];
    din.readFully(b);
    din.close();
    return b;
  }

  // write some records into the tfile
  // write them twice
  private int writeSomeRecords(Writer writer, int start, int n)
      throws IOException {
    String value = "value";
    for (int i = start; i < (start + n); i++) {
      String key = String.format(localFormatter, i);
      writer.append(key.getBytes(), (value + key).getBytes());
      writer.append(key.getBytes(), (value + key).getBytes());
    }
    return (start + n);
  }

  // read the records and check
  private int readAndCheckbytes(Scanner scanner, int start, int n)
      throws IOException {
    String value = "value";
    for (int i = start; i < (start + n); i++) {
      byte[] key = readKey(scanner);
      byte[] val = readValue(scanner);
      String keyStr = String.format(localFormatter, i);
      String valStr = value + keyStr;
      assertTrue("btyes for keys do not match " + keyStr + " "
          + new String(key), Arrays.equals(keyStr.getBytes(), key));
      assertTrue("bytes for vals do not match " + valStr + " "
          + new String(val), Arrays.equals(
          valStr.getBytes(), val));
      assertTrue(scanner.advance());
      key = readKey(scanner);
      val = readValue(scanner);
      assertTrue("btyes for keys do not match", Arrays.equals(
          keyStr.getBytes(), key));
      assertTrue("bytes for vals do not match", Arrays.equals(
          valStr.getBytes(), val));
      assertTrue(scanner.advance());
    }
    return (start + n);
  }

  // write some large records
  // write them twice
  private int writeLargeRecords(Writer writer, int start, int n)
      throws IOException {
    byte[] value = new byte[largeVal];
    for (int i = start; i < (start + n); i++) {
      String key = String.format(localFormatter, i);
      writer.append(key.getBytes(), value);
      writer.append(key.getBytes(), value);
    }
    return (start + n);
  }

  // read large records
  // read them twice since its duplicated
  private int readLargeRecords(Scanner scanner, int start, int n)
      throws IOException {
    for (int i = start; i < (start + n); i++) {
      byte[] key = readKey(scanner);
      String keyStr = String.format(localFormatter, i);
      assertTrue("btyes for keys do not match", Arrays.equals(
          keyStr.getBytes(), key));
      scanner.advance();
      key = readKey(scanner);
      assertTrue("btyes for keys do not match", Arrays.equals(
          keyStr.getBytes(), key));
      scanner.advance();
    }
    return (start + n);
  }

  // write empty keys and values
  private void writeEmptyRecords(Writer writer, int n) throws IOException {
    byte[] key = new byte[0];
    byte[] value = new byte[0];
    for (int i = 0; i < n; i++) {
      writer.append(key, value);
    }
  }

  // read empty keys and values
  private void readEmptyRecords(Scanner scanner, int n) throws IOException {
    byte[] key = new byte[0];
    byte[] value = new byte[0];
    byte[] readKey = null;
    byte[] readValue = null;
    for (int i = 0; i < n; i++) {
      readKey = readKey(scanner);
      readValue = readValue(scanner);
      assertTrue("failed to match keys", Arrays.equals(readKey, key));
      assertTrue("failed to match values", Arrays.equals(readValue, value));
      assertTrue("failed to advance cursor", scanner.advance());
    }
  }

  private int writePrepWithKnownLength(Writer writer, int start, int n)
      throws IOException {
    // get the length of the key
    String key = String.format(localFormatter, start);
    int keyLen = key.getBytes().length;
    String value = "value" + key;
    int valueLen = value.getBytes().length;
    for (int i = start; i < (start + n); i++) {
      DataOutputStream out = writer.prepareAppendKey(keyLen);
      String localKey = String.format(localFormatter, i);
      out.write(localKey.getBytes());
      out.close();
      out = writer.prepareAppendValue(valueLen);
      String localValue = "value" + localKey;
      out.write(localValue.getBytes());
      out.close();
    }
    return (start + n);
  }

  private int readPrepWithKnownLength(Scanner scanner, int start, int n)
      throws IOException {
    for (int i = start; i < (start + n); i++) {
      String key = String.format(localFormatter, i);
      byte[] read = readKey(scanner);
      assertTrue("keys not equal", Arrays.equals(key.getBytes(), read));
      String value = "value" + key;
      read = readValue(scanner);
      assertTrue("values not equal", Arrays.equals(value.getBytes(), read));
      scanner.advance();
    }
    return (start + n);
  }

  private int writePrepWithUnkownLength(Writer writer, int start, int n)
      throws IOException {
    for (int i = start; i < (start + n); i++) {
      DataOutputStream out = writer.prepareAppendKey(-1);
      String localKey = String.format(localFormatter, i);
      out.write(localKey.getBytes());
      out.close();
      String value = "value" + localKey;
      out = writer.prepareAppendValue(-1);
      out.write(value.getBytes());
      out.close();
    }
    return (start + n);
  }

  private int readPrepWithUnknownLength(Scanner scanner, int start, int n)
      throws IOException {
    for (int i = start; i < start; i++) {
      String key = String.format(localFormatter, i);
      byte[] read = readKey(scanner);
      assertTrue("keys not equal", Arrays.equals(key.getBytes(), read));
      try {
        read = readValue(scanner);
        assertTrue(false);
      }
      catch (IOException ie) {
        // should have thrown exception
      }
      String value = "value" + key;
      read = readLongValue(scanner, value.getBytes().length);
      assertTrue("values nto equal", Arrays.equals(read, value.getBytes()));
      scanner.advance();
    }
    return (start + n);
  }

  private byte[] getSomeKey(int rowId) {
    return String.format(localFormatter, rowId).getBytes();
  }

  private void writeRecords(Writer writer) throws IOException {
    writeEmptyRecords(writer, 10);
    int ret = writeSomeRecords(writer, 0, 100);
    ret = writeLargeRecords(writer, ret, 1);
    ret = writePrepWithKnownLength(writer, ret, 40);
    ret = writePrepWithUnkownLength(writer, ret, 50);
    writer.close();
  }

  private void readAllRecords(Scanner scanner) throws IOException {
    readEmptyRecords(scanner, 10);
    int ret = readAndCheckbytes(scanner, 0, 100);
    ret = readLargeRecords(scanner, ret, 1);
    ret = readPrepWithKnownLength(scanner, ret, 40);
    ret = readPrepWithUnknownLength(scanner, ret, 50);
  }

  private FSDataOutputStream createFSOutput(Path name) throws IOException {
    if (fs.exists(name)) fs.delete(name, true);
    FSDataOutputStream fout = fs.create(name);
    return fout;
  }

  /**
   * test none codecs
   */
  void basicWithSomeCodec(String codec) throws IOException {
    Path ncTFile = new Path(ROOT, "basic.tfile");
    FSDataOutputStream fout = createFSOutput(ncTFile);
    Writer writer = new Writer(fout, minBlockSize, codec, "memcmp", conf);
    writeRecords(writer);
    fout.close();
    FSDataInputStream fin = fs.open(ncTFile);
    Reader reader =
        new Reader(fs.open(ncTFile), fs.getFileStatus(ncTFile).getLen(), conf);

    Scanner scanner = reader.createScanner();
    readAllRecords(scanner);
    scanner.seekTo(getSomeKey(50));
    assertTrue("location lookup failed", scanner.seekTo(getSomeKey(50)));
    // read the key and see if it matches
    byte[] readKey = readKey(scanner);
    assertTrue("seeked key does not match", Arrays.equals(getSomeKey(50),
        readKey));

    scanner.seekTo(new byte[0]);
    byte[] val1 = readValue(scanner);
    scanner.seekTo(new byte[0]);
    byte[] val2 = readValue(scanner);
    assertTrue(Arrays.equals(val1, val2));
    
    // check for lowerBound
    scanner.lowerBound(getSomeKey(50));
    assertTrue("locaton lookup failed", scanner.currentLocation
        .compareTo(reader.end()) < 0);
    readKey = readKey(scanner);
    assertTrue("seeked key does not match", Arrays.equals(readKey,
        getSomeKey(50)));

    // check for upper bound
    scanner.upperBound(getSomeKey(50));
    assertTrue("location lookup failed", scanner.currentLocation
        .compareTo(reader.end()) < 0);
    readKey = readKey(scanner);
    assertTrue("seeked key does not match", Arrays.equals(readKey,
        getSomeKey(51)));

    scanner.close();
    // test for a range of scanner
    scanner = reader.createScannerByKey(getSomeKey(10), getSomeKey(60));
    readAndCheckbytes(scanner, 10, 50);
    assertFalse(scanner.advance());
    scanner.close();
    reader.close();
    fin.close();
    fs.delete(ncTFile, true);
  }

  // unsorted with some codec
  void unsortedWithSomeCodec(String codec) throws IOException {
    Path uTfile = new Path(ROOT, "unsorted.tfile");
    FSDataOutputStream fout = createFSOutput(uTfile);
    Writer writer = new Writer(fout, minBlockSize, codec, null, conf);
    writeRecords(writer);
    writer.close();
    fout.close();
    FSDataInputStream fin = fs.open(uTfile);
    Reader reader =
        new Reader(fs.open(uTfile), fs.getFileStatus(uTfile).getLen(), conf);

    Scanner scanner = reader.createScanner();
    readAllRecords(scanner);
    scanner.close();
    reader.close();
    fin.close();
    fs.delete(uTfile, true);
  }

  public void testTFileFeatures() throws IOException {
    basicWithSomeCodec("none");
    basicWithSomeCodec("gz");
  }

  // test unsorted t files.
  public void testUnsortedTFileFeatures() throws IOException {
    unsortedWithSomeCodec("none");
    unsortedWithSomeCodec("gz");
  }

  private void writeNumMetablocks(Writer writer, String compression, int n)
      throws IOException {
    for (int i = 0; i < n; i++) {
      DataOutputStream dout =
          writer.prepareMetaBlock("TfileMeta" + i, compression);
      byte[] b = ("something to test" + i).getBytes();
      dout.write(b);
      dout.close();
    }
  }

  private void someTestingWithMetaBlock(Writer writer, String compression)
      throws IOException {
    DataOutputStream dout = null;
    writeNumMetablocks(writer, compression, 10);
    try {
      dout = writer.prepareMetaBlock("TfileMeta1", compression);
      assertTrue(false);
    }
    catch (MetaBlockAlreadyExists me) {
      // avoid this exception
    }
    dout = writer.prepareMetaBlock("TFileMeta100", compression);
    dout.close();
  }

  private void readNumMetablocks(Reader reader, int n) throws IOException {
    int len = ("something to test" + 0).getBytes().length;
    for (int i = 0; i < n; i++) {
      DataInputStream din = reader.getMetaBlock("TfileMeta" + i);
      byte b[] = new byte[len];
      din.readFully(b);
      assertTrue("faield to match metadata", Arrays.equals(
          ("something to test" + i).getBytes(), b));
      din.close();
    }
  }

  private void someReadingWithMetaBlock(Reader reader) throws IOException {
    DataInputStream din = null;
    readNumMetablocks(reader, 10);
    try {
      din = reader.getMetaBlock("NO ONE");
      assertTrue(false);
    }
    catch (MetaBlockDoesNotExist me) {
      // should catch
    }
    din = reader.getMetaBlock("TFileMeta100");
    int read = din.read();
    assertTrue("check for status", (read == -1));
    din.close();
  }

  // test meta blocks for tfiles
  public void testMetaBlocks() throws IOException {
    Path mFile = new Path(ROOT, "meta.tfile");
    FSDataOutputStream fout = createFSOutput(mFile);
    Writer writer = new Writer(fout, minBlockSize, "none", null, conf);
    someTestingWithMetaBlock(writer, "none");
    writer.close();
    fout.close();
    FSDataInputStream fin = fs.open(mFile);
    Reader reader = new Reader(fin, fs.getFileStatus(mFile).getLen(), conf);
    someReadingWithMetaBlock(reader);
    fs.delete(mFile, true);
    reader.close();
    fin.close();
  }
}
