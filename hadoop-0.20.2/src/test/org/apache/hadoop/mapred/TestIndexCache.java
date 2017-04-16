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

import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Random;
import java.util.zip.CRC32;
import java.util.zip.CheckedOutputStream;

import org.apache.hadoop.fs.ChecksumException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.security.UserGroupInformation;

import junit.framework.TestCase;

public class TestIndexCache extends TestCase {

  public void testLRCPolicy() throws Exception {
    Random r = new Random();
    long seed = r.nextLong();
    r.setSeed(seed);
    System.out.println("seed: " + seed);
    JobConf conf = new JobConf();
    FileSystem fs = FileSystem.getLocal(conf).getRaw();
    Path p = new Path(System.getProperty("test.build.data", "/tmp"),
        "cache").makeQualified(fs);
    fs.delete(p, true);
    conf.setInt("mapred.tasktracker.indexcache.mb", 1);
    final int partsPerMap = 1000;
    final int bytesPerFile = partsPerMap * 24;
    IndexCache cache = new IndexCache(conf);

    // fill cache
    int totalsize = bytesPerFile;
    for (; totalsize < 1024 * 1024; totalsize += bytesPerFile) {
      Path f = new Path(p, Integer.toString(totalsize, 36));
      writeFile(fs, f, totalsize, partsPerMap);
      IndexRecord rec = cache.getIndexInformation(
          Integer.toString(totalsize, 36), r.nextInt(partsPerMap), f,
          UserGroupInformation.getCurrentUser().getShortUserName());
      checkRecord(rec, totalsize);
    }

    // delete files, ensure cache retains all elem
    for (FileStatus stat : fs.listStatus(p)) {
      fs.delete(stat.getPath(),true);
    }
    for (int i = bytesPerFile; i < 1024 * 1024; i += bytesPerFile) {
      Path f = new Path(p, Integer.toString(i, 36));
      IndexRecord rec = cache.getIndexInformation(Integer.toString(i, 36),
          r.nextInt(partsPerMap), f,
          UserGroupInformation.getCurrentUser().getShortUserName());
      checkRecord(rec, i);
    }

    // push oldest (bytesPerFile) out of cache
    Path f = new Path(p, Integer.toString(totalsize, 36));
    writeFile(fs, f, totalsize, partsPerMap);
    cache.getIndexInformation(Integer.toString(totalsize, 36),
        r.nextInt(partsPerMap), f,
        UserGroupInformation.getCurrentUser().getShortUserName());
    fs.delete(f, false);

    // oldest fails to read, or error
    boolean fnf = false;
    try {
      cache.getIndexInformation(Integer.toString(bytesPerFile, 36),
          r.nextInt(partsPerMap), new Path(p, Integer.toString(bytesPerFile)),
          UserGroupInformation.getCurrentUser().getShortUserName());
    } catch (IOException e) {
      if (e.getCause() == null ||
          !(e.getCause()  instanceof FileNotFoundException)) {
        throw e;
      }
      else {
        fnf = true;
      }
    }
    if (!fnf)
      fail("Failed to push out last entry");
    // should find all the other entries
    for (int i = bytesPerFile << 1; i < 1024 * 1024; i += bytesPerFile) {
      IndexRecord rec = cache.getIndexInformation(Integer.toString(i, 36),
          r.nextInt(partsPerMap), new Path(p, Integer.toString(i, 36)),
          UserGroupInformation.getCurrentUser().getShortUserName());
      checkRecord(rec, i);
    }
    IndexRecord rec = cache.getIndexInformation(Integer.toString(totalsize, 36),
        r.nextInt(partsPerMap), f,
        UserGroupInformation.getCurrentUser().getShortUserName());
    checkRecord(rec, totalsize);
  }

  public void testBadIndex() throws Exception {
    final int parts = 30;
    JobConf conf = new JobConf();
    FileSystem fs = FileSystem.getLocal(conf).getRaw();
    Path p = new Path(System.getProperty("test.build.data", "/tmp"),
        "cache").makeQualified(fs);
    fs.delete(p, true);
    conf.setInt("mapred.tasktracker.indexcache.mb", 1);
    IndexCache cache = new IndexCache(conf);

    Path f = new Path(p, "badindex");
    FSDataOutputStream out = fs.create(f, false);
    CheckedOutputStream iout = new CheckedOutputStream(out, new CRC32());
    DataOutputStream dout = new DataOutputStream(iout);
    for (int i = 0; i < parts; ++i) {
      for (int j = 0; j < MapTask.MAP_OUTPUT_INDEX_RECORD_LENGTH / 8; ++j) {
        if (0 == (i % 3)) {
          dout.writeLong(i);
        } else {
          out.writeLong(i);
        }
      }
    }
    out.writeLong(iout.getChecksum().getValue());
    dout.close();
    try {
      cache.getIndexInformation("badindex", 7, f, 
          UserGroupInformation.getCurrentUser().getShortUserName());
      fail("Did not detect bad checksum");
    } catch (IOException e) {
      if (!(e.getCause() instanceof ChecksumException)) {
        throw e;
      }
    }
  }

  private static void checkRecord(IndexRecord rec, long fill) {
    assertEquals(fill, rec.startOffset);
    assertEquals(fill, rec.rawLength);
    assertEquals(fill, rec.partLength);
  }

  private static void writeFile(FileSystem fs, Path f, long fill, int parts)
      throws IOException {
    FSDataOutputStream out = fs.create(f, false);
    CheckedOutputStream iout = new CheckedOutputStream(out, new CRC32());
    DataOutputStream dout = new DataOutputStream(iout);
    for (int i = 0; i < parts; ++i) {
      for (int j = 0; j < MapTask.MAP_OUTPUT_INDEX_RECORD_LENGTH / 8; ++j) {
        dout.writeLong(fill);
      }
    }
    out.writeLong(iout.getChecksum().getValue());
    dout.close();
  }
}
