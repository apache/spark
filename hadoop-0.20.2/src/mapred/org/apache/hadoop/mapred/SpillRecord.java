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

import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.util.zip.CheckedInputStream;
import java.util.zip.CheckedOutputStream;
import java.util.zip.Checksum;

import org.apache.hadoop.fs.ChecksumException;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SecureIOUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.PureJavaCrc32;

import static org.apache.hadoop.mapred.MapTask.MAP_OUTPUT_INDEX_RECORD_LENGTH;

class SpillRecord {

  /** Backing store */
  private final ByteBuffer buf;
  /** View of backing storage as longs */
  private final LongBuffer entries;

  public SpillRecord(int numPartitions) {
    buf = ByteBuffer.allocate(
        numPartitions * MapTask.MAP_OUTPUT_INDEX_RECORD_LENGTH);
    entries = buf.asLongBuffer();
  }

  public SpillRecord(Path indexFileName, JobConf job, String expectedIndexOwner)
  throws IOException {
    this(indexFileName, job, new PureJavaCrc32(), expectedIndexOwner);
  }

  public SpillRecord(Path indexFileName, JobConf job, Checksum crc,
      String expectedIndexOwner) throws IOException {

    final FileSystem rfs = FileSystem.getLocal(job).getRaw();
    final DataInputStream in = 
      new DataInputStream(SecureIOUtils.openForRead(
         new File(indexFileName.toUri().getPath()), expectedIndexOwner));
    try {
      final long length = rfs.getFileStatus(indexFileName).getLen();
      final int partitions = (int) length / MAP_OUTPUT_INDEX_RECORD_LENGTH;
      final int size = partitions * MAP_OUTPUT_INDEX_RECORD_LENGTH;

      buf = ByteBuffer.allocate(size);
      if (crc != null) {
        crc.reset();
        CheckedInputStream chk = new CheckedInputStream(in, crc);
        IOUtils.readFully(chk, buf.array(), 0, size);
        if (chk.getChecksum().getValue() != in.readLong()) {
          throw new ChecksumException("Checksum error reading spill index: " +
                                indexFileName, -1);
        }
      } else {
        IOUtils.readFully(in, buf.array(), 0, size);
      }
      entries = buf.asLongBuffer();
    } finally {
      in.close();
    }
  }

  /**
   * Return number of IndexRecord entries in this spill.
   */
  public int size() {
    return entries.capacity() / (MapTask.MAP_OUTPUT_INDEX_RECORD_LENGTH / 8);
  }

  /**
   * Get spill offsets for given partition.
   */
  public IndexRecord getIndex(int partition) {
    final int pos = partition * MapTask.MAP_OUTPUT_INDEX_RECORD_LENGTH / 8;
    return new IndexRecord(entries.get(pos), entries.get(pos + 1),
                           entries.get(pos + 2));
  }

  /**
   * Set spill offsets for given partition.
   */
  public void putIndex(IndexRecord rec, int partition) {
    final int pos = partition * MapTask.MAP_OUTPUT_INDEX_RECORD_LENGTH / 8;
    entries.put(pos, rec.startOffset);
    entries.put(pos + 1, rec.rawLength);
    entries.put(pos + 2, rec.partLength);
  }

  /**
   * Write this spill record to the location provided.
   */
  public void writeToFile(Path loc, JobConf job)
      throws IOException {
    writeToFile(loc, job, new PureJavaCrc32());
  }

  public void writeToFile(Path loc, JobConf job, Checksum crc)
      throws IOException {
    final FileSystem rfs = FileSystem.getLocal(job).getRaw();
    CheckedOutputStream chk = null;
    final FSDataOutputStream out = rfs.create(loc);
    try {
      if (crc != null) {
        crc.reset();
        chk = new CheckedOutputStream(out, crc);
        chk.write(buf.array());
        out.writeLong(chk.getChecksum().getValue());
      } else {
        out.write(buf.array());
      }
    } finally {
      if (chk != null) {
        chk.close();
      } else {
        out.close();
      }
    }
  }

}

class IndexRecord {
  long startOffset;
  long rawLength;
  long partLength;

  public IndexRecord() { }

  public IndexRecord(long startOffset, long rawLength, long partLength) {
    this.startOffset = startOffset;
    this.rawLength = rawLength;
    this.partLength = partLength;
  }
}
