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

import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;

import org.apache.hadoop.io.UTF8;
import org.apache.hadoop.fs.Path;

/** A section of an input file.  Returned by {@link
 * InputFormat#getSplits(JobConf, int)} and passed to
 * {@link InputFormat#getRecordReader(InputSplit,JobConf,Reporter)}. 
 */
public class FileSplit extends org.apache.hadoop.mapreduce.InputSplit 
                       implements InputSplit {
  private Path file;
  private long start;
  private long length;
  private String[] hosts;
  
  FileSplit() {}

  /** Constructs a split.
   * @deprecated
   * @param file the file name
   * @param start the position of the first byte in the file to process
   * @param length the number of bytes in the file to process
   */
  @Deprecated
  public FileSplit(Path file, long start, long length, JobConf conf) {
    this(file, start, length, (String[])null);
  }

  /** Constructs a split with host information
   *
   * @param file the file name
   * @param start the position of the first byte in the file to process
   * @param length the number of bytes in the file to process
   * @param hosts the list of hosts containing the block, possibly null
   */
  public FileSplit(Path file, long start, long length, String[] hosts) {
    this.file = file;
    this.start = start;
    this.length = length;
    this.hosts = hosts;
  }

  /** The file containing this split's data. */
  public Path getPath() { return file; }
  
  /** The position of the first byte in the file to process. */
  public long getStart() { return start; }
  
  /** The number of bytes in the file to process. */
  public long getLength() { return length; }

  public String toString() { return file + ":" + start + "+" + length; }

  ////////////////////////////////////////////
  // Writable methods
  ////////////////////////////////////////////

  public void write(DataOutput out) throws IOException {
    UTF8.writeString(out, file.toString());
    out.writeLong(start);
    out.writeLong(length);
  }
  public void readFields(DataInput in) throws IOException {
    file = new Path(UTF8.readString(in));
    start = in.readLong();
    length = in.readLong();
    hosts = null;
  }

  public String[] getLocations() throws IOException {
    if (this.hosts == null) {
      return new String[]{};
    } else {
      return this.hosts;
    }
  }
  
}
