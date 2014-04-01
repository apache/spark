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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;

/**
 * InputFormat reading keys, values from SequenceFiles in binary (raw)
 * format.
 */
public class SequenceFileAsBinaryInputFormat
    extends SequenceFileInputFormat<BytesWritable,BytesWritable> {

  public SequenceFileAsBinaryInputFormat() {
    super();
  }

  public RecordReader<BytesWritable,BytesWritable> getRecordReader(
      InputSplit split, JobConf job, Reporter reporter)
      throws IOException {
    return new SequenceFileAsBinaryRecordReader(job, (FileSplit)split);
  }

  /**
   * Read records from a SequenceFile as binary (raw) bytes.
   */
  public static class SequenceFileAsBinaryRecordReader
      implements RecordReader<BytesWritable,BytesWritable> {
    private SequenceFile.Reader in;
    private long start;
    private long end;
    private boolean done = false;
    private DataOutputBuffer buffer = new DataOutputBuffer();
    private SequenceFile.ValueBytes vbytes;

    public SequenceFileAsBinaryRecordReader(Configuration conf, FileSplit split)
        throws IOException {
      Path path = split.getPath();
      FileSystem fs = path.getFileSystem(conf);
      this.in = new SequenceFile.Reader(fs, path, conf);
      this.end = split.getStart() + split.getLength();
      if (split.getStart() > in.getPosition())
        in.sync(split.getStart());                  // sync to start
      this.start = in.getPosition();
      vbytes = in.createValueBytes();
      done = start >= end;
    }

    public BytesWritable createKey() {
      return new BytesWritable();
    }

    public BytesWritable createValue() {
      return new BytesWritable();
    }

    /**
     * Retrieve the name of the key class for this SequenceFile.
     * @see org.apache.hadoop.io.SequenceFile.Reader#getKeyClassName
     */
    public String getKeyClassName() {
      return in.getKeyClassName();
    }

    /**
     * Retrieve the name of the value class for this SequenceFile.
     * @see org.apache.hadoop.io.SequenceFile.Reader#getValueClassName
     */
    public String getValueClassName() {
      return in.getValueClassName();
    }

    /**
     * Read raw bytes from a SequenceFile.
     */
    public synchronized boolean next(BytesWritable key, BytesWritable val)
        throws IOException {
      if (done) return false;
      long pos = in.getPosition();
      boolean eof = -1 == in.nextRawKey(buffer);
      if (!eof) {
        key.set(buffer.getData(), 0, buffer.getLength());
        buffer.reset();
        in.nextRawValue(vbytes);
        vbytes.writeUncompressedBytes(buffer);
        val.set(buffer.getData(), 0, buffer.getLength());
        buffer.reset();
      }
      return !(done = (eof || (pos >= end && in.syncSeen())));
    }

    public long getPos() throws IOException {
      return in.getPosition();
    }

    public void close() throws IOException {
      in.close();
    }

    /**
     * Return the progress within the input split
     * @return 0.0 to 1.0 of the input byte range
     */
    public float getProgress() throws IOException {
      if (end == start) {
        return 0.0f;
      } else {
        return Math.min(1.0f, (float)((in.getPosition() - start) /
                                      (double)(end - start)));
      }
    }
  }
}
