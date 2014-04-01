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

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * InputFormat reading keys, values from SequenceFiles in binary (raw)
 * format.
 */
public class SequenceFileAsBinaryInputFormat
    extends SequenceFileInputFormat<BytesWritable,BytesWritable> {

  public SequenceFileAsBinaryInputFormat() {
    super();
  }

  public RecordReader<BytesWritable,BytesWritable> createRecordReader(
      InputSplit split, TaskAttemptContext context)
      throws IOException {
    return new SequenceFileAsBinaryRecordReader();
  }

  /**
   * Read records from a SequenceFile as binary (raw) bytes.
   */
  public static class SequenceFileAsBinaryRecordReader
      extends RecordReader<BytesWritable,BytesWritable> {
    private SequenceFile.Reader in;
    private long start;
    private long end;
    private boolean done = false;
    private DataOutputBuffer buffer = new DataOutputBuffer();
    private SequenceFile.ValueBytes vbytes;
    private BytesWritable key = null;
    private BytesWritable value = null;

    public void initialize(InputSplit split, TaskAttemptContext context) 
        throws IOException, InterruptedException {
      Path path = ((FileSplit)split).getPath();
      Configuration conf = context.getConfiguration();
      FileSystem fs = path.getFileSystem(conf);
      this.in = new SequenceFile.Reader(fs, path, conf);
      this.end = ((FileSplit)split).getStart() + split.getLength();
      if (((FileSplit)split).getStart() > in.getPosition()) {
        in.sync(((FileSplit)split).getStart());    // sync to start
      }
      this.start = in.getPosition();
      vbytes = in.createValueBytes();
      done = start >= end;
    }
    
    @Override
    public BytesWritable getCurrentKey() 
        throws IOException, InterruptedException {
      return key;
    }
    
    @Override
    public BytesWritable getCurrentValue() 
        throws IOException, InterruptedException {
      return value;
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
    public synchronized boolean nextKeyValue()
        throws IOException, InterruptedException {
      if (done) {
        return false;
      }
      long pos = in.getPosition();
      boolean eof = -1 == in.nextRawKey(buffer);
      if (!eof) {
        if (key == null) {
          key = new BytesWritable();
        }
        if (value == null) {
          value = new BytesWritable();
        }
        key.set(buffer.getData(), 0, buffer.getLength());
        buffer.reset();
        in.nextRawValue(vbytes);
        vbytes.writeUncompressedBytes(buffer);
        value.set(buffer.getData(), 0, buffer.getLength());
        buffer.reset();
      }
      return !(done = (eof || (pos >= end && in.syncSeen())));
    }

    public void close() throws IOException {
      in.close();
    }

    /**
     * Return the progress within the input split
     * @return 0.0 to 1.0 of the input byte range
     */
    public float getProgress() throws IOException, InterruptedException {
      if (end == start) {
        return 0.0f;
      } else {
        return Math.min(1.0f, (float)((in.getPosition() - start) /
                                      (double)(end - start)));
      }
    }
  }
}
