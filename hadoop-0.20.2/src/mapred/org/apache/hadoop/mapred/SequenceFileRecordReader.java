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
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.ReflectionUtils;

/** An {@link RecordReader} for {@link SequenceFile}s. */
public class SequenceFileRecordReader<K, V> implements RecordReader<K, V> {
  
  private SequenceFile.Reader in;
  private long start;
  private long end;
  private boolean more = true;
  protected Configuration conf;

  public SequenceFileRecordReader(Configuration conf, FileSplit split)
    throws IOException {
    Path path = split.getPath();
    FileSystem fs = path.getFileSystem(conf);
    this.in = new SequenceFile.Reader(fs, path, conf);
    this.end = split.getStart() + split.getLength();
    this.conf = conf;

    if (split.getStart() > in.getPosition())
      in.sync(split.getStart());                  // sync to start

    this.start = in.getPosition();
    more = start < end;
  }


  /** The class of key that must be passed to {@link
   * #next(Object, Object)}.. */
  public Class getKeyClass() { return in.getKeyClass(); }

  /** The class of value that must be passed to {@link
   * #next(Object, Object)}.. */
  public Class getValueClass() { return in.getValueClass(); }
  
  @SuppressWarnings("unchecked")
  public K createKey() {
    return (K) ReflectionUtils.newInstance(getKeyClass(), conf);
  }
  
  @SuppressWarnings("unchecked")
  public V createValue() {
    return (V) ReflectionUtils.newInstance(getValueClass(), conf);
  }
    
  public synchronized boolean next(K key, V value) throws IOException {
    if (!more) return false;
    long pos = in.getPosition();
    boolean remaining = (in.next(key) != null);
    if (remaining) {
      getCurrentValue(value);
    }
    if (pos >= end && in.syncSeen()) {
      more = false;
    } else {
      more = remaining;
    }
    return more;
  }
  
  protected synchronized boolean next(K key)
    throws IOException {
    if (!more) return false;
    long pos = in.getPosition();
    boolean remaining = (in.next(key) != null);
    if (pos >= end && in.syncSeen()) {
      more = false;
    } else {
      more = remaining;
    }
    return more;
  }
  
  protected synchronized void getCurrentValue(V value)
    throws IOException {
    in.getCurrentValue(value);
  }
  
  /**
   * Return the progress within the input split
   * @return 0.0 to 1.0 of the input byte range
   */
  public float getProgress() throws IOException {
    if (end == start) {
      return 0.0f;
    } else {
      return Math.min(1.0f, (in.getPosition() - start) / (float)(end - start));
    }
  }
  
  public synchronized long getPos() throws IOException {
    return in.getPosition();
  }
  
  protected synchronized void seek(long pos) throws IOException {
    in.seek(pos);
  }
  public synchronized void close() throws IOException { in.close(); }
  
}

