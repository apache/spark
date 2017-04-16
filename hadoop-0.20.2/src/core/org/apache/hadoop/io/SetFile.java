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

package org.apache.hadoop.io;

import java.io.*;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;

/** A file-based set of keys. */
public class SetFile extends MapFile {

  protected SetFile() {}                            // no public ctor

  /** 
   * Write a new set file.
   */
  public static class Writer extends MapFile.Writer {

    /** Create the named set for keys of the named class. 
     *  @deprecated pass a Configuration too
     */
    public Writer(FileSystem fs, String dirName,
	Class<? extends WritableComparable> keyClass) throws IOException {
      super(new Configuration(), fs, dirName, keyClass, NullWritable.class);
    }

    /** Create a set naming the element class and compression type. */
    public Writer(Configuration conf, FileSystem fs, String dirName,
                  Class<? extends WritableComparable> keyClass,
                  SequenceFile.CompressionType compress)
      throws IOException {
      this(conf, fs, dirName, WritableComparator.get(keyClass), compress);
    }

    /** Create a set naming the element comparator and compression type. */
    public Writer(Configuration conf, FileSystem fs, String dirName,
                  WritableComparator comparator,
                  SequenceFile.CompressionType compress) throws IOException {
      super(conf, fs, dirName, comparator, NullWritable.class, compress);
    }

    /** Append a key to a set.  The key must be strictly greater than the
     * previous key added to the set. */
    public void append(WritableComparable key) throws IOException{
      append(key, NullWritable.get());
    }
  }

  /** Provide access to an existing set file. */
  public static class Reader extends MapFile.Reader {

    /** Construct a set reader for the named set.*/
    public Reader(FileSystem fs, String dirName, Configuration conf) throws IOException {
      super(fs, dirName, conf);
    }

    /** Construct a set reader for the named set using the named comparator.*/
    public Reader(FileSystem fs, String dirName, WritableComparator comparator, Configuration conf)
      throws IOException {
      super(fs, dirName, comparator, conf);
    }

    // javadoc inherited
    public boolean seek(WritableComparable key)
      throws IOException {
      return super.seek(key);
    }

    /** Read the next key in a set into <code>key</code>.  Returns
     * true if such a key exists and false when at the end of the set. */
    public boolean next(WritableComparable key)
      throws IOException {
      return next(key, NullWritable.get());
    }

    /** Read the matching key from a set into <code>key</code>.
     * Returns <code>key</code>, or null if no match exists. */
    public WritableComparable get(WritableComparable key)
      throws IOException {
      if (seek(key)) {
        next(key);
        return key;
      } else
        return null;
    }
  }

}
