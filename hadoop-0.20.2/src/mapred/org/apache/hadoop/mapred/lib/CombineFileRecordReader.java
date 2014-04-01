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

package org.apache.hadoop.mapred.lib;

import java.io.*;
import java.util.*;
import java.lang.reflect.*;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.mapred.*;
import org.apache.hadoop.conf.Configuration;

/**
 * A generic RecordReader that can hand out different recordReaders
 * for each chunk in a {@link CombineFileSplit}.
 * A CombineFileSplit can combine data chunks from multiple files. 
 * This class allows using different RecordReaders for processing
 * these data chunks from different files.
 * @see CombineFileSplit
 */

public class CombineFileRecordReader<K, V> implements RecordReader<K, V> {

  static final Class [] constructorSignature = new Class [] 
                                         {CombineFileSplit.class, 
                                          Configuration.class, 
                                          Reporter.class,
                                          Integer.class};

  protected CombineFileSplit split;
  protected JobConf jc;
  protected Reporter reporter;
  protected Class<RecordReader<K, V>> rrClass;
  protected Constructor<RecordReader<K, V>> rrConstructor;
  protected FileSystem fs;
  
  protected int idx;
  protected long progress;
  protected RecordReader<K, V> curReader;
  
  public boolean next(K key, V value) throws IOException {

    while ((curReader == null) || !curReader.next(key, value)) {
      if (!initNextRecordReader()) {
        return false;
      }
    }
    return true;
  }

  public K createKey() {
    return curReader.createKey();
  }
  
  public V createValue() {
    return curReader.createValue();
  }
  
  /**
   * return the amount of data processed
   */
  public long getPos() throws IOException {
    return progress;
  }
  
  public void close() throws IOException {
    if (curReader != null) {
      curReader.close();
      curReader = null;
    }
  }
  
  /**
   * return progress based on the amount of data processed so far.
   */
  public float getProgress() throws IOException {
    long subprogress = 0;    // bytes processed in current split
    if (null != curReader) {
      // idx is always one past the current subsplit's true index.
      subprogress = (long)(curReader.getProgress() * split.getLength(idx - 1));
    }
    return Math.min(1.0f,  progress/(float)(split.getLength()));
  }
  
  /**
   * A generic RecordReader that can hand out different recordReaders
   * for each chunk in the CombineFileSplit.
   */
  public CombineFileRecordReader(JobConf job, CombineFileSplit split, 
                                 Reporter reporter,
                                 Class<RecordReader<K, V>> rrClass)
    throws IOException {
    this.split = split;
    this.jc = job;
    this.rrClass = rrClass;
    this.reporter = reporter;
    this.idx = 0;
    this.curReader = null;
    this.progress = 0;

    try {
      rrConstructor = rrClass.getDeclaredConstructor(constructorSignature);
      rrConstructor.setAccessible(true);
    } catch (Exception e) {
      throw new RuntimeException(rrClass.getName() + 
                                 " does not have valid constructor", e);
    }
    initNextRecordReader();
  }
  
  /**
   * Get the record reader for the next chunk in this CombineFileSplit.
   */
  protected boolean initNextRecordReader() throws IOException {

    if (curReader != null) {
      curReader.close();
      curReader = null;
      if (idx > 0) {
        progress += split.getLength(idx-1);    // done processing so far
      }
    }

    // if all chunks have been processed, nothing more to do.
    if (idx == split.getNumPaths()) {
      return false;
    }

    // get a record reader for the idx-th chunk
    try {
      curReader =  rrConstructor.newInstance(new Object [] 
                            {split, jc, reporter, Integer.valueOf(idx)});

      // setup some helper config variables.
      jc.set("map.input.file", split.getPath(idx).toString());
      jc.setLong("map.input.start", split.getOffset(idx));
      jc.setLong("map.input.length", split.getLength(idx));
    } catch (Exception e) {
      throw new RuntimeException (e);
    }
    idx++;
    return true;
  }
}
