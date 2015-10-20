/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.hive.mapred;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

/**
 * A generic RecordReader that can hand out different recordReaders
 * for each split in a {@link org.apache.spark.sql.hive.mapred.CombineSplit}.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class CombineSplitRecordReader<K, V> implements RecordReader<K, V> {
  protected CombineSplit split;
  protected JobConf jc;
  protected FileSystem fs;

  protected int idx;
  protected long progressedBytes;
  protected RecordReader<K, V> curReader;

  @Override
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
    return progressedBytes + curReader.getPos();
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
    return Math.min(1.0f,  progressedBytes /(float)(split.getLength()));
  }
  private InputFormat<K, V> inputFormat;
  /**
   * A generic RecordReader that can hand out different recordReaders
   * for each split in the CombineSplit.
   */
  public CombineSplitRecordReader(JobConf job, CombineSplit split,
                                  InputFormat<K, V> inputFormat)
    throws IOException {
    this.split = split;
    this.jc = job;
    this.idx = 0;
    this.curReader = null;
    this.progressedBytes = 0;
    this.inputFormat = inputFormat;
    initNextRecordReader();
  }

  /**
   * Get the record reader for the next split in this CombineSplit.
   */
  protected boolean initNextRecordReader() throws IOException {

    if (curReader != null) {
      curReader.close();
      curReader = null;
      if (idx > 0) {
        progressedBytes += split.getSplit(idx-1).getLength(); // done processing so far
      }
    }

    // if all splits have been processed, nothing more to do.
    if (idx == split.getSplitNum()) {
      return false;
    }

    // get a record reader for the idx-th split
    try {
      curReader = inputFormat.getRecordReader(split.getSplit(idx), jc, Reporter.NULL);
    } catch (Exception e) {
      throw new RuntimeException (e);
    }
    idx++;
    return true;
  }
}

