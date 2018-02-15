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

package org.apache.hadoop.hive.ql.io;

import org.apache.hadoop.hive.io.HiveIOExceptionHandlerUtil;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.*;

import java.io.IOException;

/**
 * A {@link RecordReader} implementation for reading {@link BucketizedHiveInputSplit}.
 * Each {@link BucketizedHiveInputSplit} packs multiple {@link InputSplit} in itself which
 * correspond to a single bucket. This record reader would open the record reader for those
 * splits one by one (transparent to the caller) and return records.
 *
 * The Hive counterpart for this class is {@link BucketizedHiveRecordReader}. The reason for not
 * re-using {@link BucketizedHiveRecordReader} in Hive is because it relies on Map-Reduce plan
 * which is not avaliable while we are running jobs in Spark.
 */
public class BucketizedSparkRecordReader<K extends WritableComparable, V extends Writable>
        implements RecordReader<K, V> {

  protected final BucketizedHiveInputSplit split;
  protected final InputFormat inputFormat;

  private final Reporter reporter;
  private long progress;
  private int index;

  protected RecordReader recordReader;
  protected JobConf jobConf;

  public BucketizedSparkRecordReader(
      InputFormat inputFormat,
      BucketizedHiveInputSplit bucketizedSplit,
      JobConf jobConf,
      Reporter reporter) throws IOException {
    this.recordReader = null;
    this.jobConf = jobConf;
    this.split = bucketizedSplit;
    this.inputFormat = inputFormat;
    this.reporter = reporter;
    initNextRecordReader();
  }

  /**
   * Get the record reader for the next chunk
   */
  private boolean initNextRecordReader() throws IOException {
    if (recordReader != null) {
      recordReader.close();
      recordReader = null;
      if (index > 0) {
        progress += split.getLength(index - 1); // done processing so far
      }
    }

    // if all chunks have been processed, nothing more to do.
    if (index == split.getNumSplits()) {
      return false;
    }

    try {
      // get a record reader for the index-th chunk
      recordReader = inputFormat.getRecordReader(split.getSplit(index), jobConf, reporter);
    } catch (Exception e) {
      recordReader = HiveIOExceptionHandlerUtil.handleRecordReaderCreationException(e, jobConf);
    }

    index++;
    return true;
  }

  private boolean doNextWithExceptionHandler(K key, V value) throws IOException {
    try {
      return recordReader.next(key,  value);
    } catch (Exception e) {
      return HiveIOExceptionHandlerUtil.handleRecordReaderNextException(e, jobConf);
    }
  }

  @Override
  public boolean next(K key, V value) throws IOException {
    try {
      while ((recordReader == null) || !doNextWithExceptionHandler(key, value)) {
        if (!initNextRecordReader()) {
          return false;
        }
      }
      return true;
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public K createKey() {
    return (K) recordReader.createKey();
  }

  @Override
  public V createValue() {
    return (V) recordReader.createValue();
  }

  @Override
  public long getPos() throws IOException {
    if (recordReader != null) {
      return recordReader.getPos();
    } else {
      return 0;
    }
  }

  @Override
  public void close() throws IOException {
    if (recordReader != null) {
      recordReader.close();
      recordReader = null;
    }
    index = 0;
  }

  @Override
  public float getProgress() throws IOException {
    return Math.min(1.0f, (recordReader == null ?
            progress : recordReader.getPos()) / (float) (split.getLength()));
  }
}
