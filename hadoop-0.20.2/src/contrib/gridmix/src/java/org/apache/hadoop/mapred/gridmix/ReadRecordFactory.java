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
package org.apache.hadoop.mapred.gridmix;

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;

/**
 * For every record consumed, read key + val bytes from the stream provided.
 */
class ReadRecordFactory extends RecordFactory {

  /**
   * Size of internal, scratch buffer to read from internal stream.
   */
  public static final String GRIDMIX_READ_BUF_SIZE = "gridmix.read.buffer.size";

  private final byte[] buf;
  private final InputStream src;
  private final RecordFactory factory;

  /**
   * @param targetBytes Expected byte count.
   * @param targetRecords Expected record count.
   * @param src Stream to read bytes.
   * @param conf Used to establish read buffer size. @see #GRIDMIX_READ_BUF_SIZE
   */
  public ReadRecordFactory(long targetBytes, long targetRecords,
      InputStream src, Configuration conf) {
    this(new AvgRecordFactory(targetBytes, targetRecords, conf), src, conf);
  }

  /**
   * @param factory Factory to draw record sizes.
   * @param src Stream to read bytes.
   * @param conf Used to establish read buffer size. @see #GRIDMIX_READ_BUF_SIZE
   */
  public ReadRecordFactory(RecordFactory factory, InputStream src,
      Configuration conf) {
    this.src = src;
    this.factory = factory;
    buf = new byte[conf.getInt(GRIDMIX_READ_BUF_SIZE, 64 * 1024)];
  }

  @Override
  public boolean next(GridmixKey key, GridmixRecord val) throws IOException {
    if (!factory.next(key, val)) {
      return false;
    }
    for (int len = (null == key ? 0 : key.getSize()) + val.getSize();
         len > 0; len -= buf.length) {
      IOUtils.readFully(src, buf, 0, Math.min(buf.length, len));
    }
    return true;
  }

  @Override
  public float getProgress() throws IOException {
    return factory.getProgress();
  }

  @Override
  public void close() throws IOException {
    IOUtils.cleanup(null, src);
    factory.close();
  }
}
