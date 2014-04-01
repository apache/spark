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
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

/**
 * Given a {@link org.apache.hadoop.mapreduce.lib.input.CombineFileSplit},
 * circularly read through each input source.
 */
class FileQueue extends InputStream {

  private int idx = -1;
  private long curlen = -1L;
  private FSDataInputStream input;
  private final byte[] z = new byte[1];
  private final Path[] paths;
  private final long[] lengths;
  private final long[] startoffset;
  private final Configuration conf;

  /**
   * @param split Description of input sources.
   * @param conf Used to resolve FileSystem instances.
   */
  public FileQueue(CombineFileSplit split, Configuration conf)
      throws IOException {
    this.conf = conf;
    paths = split.getPaths();
    startoffset = split.getStartOffsets();
    lengths = split.getLengths();
    nextSource();
  }

  protected void nextSource() throws IOException {
    if (0 == paths.length) {
      return;
    }
    if (input != null) {
      input.close();
    }
    idx = (idx + 1) % paths.length;
    curlen = lengths[idx];
    final Path file = paths[idx];
    final FileSystem fs = file.getFileSystem(conf);
    input = fs.open(file);
    input.seek(startoffset[idx]);
  }

  @Override
  public int read() throws IOException {
    final int tmp = read(z);
    return tmp == -1 ? -1 : (0xFF & z[0]);
  }

  @Override
  public int read(byte[] b) throws IOException {
    return read(b, 0, b.length);
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    int kvread = 0;
    while (kvread < len) {
      if (curlen <= 0) {
        nextSource();
        continue;
      }
      final int srcRead = (int) Math.min(len - kvread, curlen);
      IOUtils.readFully(input, b, kvread, srcRead);
      curlen -= srcRead;
      kvread += srcRead;
    }
    return kvread;
  }

  @Override
  public void close() throws IOException {
    input.close();
  }

}
