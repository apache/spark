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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Given a {@link #FilePool}, obtain a set of files capable of satisfying
 * a full set of splits, then iterate over each source to fill the request.
 */
class InputStriper {
  public static final Log LOG = LogFactory.getLog(InputStriper.class);
  int idx;
  long currentStart;
  FileStatus current;
  final List<FileStatus> files = new ArrayList<FileStatus>();

  /**
   * @param inputDir Pool from which files are requested.
   * @param mapBytes Sum of all expected split requests.
   */
  InputStriper(FilePool inputDir, long mapBytes)
      throws IOException {
    final long inputBytes = inputDir.getInputFiles(mapBytes, files);
    if (mapBytes > inputBytes) {
      LOG.warn("Using " + inputBytes + "/" + mapBytes + " bytes");
    }
    if (files.isEmpty() && mapBytes > 0) {
      throw new IOException("Failed to satisfy request for " + mapBytes);
    }
    current = files.isEmpty() ? null : files.get(0);
  }

  /**
   * @param inputDir Pool used to resolve block locations.
   * @param bytes Target byte count
   * @param nLocs Number of block locations per split.
   * @return A set of files satisfying the byte count, with locations weighted
   *         to the dominating proportion of input bytes.
   */
  CombineFileSplit splitFor(FilePool inputDir, long bytes, int nLocs)
      throws IOException {
    final ArrayList<Path> paths = new ArrayList<Path>();
    final ArrayList<Long> start = new ArrayList<Long>();
    final ArrayList<Long> length = new ArrayList<Long>();
    final HashMap<String,Double> sb = new HashMap<String,Double>();
    do {
      paths.add(current.getPath());
      start.add(currentStart);
      final long fromFile = Math.min(bytes, current.getLen() - currentStart);
      length.add(fromFile);
      for (BlockLocation loc :
          inputDir.locationsFor(current, currentStart, fromFile)) {
        final double tedium = loc.getLength() / (1.0 * bytes);
        for (String l : loc.getHosts()) {
          Double j = sb.get(l);
          if (null == j) {
            sb.put(l, tedium);
          } else {
            sb.put(l, j.doubleValue() + tedium);
          }
        }
      }
      currentStart += fromFile;
      bytes -= fromFile;
      if (current.getLen() - currentStart == 0) {
        current = files.get(++idx % files.size());
        currentStart = 0;
      }
    } while (bytes > 0);
    final ArrayList<Entry<String,Double>> sort =
      new ArrayList<Entry<String,Double>>(sb.entrySet());
    Collections.sort(sort, hostRank);
    final String[] hosts = new String[Math.min(nLocs, sort.size())];
    for (int i = 0; i < nLocs && i < sort.size(); ++i) {
      hosts[i] = sort.get(i).getKey();
    }
    return new CombineFileSplit(paths.toArray(new Path[0]),
        toLongArray(start), toLongArray(length), hosts);
  }

  private long[] toLongArray(final ArrayList<Long> sigh) {
    final long[] ret = new long[sigh.size()];
    for (int i = 0; i < ret.length; ++i) {
      ret[i] = sigh.get(i);
    }
    return ret;
  }

  static final Comparator<Entry<String,Double>> hostRank =
    new Comparator<Entry<String,Double>>() {
      public int compare(Entry<String,Double> a, Entry<String,Double> b) {
          final double va = a.getValue();
          final double vb = b.getValue();
          return va > vb ? -1 : va < vb ? 1 : 0;
        }
    };
}
