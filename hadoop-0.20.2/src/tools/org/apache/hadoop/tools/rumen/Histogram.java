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
/**
 * 
 */
package org.apache.hadoop.tools.rumen;

import java.io.PrintStream;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

/**
 * {@link Histogram} represents an ordered summary of a sequence of {@code long}
 * s which can be queried to produce a discrete approximation of its cumulative
 * distribution function
 * 
 */
class Histogram implements Iterable<Map.Entry<Long, Long>> {
  private TreeMap<Long, Long> content = new TreeMap<Long, Long>();

  private String name;

  private long totalCount;

  public Histogram() {
    this("(anonymous)");
  }

  public Histogram(String name) {
    super();

    this.name = name;

    totalCount = 0L;
  }

  public void dump(PrintStream stream) {
    stream.print("dumping Histogram " + name + ":\n");

    Iterator<Map.Entry<Long, Long>> iter = iterator();

    while (iter.hasNext()) {
      Map.Entry<Long, Long> ent = iter.next();

      stream.print("val/count pair: " + (long) ent.getKey() + ", "
          + (long) ent.getValue() + "\n");
    }

    stream.print("*** end *** \n");
  }

  public Iterator<Map.Entry<Long, Long>> iterator() {
    return content.entrySet().iterator();
  }

  public long get(long key) {
    Long result = content.get(key);

    return result == null ? 0 : result;
  }

  public long getTotalCount() {
    return totalCount;
  }

  public void enter(long value) {
    Long existingValue = content.get(value);

    if (existingValue == null) {
      content.put(value, 1L);
    } else {
      content.put(value, existingValue + 1L);
    }

    ++totalCount;
  }

  /**
   * Produces a discrete approximation of the CDF. The user provides the points
   * on the {@code Y} axis he wants, and we give the corresponding points on the
   * {@code X} axis, plus the minimum and maximum from the data.
   * 
   * @param scale
   *          the denominator applied to every element of buckets. For example,
   *          if {@code scale} is {@code 1000}, a {@code buckets} element of 500
   *          will specify the median in that output slot.
   * @param buckets
   *          an array of int, all less than scale and each strictly greater
   *          than its predecessor if any. We don't check these requirements.
   * @return a {@code long[]}, with two more elements than {@code buckets} has.
   *         The first resp. last element is the minimum resp. maximum value
   *         that was ever {@code enter}ed. The rest of the elements correspond
   *         to the elements of {@code buckets} and carry the first element
   *         whose rank is no less than {@code #content elements * scale /
   *         bucket}.
   * 
   */
  public long[] getCDF(int scale, int[] buckets) {
    if (totalCount == 0) {
      return null;
    }

    long[] result = new long[buckets.length + 2];

    // fill in the min and the max
    result[0] = content.firstEntry().getKey();

    result[buckets.length + 1] = content.lastEntry().getKey();

    Iterator<Map.Entry<Long, Long>> iter = content.entrySet().iterator();
    long cumulativeCount = 0;
    int bucketCursor = 0;

    
    // Loop invariant: the item at buckets[bucketCursor] can still be reached
    // from iter, and the number of logged elements no longer available from
    // iter is cumulativeCount.
    // 
    // cumulativeCount/totalCount is therefore strictly less than
    // buckets[bucketCursor]/scale .
     
    while (iter.hasNext()) {
      long targetCumulativeCount = buckets[bucketCursor] * totalCount / scale;

      Map.Entry<Long, Long> elt = iter.next();

      cumulativeCount += elt.getValue();

      while (cumulativeCount >= targetCumulativeCount) {
        result[bucketCursor + 1] = elt.getKey();

        ++bucketCursor;

        if (bucketCursor < buckets.length) {
          targetCumulativeCount = buckets[bucketCursor] * totalCount / scale;
        } else {
          break;
        }
      }

      if (bucketCursor == buckets.length) {
        break;
      }
    }

    return result;
  }
}
