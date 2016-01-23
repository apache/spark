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

package org.apache.spark.util.sketch;

import java.io.InputStream;
import java.io.OutputStream;

/**
 * A Count-Min sketch is a probabilistic data structure used for summarizing streams of data in
 * sub-linear space.  Currently, supported data types include:
 * <ul>
 *   <li>{@link Byte}</li>
 *   <li>{@link Short}</li>
 *   <li>{@link Integer}</li>
 *   <li>{@link Long}</li>
 *   <li>{@link String}</li>
 * </ul>
 * Each {@link CountMinSketch} is initialized with a random seed, and a pair
 * of parameters:
 * <ol>
 *   <li>relative error (or {@code eps}), and
 *   <li>confidence (or {@code delta})
 * </ol>
 * Suppose you want to estimate the number of times an element {@code x} has appeared in a data
 * stream so far.  With probability {@code delta}, the estimate of this frequency is within the
 * range {@code true frequency <= estimate <= true frequency + eps * N}, where {@code N} is the
 * total count of items have appeared the the data stream so far.
 *
 * Under the cover, a {@link CountMinSketch} is essentially a two-dimensional {@code long} array
 * with depth {@code d} and width {@code w}, where
 * <ul>
 *   <li>{@code d = ceil(2 / eps)}</li>
 *   <li>{@code w = ceil(-log(1 - confidence) / log(2))}</li>
 * </ul>
 *
 * See http://www.eecs.harvard.edu/~michaelm/CS222/countmin.pdf for technical details,
 * including proofs of the estimates and error bounds used in this implementation.
 *
 * This implementation is largely based on the {@code CountMinSketch} class from stream-lib.
 */
abstract public class CountMinSketch {
  /**
   * Returns the relative error (or {@code eps}) of this {@link CountMinSketch}.
   */
  public abstract double relativeError();

  /**
   * Returns the confidence (or {@code delta}) of this {@link CountMinSketch}.
   */
  public abstract double confidence();

  /**
   * Depth of this {@link CountMinSketch}.
   */
  public abstract int depth();

  /**
   * Width of this {@link CountMinSketch}.
   */
  public abstract int width();

  /**
   * Total count of items added to this {@link CountMinSketch} so far.
   */
  public abstract long totalCount();

  /**
   * Adds 1 to {@code item}.
   */
  public abstract void add(Object item);

  /**
   * Adds {@code count} to {@code item}.
   */
  public abstract void add(Object item, long count);

  /**
   * Returns the estimated frequency of {@code item}.
   */
  public abstract long estimateCount(Object item);

  /**
   * Merges another {@link CountMinSketch} with this one in place.
   *
   * Note that only Count-Min sketches with the same {@code depth}, {@code width}, and random seed
   * can be merged.
   */
  public abstract CountMinSketch mergeInPlace(CountMinSketch other);

  /**
   * Writes out this {@link CountMinSketch} to an output stream in binary format.
   */
  public abstract void writeTo(OutputStream out);

  /**
   * Reads in a {@link CountMinSketch} from an input stream.
   */
  public static CountMinSketch readFrom(InputStream in) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  /**
   * Creates a {@link CountMinSketch} with given {@code depth}, {@code width}, and random
   * {@code seed}.
   */
  public static CountMinSketch create(int depth, int width, int seed) {
    return new CountMinSketchImpl(depth, width, seed);
  }

  /**
   * Creates a {@link CountMinSketch} with given relative error ({@code eps}), {@code confidence},
   * and random {@code seed}.
   */
  public static CountMinSketch create(double eps, double confidence, int seed) {
    return new CountMinSketchImpl(eps, confidence, seed);
  }
}
