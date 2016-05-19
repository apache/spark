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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * A Count-min sketch is a probabilistic data structure used for summarizing streams of data in
 * sub-linear space.  Currently, supported data types include:
 * <ul>
 *   <li>{@link Byte}</li>
 *   <li>{@link Short}</li>
 *   <li>{@link Integer}</li>
 *   <li>{@link Long}</li>
 *   <li>{@link String}</li>
 * </ul>
 * A {@link CountMinSketch} is initialized with a random seed, and a pair of parameters:
 * <ol>
 *   <li>relative error (or {@code eps}), and
 *   <li>confidence (or {@code delta})
 * </ol>
 * Suppose you want to estimate the number of times an element {@code x} has appeared in a data
 * stream so far.  With probability {@code delta}, the estimate of this frequency is within the
 * range {@code true frequency <= estimate <= true frequency + eps * N}, where {@code N} is the
 * total count of items have appeared the data stream so far.
 *
 * Under the cover, a {@link CountMinSketch} is essentially a two-dimensional {@code long} array
 * with depth {@code d} and width {@code w}, where
 * <ul>
 *   <li>{@code d = ceil(2 / eps)}</li>
 *   <li>{@code w = ceil(-log(1 - confidence) / log(2))}</li>
 * </ul>
 *
 * This implementation is largely based on the {@code CountMinSketch} class from stream-lib.
 */
public abstract class CountMinSketch {

  public enum Version {
    /**
     * {@code CountMinSketch} binary format version 1.  All values written in big-endian order:
     * <ul>
     *   <li>Version number, always 1 (32 bit)</li>
     *   <li>Total count of added items (64 bit)</li>
     *   <li>Depth (32 bit)</li>
     *   <li>Width (32 bit)</li>
     *   <li>Hash functions (depth * 64 bit)</li>
     *   <li>
     *     Count table
     *     <ul>
     *       <li>Row 0 (width * 64 bit)</li>
     *       <li>Row 1 (width * 64 bit)</li>
     *       <li>...</li>
     *       <li>Row {@code depth - 1} (width * 64 bit)</li>
     *     </ul>
     *   </li>
     * </ul>
     */
    V1(1);

    private final int versionNumber;

    Version(int versionNumber) {
      this.versionNumber = versionNumber;
    }

    int getVersionNumber() {
      return versionNumber;
    }
  }

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
   * Increments {@code item}'s count by one.
   */
  public abstract void add(Object item);

  /**
   * Increments {@code item}'s count by {@code count}.
   */
  public abstract void add(Object item, long count);

  /**
   * Increments {@code item}'s count by one.
   */
  public abstract void addLong(long item);

  /**
   * Increments {@code item}'s count by {@code count}.
   */
  public abstract void addLong(long item, long count);

  /**
   * Increments {@code item}'s count by one.
   */
  public abstract void addString(String item);

  /**
   * Increments {@code item}'s count by {@code count}.
   */
  public abstract void addString(String item, long count);

  /**
   * Increments {@code item}'s count by one.
   */
  public abstract void addBinary(byte[] item);

  /**
   * Increments {@code item}'s count by {@code count}.
   */
  public abstract void addBinary(byte[] item, long count);

  /**
   * Returns the estimated frequency of {@code item}.
   */
  public abstract long estimateCount(Object item);

  /**
   * Merges another {@link CountMinSketch} with this one in place.
   *
   * Note that only Count-Min sketches with the same {@code depth}, {@code width}, and random seed
   * can be merged.
   *
   * @exception IncompatibleMergeException if the {@code other} {@link CountMinSketch} has
   *            incompatible depth, width, relative-error, confidence, or random seed.
   */
  public abstract CountMinSketch mergeInPlace(CountMinSketch other)
      throws IncompatibleMergeException;

  /**
   * Writes out this {@link CountMinSketch} to an output stream in binary format. It is the caller's
   * responsibility to close the stream.
   */
  public abstract void writeTo(OutputStream out) throws IOException;

  /**
   * Reads in a {@link CountMinSketch} from an input stream. It is the caller's responsibility to
   * close the stream.
   */
  public static CountMinSketch readFrom(InputStream in) throws IOException {
    return CountMinSketchImpl.readFrom(in);
  }

  /**
   * Creates a {@link CountMinSketch} with given {@code depth}, {@code width}, and random
   * {@code seed}.
   *
   * @param depth depth of the Count-min Sketch, must be positive
   * @param width width of the Count-min Sketch, must be positive
   * @param seed random seed
   */
  public static CountMinSketch create(int depth, int width, int seed) {
    return new CountMinSketchImpl(depth, width, seed);
  }

  /**
   * Creates a {@link CountMinSketch} with given relative error ({@code eps}), {@code confidence},
   * and random {@code seed}.
   *
   * @param eps relative error, must be positive
   * @param confidence confidence, must be positive and less than 1.0
   * @param seed random seed
   */
  public static CountMinSketch create(double eps, double confidence, int seed) {
    return new CountMinSketchImpl(eps, confidence, seed);
  }
}
