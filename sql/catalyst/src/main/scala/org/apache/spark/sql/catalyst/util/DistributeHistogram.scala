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

package org.apache.spark.sql.catalyst.util

import java.nio.ByteBuffer
import java.util
import java.util.{Collections, Random}

import com.google.common.primitives.{Doubles, Ints}

/**
 * A generic, re-usable histogram class that supports partial aggregations.
 * The algorithm is a heuristic adapted from the following paper:
 * Yael Ben-Haim and Elad Tom-Tov, "A streaming parallel decision tree algorithm",
 * J. Machine Learning Research 11 (2010), pp. 849--872. Although there are no approximation
 * guarantees, it appears to work well with adequate data and a large (e.g., 20-80) number
 * of histogram bins.
 */
class DistributeHistogram(private var nBins: Int) {
  private var nUsedBins: Int = 0
  private var bins: util.ArrayList[Coord] = new util.ArrayList[Coord]()
  // init the RNG for breaking ties in histogram merging. A fixed seed is specified here
  // to aid testing, but can be eliminated to use a time-based seed (which would
  // make the algorithm non-deterministic).
  private val prng = new Random(31183)

  /**
   * Returns the number of bins.
   */
  def getNBins: Int = nBins

  /**
   * Set the number of bins.
   */
  def setNBins(nBins: Int): Unit = this.nBins = nBins

  /**
   * Returns the number of bins currently being used by the histogram.
   */
  def getUsedBins: Int = nUsedBins

  /**
   * Set the number of bins currently being used by the histogram.
   */
  def setUsedBins(nUsedBins: Int): Unit = this.nUsedBins = nUsedBins

  /**
   * Returns a particular histogram bin.
   */
  def getBins: util.ArrayList[Coord] = bins

  /**
   * Set a particular histogram bin.
   */
  def setBins(bins: Coord, site: Iny): Unit = this.bins = bins

  /**
   * Takes a serialized histogram created by the serialize() method and merges
   * it with the current histogram object.
   */
  def merge(other: DistributeHistogram): Unit = {
    if (other == null) {
      return
    }
    if (nBins == 0 || nUsedBins == 0) {
      // Our aggregation buffer has nothing in it, so just copy over 'other'
      // by deserializing the ArrayList of (x,y) pairs into an array of Coord objects
      nBins = other.nBins
      nUsedBins = other.getUsedBins
      bins = other.getBins
    } else {
      bins.addAll(other.getBins)
      Collections.sort(bins)
      // Now trim the overstuffed histogram down to the correct number of bins
      nUsedBins += other.getUsedBins
      trim()
    }
  }

  /**
   * Adds a new data point to the histogram approximation. This method implements Algorithm #1
   * from Ben-Haim and Tom-Tov, "A Streaming Parallel Decision Tree Algorithm", JMLR 2010.
   *
   * @param v The data point to add to the histogram approximation.
   */
  def add(v: Double): Unit = {
    // Binary search to find the closest bucket that v should go into.
    // 'bin' should be interpreted as the bin to shift right in order to accomodate
    // v. As a result, bin is in the range [0,N], where N means that the value v is
    // greater than all the N bins currently in the histogram. It is also possible that
    // a bucket centered at 'v' already exists, so this must be checked in the next step.
    var bin = 0
    var l = 0
    var r = nUsedBins
    var equal = false
    while (l < r && !equal) {
      bin = (l + r) / 2
      if (bins.get(bin).x > v) {
        r = bin
      } else if (bins.get(bin).x < v) {
        bin += 1;
        l = bin
      } else {
        equal = true
      }
    }
    // If we found an exact bin match for value v, then just increment that bin's count.
    // Otherwise, we need to insert a new bin and trim the resulting histogram back to size.
    // A possible optimization here might be to set some threshold under which 'v' is just
    // assumed to be equal to the closest bin -- if fabs(v-bins[bin].x) < THRESHOLD, then
    // just increment 'bin'. This is not done now because we don't want to make any
    // assumptions about the range of numeric data being analyzed.
    if (bin < nUsedBins && bins.get(bin).x == v) {
      bins.get(bin).y += 1
    } else {
      val newBin = Coord(v, 1)
      bins.add(bin, newBin)
      // Trim the bins down to the correct number of bins.
      nUsedBins += 1;
      if (nUsedBins > nBins) {
        trim()
      }
    }
  }

  /**
   * Trims a histogram down to 'nBins' bins by iteratively merging the closest bins.
   * If two pairs of bins are equally close to each other, decide uniformly at random which
   * pair to merge, based on a PRNG.
   */
  private def trim(): Unit = {
    while (nUsedBins > nBins) {
      // Find the closest pair of bins in terms of x coordinates. Break ties randomly.
      var smallestDiff = bins.get(1).x - bins.get(0).x
      var smallestDiffLoc = 0
      var smallestDiffCount = 1
      for (i <- 1 until nUsedBins - 1) {
        val diff = bins.get(i + 1).x - bins.get(i).x
        if (diff < smallestDiff) {
          smallestDiff = diff
          smallestDiffLoc = i
          smallestDiffCount = 1
        } else if (diff == smallestDiff && prng.nextDouble <= (1.0 / {
          smallestDiffCount += 1
          smallestDiffCount
        })) {
          smallestDiffLoc = i
        }
      }
      // Merge the two closest bins into their average x location, weighted by their heights.
      // The height of the new bin is the sum of the heights of the old bins.
      // double d = bins[smallestDiffLoc].y + bins[smallestDiffLoc+1].y;
      // bins[smallestDiffLoc].x *= bins[smallestDiffLoc].y / d;
      // bins[smallestDiffLoc].x += bins[smallestDiffLoc+1].x / d *
      //   bins[smallestDiffLoc+1].y;
      // bins[smallestDiffLoc].y = d;
      val d = bins.get(smallestDiffLoc).y + bins.get(smallestDiffLoc + 1).y
      val smallestDiffBin = bins.get(smallestDiffLoc)
      smallestDiffBin.x *= smallestDiffBin.y / d
      smallestDiffBin.x += bins.get(smallestDiffLoc + 1).x / d * bins.get(smallestDiffLoc + 1).y
      smallestDiffBin.y = d
      // Shift the remaining bins left one position
      bins.remove(smallestDiffLoc + 1)
      nUsedBins -= 1
    }
  }
}

/**
 * The Coord class defines a histogram bin, which is just an (x,y) pair.
 */
case class Coord(var x: Double, var y: Double) extends Comparable[Coord] {
  override def compareTo(other: Coord): Int = {
    java.lang.Double.compare(x, other.x)
  }
}



