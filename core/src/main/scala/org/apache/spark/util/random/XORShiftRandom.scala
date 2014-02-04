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

package org.apache.spark.util.random

import java.util.{Random => JavaRandom}
import org.apache.spark.util.Utils.timeIt

/**
 * This class implements a XORShift random number generator algorithm 
 * Source:
 * Marsaglia, G. (2003). Xorshift RNGs. Journal of Statistical Software, Vol. 8, Issue 14.
 * @see <a href="http://www.jstatsoft.org/v08/i14/paper">Paper</a>
 * This implementation is approximately 3.5 times faster than
 * {@link java.util.Random java.util.Random}, partly because of the algorithm, but also due
 * to renouncing thread safety. JDK's implementation uses an AtomicLong seed, this class 
 * uses a regular Long. We can forgo thread safety since we use a new instance of the RNG
 * for each thread.
 */
private[spark] class XORShiftRandom(init: Long) extends JavaRandom(init) {
  
  def this() = this(System.nanoTime)

  private var seed = init
  
  // we need to just override next - this will be called by nextInt, nextDouble,
  // nextGaussian, nextLong, etc.
  override protected def next(bits: Int): Int = {    
    var nextSeed = seed ^ (seed << 21)
    nextSeed ^= (nextSeed >>> 35)
    nextSeed ^= (nextSeed << 4)  
    seed = nextSeed
    (nextSeed & ((1L << bits) -1)).asInstanceOf[Int]
  }

  override def setSeed(s: Long) {
    seed = s
  }
}

/** Contains benchmark method and main method to run benchmark of the RNG */
private[spark] object XORShiftRandom {

  /**
   * Main method for running benchmark
   * @param args takes one argument - the number of random numbers to generate
   */
  def main(args: Array[String]): Unit = {
    if (args.length != 1) {
      println("Benchmark of XORShiftRandom vis-a-vis java.util.Random")
      println("Usage: XORShiftRandom number_of_random_numbers_to_generate")
      System.exit(1)
    }
    println(benchmark(args(0).toInt))
  }

  /**
   * @param numIters Number of random numbers to generate while running the benchmark
   * @return Map of execution times for {@link java.util.Random java.util.Random}
   * and XORShift
   */
  def benchmark(numIters: Int) = {

    val seed = 1L
    val million = 1e6.toInt
    val javaRand = new JavaRandom(seed)
    val xorRand = new XORShiftRandom(seed)
    
    // this is just to warm up the JIT - we're not timing anything
    timeIt(1e6.toInt) {
      javaRand.nextInt()
      xorRand.nextInt()
    }

    val iters = timeIt(numIters)(_)
    
    /* Return results as a map instead of just printing to screen
    in case the user wants to do something with them */ 
    Map("javaTime" -> iters {javaRand.nextInt()},
        "xorTime" -> iters {xorRand.nextInt()})

  }

}
