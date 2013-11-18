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

package org.apache.spark.util

import java.util.{Random => JavaRandom}
import Utils.{TimesInt, intToTimesInt, timeIt}

class XORShiftRandom(init: Long) extends JavaRandom(init) {
  
  def this() = this(System.nanoTime)

  var seed = init
  
  // we need to just override next - this will be called by nextInt, nextDouble,
  // nextGaussian, nextLong, etc.
  override protected def next(bits: Int): Int = {
    
    var nextSeed = seed ^ (seed << 21)
    nextSeed ^= (nextSeed >>> 35)
    nextSeed ^= (nextSeed << 4)  
    seed = nextSeed
    (nextSeed & ((1L << bits) -1)).asInstanceOf[Int]
  }
}

object XORShiftRandom {

  def benchmark(numIters: Int) = {

    val seed = 1L
    val million = 1e6.toInt
    val javaRand = new JavaRandom(seed)
    val xorRand = new XORShiftRandom(seed)

    // warm up the JIT
    million.times {
      javaRand.nextInt
      xorRand.nextInt
    }

    /* Return results as a map instead of just printing to screen
       in case the user wants to do something with them */ 
    Map("javaTime" -> timeIt(javaRand.nextInt, numIters),
        "xorTime" -> timeIt(xorRand.nextInt, numIters))

  }

}