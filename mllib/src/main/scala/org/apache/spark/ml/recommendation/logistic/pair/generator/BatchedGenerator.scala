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
package org.apache.spark.ml.recommendation.logistic.pair.generator

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.ml.recommendation.logistic.pair.{LongPair, LongPairMulti}



object BatchedGenerator {
  final private val TOTAL_BATCH_SIZE = 10000000

  def apply(pairGenerator: Iterator[LongPair],
            numPartitions: Int,
            withRating: Boolean): BatchedGenerator = {
    val l = Array.fill(numPartitions)(ArrayBuffer.empty[Long])
    val r = Array.fill(numPartitions)(ArrayBuffer.empty[Long])
    val w = if (withRating) {
      Array.fill(numPartitions)(ArrayBuffer.empty[Float])
    } else {
      null.asInstanceOf[Array[ArrayBuffer[Float]]]
    }

    new BatchedGenerator(pairGenerator, l, r, w, TOTAL_BATCH_SIZE / numPartitions)
  }
}

class BatchedGenerator(private val pairGenerator: Iterator[LongPair],
                       private val l: Array[ArrayBuffer[Long]],
                       private val r: Array[ArrayBuffer[Long]],
                       private val w: Array[ArrayBuffer[Float]],
                       private val batchSize: Int
                      ) extends Iterator[LongPairMulti] with Serializable {

  private var nonEmptyCounter = 0
  private var ptr = 0

  override def hasNext: Boolean = pairGenerator.hasNext || nonEmptyCounter > 0

  override def next: LongPairMulti = {
    while (pairGenerator.hasNext) {
      val pair = pairGenerator.next
      val part = pair.part

      if (l(part).isEmpty) {
        nonEmptyCounter += 1
      }

      l(part) += pair.left
      r(part) += pair.right
      if (w != null) w(part) += pair.rating

      if (l(part).size >= batchSize) {
        val result = LongPairMulti(part,
          l(part).toArray, r(part).toArray,
          if (w == null) null else w(part).toArray)

        l(part).clear
        r(part).clear
        if (w != null) w(part).clear

        nonEmptyCounter -= 1
        return result
      }
    }

    while (ptr < l.length && l(ptr).isEmpty) {
      ptr += 1
    }

    if (ptr < l.length) {
      val result = LongPairMulti(ptr,
        l(ptr).toArray, r(ptr).toArray,
        if (w == null) null else w(ptr).toArray)

      l(ptr).clear
      r(ptr).clear
      if (w != null) w(ptr).clear
      nonEmptyCounter -= 1
      return result
    }

    throw new NoSuchElementException("next on empty iterator")
  }
}