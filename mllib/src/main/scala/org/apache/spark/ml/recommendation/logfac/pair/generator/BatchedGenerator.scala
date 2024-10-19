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
package org.apache.spark.ml.recommendation.logfac.pair.generator

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.ml.recommendation.logfac.pair.{LongPair, LongPairMulti}



private[ml] object BatchedGenerator {
  final private val TOTAL_BATCH_SIZE = 10000000

  def apply(pairGenerator: Iterator[LongPair],
            numPartitions: Int,
            withLabel: Boolean,
            withWeight: Boolean): BatchedGenerator = {
    val left = Array.fill(numPartitions)(ArrayBuffer.empty[Long])
    val right = Array.fill(numPartitions)(ArrayBuffer.empty[Long])
    val label = if (withLabel) {
      Array.fill(numPartitions)(ArrayBuffer.empty[Float])
    } else {
      null.asInstanceOf[Array[ArrayBuffer[Float]]]
    }
    val weight = if (withWeight) {
      Array.fill(numPartitions)(ArrayBuffer.empty[Float])
    } else {
      null.asInstanceOf[Array[ArrayBuffer[Float]]]
    }

    new BatchedGenerator(pairGenerator, left, right, label, weight,
      TOTAL_BATCH_SIZE / numPartitions)
  }
}

private[ml] class BatchedGenerator(private val pairGenerator: Iterator[LongPair],
                                   private val left: Array[ArrayBuffer[Long]],
                                   private val right: Array[ArrayBuffer[Long]],
                                   private val label: Array[ArrayBuffer[Float]],
                                   private val weight: Array[ArrayBuffer[Float]],
                                   private val batchSize: Int
                                  ) extends Iterator[LongPairMulti] with Serializable {

  private var nonEmptyCounter = 0
  private var ptr = 0

  override def hasNext: Boolean = pairGenerator.hasNext || nonEmptyCounter > 0

  override def next(): LongPairMulti = {
    while (pairGenerator.hasNext) {
      val pair = pairGenerator.next()
      val part = pair.part

      if (left(part).isEmpty) {
        nonEmptyCounter += 1
      }

      left(part) += pair.left
      right(part) += pair.right
      if (label != null) label(part) += pair.label
      if (weight != null) weight(part) += pair.weight

      if (left(part).size >= batchSize) {
        val result = LongPairMulti(part,
          left(part).toArray, right(part).toArray,
          if (label == null) null else label(part).toArray,
          if (weight == null) null else weight(part).toArray
        )

        left(part).clear()
        right(part).clear()
        if (label != null) label(part).clear()
        if (weight != null) weight(part).clear()

        nonEmptyCounter -= 1
        return result
      }
    }

    while (ptr < left.length && left(ptr).isEmpty) {
      ptr += 1
    }

    if (ptr < left.length) {
      val result = LongPairMulti(ptr,
        left(ptr).toArray, right(ptr).toArray,
        if (label == null) null else label(ptr).toArray,
        if (weight == null) null else weight(ptr).toArray
      )

      left(ptr).clear()
      right(ptr).clear()
      if (label != null) label(ptr).clear()
      if (weight != null) weight(ptr).clear()
      nonEmptyCounter -= 1
      return result
    }

    throw new NoSuchElementException("next on empty iterator")
  }
}
