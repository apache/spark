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
package org.apache.spark.ml.recommendation.logistic.pair.generator.w2v

import org.apache.spark.Partitioner
import org.apache.spark.ml.recommendation.logistic.pair.LongPair

private[ml] abstract class PairGenerator(private val sent: Iterator[Array[Long]],
                                         protected val partitioner1: Partitioner,
                                         protected val partitioner2: Partitioner
                                        ) extends Iterator[LongPair] with Serializable {
  assert(partitioner1.numPartitions == partitioner2.numPartitions)
  private var it: Iterator[LongPair] = Iterator.empty

  protected def generate(sent: Array[Long]): Iterator[LongPair]

  override def hasNext: Boolean = {
    while (!it.hasNext && sent.hasNext) {
      it = generate(sent.next)
    }

    it.hasNext
  }

  override def next(): LongPair = {
    while (!it.hasNext && sent.hasNext) {
      it = generate(sent.next)
    }

    if (it.hasNext) {
      return it.next
    }

    throw new NoSuchElementException("next on empty iterator")
  }
}