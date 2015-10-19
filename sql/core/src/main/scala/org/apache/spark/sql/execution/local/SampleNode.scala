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

package org.apache.spark.sql.execution.local

import org.apache.spark.sql.SQLConf
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.util.random.{BernoulliCellSampler, PoissonSampler}


/**
 * Sample the dataset.
 *
 * @param conf the SQLConf
 * @param lowerBound Lower-bound of the sampling probability (usually 0.0)
 * @param upperBound Upper-bound of the sampling probability. The expected fraction sampled
 *                   will be ub - lb.
 * @param withReplacement Whether to sample with replacement.
 * @param seed the random seed
 * @param child the LocalNode
 */
case class SampleNode(
    conf: SQLConf,
    lowerBound: Double,
    upperBound: Double,
    withReplacement: Boolean,
    seed: Long,
    child: LocalNode) extends UnaryLocalNode(conf) {

  override def output: Seq[Attribute] = child.output

  private[this] var iterator: Iterator[InternalRow] = _

  private[this] var currentRow: InternalRow = _

  override def open(): Unit = {
    child.open()
    val sampler =
      if (withReplacement) {
        // Disable gap sampling since the gap sampling method buffers two rows internally,
        // requiring us to copy the row, which is more expensive than the random number generator.
        new PoissonSampler[InternalRow](upperBound - lowerBound, useGapSamplingIfPossible = false)
      } else {
        new BernoulliCellSampler[InternalRow](lowerBound, upperBound)
      }
    sampler.setSeed(seed)
    iterator = sampler.sample(child.asIterator)
  }

  override def next(): Boolean = {
    if (iterator.hasNext) {
      currentRow = iterator.next()
      true
    } else {
      false
    }
  }

  override def fetch(): InternalRow = currentRow

  override def close(): Unit = child.close()

}
