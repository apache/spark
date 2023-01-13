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

package org.apache.spark.sql.catalyst.expressions.aggregate

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.types._

// scalastyle:off
/**
 * HyperLogLog++ (HLL++) is a state of the art cardinality estimation algorithm. This class
 * implements the dense version of the HLL++ algorithm as an Aggregate Function.
 *
 * This implementation has been based on the following papers:
 * HyperLogLog: the analysis of a near-optimal cardinality estimation algorithm
 * http://algo.inria.fr/flajolet/Publications/FlFuGaMe07.pdf
 *
 * HyperLogLog in Practice: Algorithmic Engineering of a State of The Art Cardinality Estimation
 * Algorithm
 * http://static.googleusercontent.com/external_content/untrusted_dlcp/research.google.com/en/us/pubs/archive/40671.pdf
 *
 * Appendix to HyperLogLog in Practice: Algorithmic Engineering of a State of the Art Cardinality
 * Estimation Algorithm
 * https://docs.google.com/document/d/1gyjfMHy43U9OWBXxfaeG-3MjGzejW1dlpyMwEYAAWEI/view?fullscreen#
 *
 * @param child to estimate the cardinality of.
 * @param relativeSD the maximum relative standard deviation allowed.
 */
// scalastyle:on
@ExpressionDescription(
  usage = """
    _FUNC_(expr[, relativeSD]) - Returns the estimated cardinality by HyperLogLog++.
      `relativeSD` defines the maximum relative standard deviation allowed.""",
  examples = """
    Examples:
      > SELECT _FUNC_(col1) FROM VALUES (1), (1), (2), (2), (3) tab(col1);
       3
  """,
  group = "agg_funcs",
  since = "1.6.0")
case class HyperLogLogPlusPlus(
    child: Expression,
    relativeSD: Double = HyperLogLogPlusPlus.defaultRelativeSD,
    mutableAggBufferOffset: Int = 0,
    inputAggBufferOffset: Int = 0) extends HyperLogLogPlusPlusTrait {

  def this(child: Expression) = {
    this(
      child = child,
      relativeSD = HyperLogLogPlusPlus.defaultRelativeSD,
      mutableAggBufferOffset = 0,
      inputAggBufferOffset = 0
    )
  }

  def this(child: Expression, relativeSD: Expression) = {
    this(
      child = child,
      relativeSD = HyperLogLogPlusPlus.validateDoubleLiteral(relativeSD),
      mutableAggBufferOffset = 0,
      inputAggBufferOffset = 0
    )
  }

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): ImperativeAggregate =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): ImperativeAggregate =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override protected def withNewChildInternal(newChild: Expression): HyperLogLogPlusPlus =
    copy(child = newChild)

}

object HyperLogLogPlusPlus {
  val defaultRelativeSD: Double = 0.05
  def validateDoubleLiteral(exp: Expression): Double = exp match {
    case Literal(d: Double, DoubleType) => d
    case Literal(dec: Decimal, _) => dec.toDouble
    case _ =>
      throw QueryCompilationErrors.secondArgumentNotDoubleLiteralError
  }
}
