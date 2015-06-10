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

package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.TaskContext
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.types.{DataType, DoubleType}
import org.apache.spark.util.Utils
import org.apache.spark.util.random.XORShiftRandom

/**
 * A Random distribution generating expression.
 * TODO: This can be made generic to generate any type of random distribution, or any type of
 * StructType.
 *
 * Since this expression is stateful, it cannot be a case object.
 */
abstract class RDG(seed: Long) extends LeafExpression with Serializable {
  self: Product =>

  /**
   * Record ID within each partition. By being transient, the Random Number Generator is
   * reset every time we serialize and deserialize it.
   */
  @transient protected lazy val rng = new XORShiftRandom(seed + TaskContext.get().partitionId())

  override def deterministic: Boolean = false

  override def nullable: Boolean = false

  override def dataType: DataType = DoubleType
}

/** Generate a random column with i.i.d. uniformly distributed values in [0, 1). */
case class Rand(seed: Long) extends RDG(seed) {
  override def eval(input: Row): Double = rng.nextDouble()
}

object Rand {
  def apply(): Rand = apply(Utils.random.nextLong())

  def apply(seed: Expression): Rand = apply(seed match {
    case IntegerLiteral(s) => s
    case _ => throw new AnalysisException("Input argument to rand must be an integer literal.")
  })
}

/** Generate a random column with i.i.d. gaussian random distribution. */
case class Randn(seed: Long) extends RDG(seed) {
  override def eval(input: Row): Double = rng.nextGaussian()
}

object Randn {
  def apply(): Randn = apply(Utils.random.nextLong())

  def apply(seed: Expression): Randn = apply(seed match {
    case IntegerLiteral(s) => s
    case _ => throw new AnalysisException("Input argument to rand must be an integer literal.")
  })
}
