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
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeGenContext, GeneratedExpressionCode}
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
abstract class RDG extends LeafExpression with Nondeterministic {

  protected def seed: Long

  /**
   * Record ID within each partition. By being transient, the Random Number Generator is
   * reset every time we serialize and deserialize and initialize it.
   */
  @transient protected var rng: XORShiftRandom = _

  override protected def initInternal(): Unit = {
    rng = new XORShiftRandom(seed + TaskContext.getPartitionId)
  }

  override def nullable: Boolean = false

  override def dataType: DataType = DoubleType
}

/** Generate a random column with i.i.d. uniformly distributed values in [0, 1). */
case class Rand(seed: Long) extends RDG {
  override protected def evalInternal(input: InternalRow): Double = rng.nextDouble()

  def this() = this(Utils.random.nextLong())

  def this(seed: Expression) = this(seed match {
    case IntegerLiteral(s) => s
    case _ => throw new AnalysisException("Input argument to rand must be an integer literal.")
  })

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    val rngTerm = ctx.freshName("rng")
    val className = classOf[XORShiftRandom].getName
    ctx.addMutableState(className, rngTerm,
      s"$rngTerm = new $className(${seed}L + org.apache.spark.TaskContext.getPartitionId());")
    ev.isNull = "false"
    s"""
      final ${ctx.javaType(dataType)} ${ev.primitive} = $rngTerm.nextDouble();
    """
  }
}

/** Generate a random column with i.i.d. gaussian random distribution. */
case class Randn(seed: Long) extends RDG {
  override protected def evalInternal(input: InternalRow): Double = rng.nextGaussian()

  def this() = this(Utils.random.nextLong())

  def this(seed: Expression) = this(seed match {
    case IntegerLiteral(s) => s
    case _ => throw new AnalysisException("Input argument to rand must be an integer literal.")
  })

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    val rngTerm = ctx.freshName("rng")
    val className = classOf[XORShiftRandom].getName
    ctx.addMutableState(className, rngTerm,
      s"$rngTerm = new $className(${seed}L + org.apache.spark.TaskContext.getPartitionId());")
    ev.isNull = "false"
    s"""
      final ${ctx.javaType(dataType)} ${ev.primitive} = $rngTerm.nextGaussian();
    """
  }
}
