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

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.types._
import org.apache.spark.util.Utils
import org.apache.spark.util.random.XORShiftRandom

/**
 * A Random distribution generating expression.
 * TODO: This can be made generic to generate any type of random distribution, or any type of
 * StructType.
 *
 * Since this expression is stateful, it cannot be a case object.
 */
abstract class RDG extends UnaryExpression with ExpectsInputTypes with Nondeterministic {
  /**
   * Record ID within each partition. By being transient, the Random Number Generator is
   * reset every time we serialize and deserialize and initialize it.
   */
  @transient protected var rng: XORShiftRandom = _

  override protected def initializeInternal(partitionIndex: Int): Unit = {
    rng = new XORShiftRandom(seed + partitionIndex)
  }

  @transient protected lazy val seed: Long = child match {
    case Literal(s, IntegerType) => s.asInstanceOf[Int]
    case Literal(s, LongType) => s.asInstanceOf[Long]
    case _ => throw new AnalysisException(
      s"Input argument to $prettyName must be an integer, long or null literal.")
  }

  override def nullable: Boolean = false

  override def dataType: DataType = DoubleType

  override def inputTypes: Seq[AbstractDataType] = Seq(TypeCollection(IntegerType, LongType))
}

/** Generate a random column with i.i.d. uniformly distributed values in [0, 1). */
// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "_FUNC_([seed]) - Returns a random value with independent and identically distributed (i.i.d.) uniformly distributed values in [0, 1).",
  extended = """
    Examples:
      > SELECT _FUNC_();
       0.9629742951434543
      > SELECT _FUNC_(0);
       0.8446490682263027
      > SELECT _FUNC_(null);
       0.8446490682263027
  """)
// scalastyle:on line.size.limit
case class Rand(child: Expression) extends RDG {

  def this() = this(Literal(Utils.random.nextLong(), LongType))

  override protected def evalInternal(input: InternalRow): Double = rng.nextDouble()

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val rngTerm = ctx.freshName("rng")
    val className = classOf[XORShiftRandom].getName
    ctx.addMutableState(className, rngTerm, "")
    ctx.addPartitionInitializationStatement(
      s"$rngTerm = new $className(${seed}L + partitionIndex);")
    ev.copy(code = s"""
      final ${ctx.javaType(dataType)} ${ev.value} = $rngTerm.nextDouble();""", isNull = "false")
  }
}

object Rand {
  def apply(seed: Long): Rand = Rand(Literal(seed, LongType))
}

/** Generate a random column with i.i.d. values drawn from the standard normal distribution. */
// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "_FUNC_([seed]) - Returns a random value with independent and identically distributed (i.i.d.) values drawn from the standard normal distribution.",
  extended = """
    Examples:
      > SELECT _FUNC_();
       -0.3254147983080288
      > SELECT _FUNC_(0);
       1.1164209726833079
      > SELECT _FUNC_(null);
       1.1164209726833079
  """)
// scalastyle:on line.size.limit
case class Randn(child: Expression) extends RDG {

  def this() = this(Literal(Utils.random.nextLong(), LongType))

  override protected def evalInternal(input: InternalRow): Double = rng.nextGaussian()

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val rngTerm = ctx.freshName("rng")
    val className = classOf[XORShiftRandom].getName
    ctx.addMutableState(className, rngTerm, "")
    ctx.addPartitionInitializationStatement(
      s"$rngTerm = new $className(${seed}L + partitionIndex);")
    ev.copy(code = s"""
      final ${ctx.javaType(dataType)} ${ev.value} = $rngTerm.nextGaussian();""", isNull = "false")
  }
}

object Randn {
  def apply(seed: Long): Randn = Randn(Literal(seed, LongType))
}
