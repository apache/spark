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

import com.clearspring.analytics.hash.MurmurHash
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._

case class Average(child: Expression) extends AlgebraicAggregate {

  override def children: Seq[Expression] = child :: Nil

  override def nullable: Boolean = true

  // Return data type.
  override def dataType: DataType = resultType

  // Expected input data type.
  // TODO: Right now, we replace old aggregate functions (based on AggregateExpression1) to the
  // new version at planning time (after analysis phase). For now, NullType is added at here
  // to make it resolved when we have cases like `select avg(null)`.
  // We can use our analyzer to cast NullType to the default data type of the NumericType once
  // we remove the old aggregate functions. Then, we will not need NullType at here.
  override def inputTypes: Seq[AbstractDataType] = Seq(TypeCollection(NumericType, NullType))

  private val resultType = child.dataType match {
    case DecimalType.Fixed(p, s) =>
      DecimalType.bounded(p + 4, s + 4)
    case _ => DoubleType
  }

  private val sumDataType = child.dataType match {
    case _ @ DecimalType.Fixed(p, s) => DecimalType.bounded(p + 10, s)
    case _ => DoubleType
  }

  private val currentSum = AttributeReference("currentSum", sumDataType)()
  private val currentCount = AttributeReference("currentCount", LongType)()

  override val bufferAttributes = currentSum :: currentCount :: Nil

  override val initialValues = Seq(
    /* currentSum = */ Cast(Literal(0), sumDataType),
    /* currentCount = */ Literal(0L)
  )

  override val updateExpressions = Seq(
    /* currentSum = */
    Add(
      currentSum,
      Coalesce(Cast(child, sumDataType) :: Cast(Literal(0), sumDataType) :: Nil)),
    /* currentCount = */ If(IsNull(child), currentCount, currentCount + 1L)
  )

  override val mergeExpressions = Seq(
    /* currentSum = */ currentSum.left + currentSum.right,
    /* currentCount = */ currentCount.left + currentCount.right
  )

  // If all input are nulls, currentCount will be 0 and we will get null after the division.
  override val evaluateExpression = child.dataType match {
    case DecimalType.Fixed(p, s) =>
      // increase the precision and scale to prevent precision loss
      val dt = DecimalType.bounded(p + 14, s + 4)
      Cast(Cast(currentSum, dt) / Cast(currentCount, dt), resultType)
    case _ =>
      Cast(currentSum, resultType) / Cast(currentCount, resultType)
  }
}

case class Count(child: Expression) extends AlgebraicAggregate {
  override def children: Seq[Expression] = child :: Nil

  override def nullable: Boolean = false

  // Return data type.
  override def dataType: DataType = LongType

  // Expected input data type.
  override def inputTypes: Seq[AbstractDataType] = Seq(AnyDataType)

  private val currentCount = AttributeReference("currentCount", LongType)()

  override val bufferAttributes = currentCount :: Nil

  override val initialValues = Seq(
    /* currentCount = */ Literal(0L)
  )

  override val updateExpressions = Seq(
    /* currentCount = */ If(IsNull(child), currentCount, currentCount + 1L)
  )

  override val mergeExpressions = Seq(
    /* currentCount = */ currentCount.left + currentCount.right
  )

  override val evaluateExpression = Cast(currentCount, LongType)
}

case class First(child: Expression) extends AlgebraicAggregate {

  override def children: Seq[Expression] = child :: Nil

  override def nullable: Boolean = true

  // First is not a deterministic function.
  override def deterministic: Boolean = false

  // Return data type.
  override def dataType: DataType = child.dataType

  // Expected input data type.
  override def inputTypes: Seq[AbstractDataType] = Seq(AnyDataType)

  private val first = AttributeReference("first", child.dataType)()

  override val bufferAttributes = first :: Nil

  override val initialValues = Seq(
    /* first = */ Literal.create(null, child.dataType)
  )

  override val updateExpressions = Seq(
    /* first = */ If(IsNull(first), child, first)
  )

  override val mergeExpressions = Seq(
    /* first = */ If(IsNull(first.left), first.right, first.left)
  )

  override val evaluateExpression = first
}

case class Last(child: Expression) extends AlgebraicAggregate {

  override def children: Seq[Expression] = child :: Nil

  override def nullable: Boolean = true

  // Last is not a deterministic function.
  override def deterministic: Boolean = false

  // Return data type.
  override def dataType: DataType = child.dataType

  // Expected input data type.
  override def inputTypes: Seq[AbstractDataType] = Seq(AnyDataType)

  private val last = AttributeReference("last", child.dataType)()

  override val bufferAttributes = last :: Nil

  override val initialValues = Seq(
    /* last = */ Literal.create(null, child.dataType)
  )

  override val updateExpressions = Seq(
    /* last = */ If(IsNull(child), last, child)
  )

  override val mergeExpressions = Seq(
    /* last = */ If(IsNull(last.right), last.left, last.right)
  )

  override val evaluateExpression = last
}

case class Max(child: Expression) extends AlgebraicAggregate {

  override def children: Seq[Expression] = child :: Nil

  override def nullable: Boolean = true

  // Return data type.
  override def dataType: DataType = child.dataType

  // Expected input data type.
  override def inputTypes: Seq[AbstractDataType] = Seq(AnyDataType)

  private val max = AttributeReference("max", child.dataType)()

  override val bufferAttributes = max :: Nil

  override val initialValues = Seq(
    /* max = */ Literal.create(null, child.dataType)
  )

  override val updateExpressions = Seq(
    /* max = */ If(IsNull(child), max, If(IsNull(max), child, Greatest(Seq(max, child))))
  )

  override val mergeExpressions = {
    val greatest = Greatest(Seq(max.left, max.right))
    Seq(
      /* max = */ If(IsNull(max.right), max.left, If(IsNull(max.left), max.right, greatest))
    )
  }

  override val evaluateExpression = max
}

case class Min(child: Expression) extends AlgebraicAggregate {

  override def children: Seq[Expression] = child :: Nil

  override def nullable: Boolean = true

  // Return data type.
  override def dataType: DataType = child.dataType

  // Expected input data type.
  override def inputTypes: Seq[AbstractDataType] = Seq(AnyDataType)

  private val min = AttributeReference("min", child.dataType)()

  override val bufferAttributes = min :: Nil

  override val initialValues = Seq(
    /* min = */ Literal.create(null, child.dataType)
  )

  override val updateExpressions = Seq(
    /* min = */ If(IsNull(child), min, If(IsNull(min), child, Least(Seq(min, child))))
  )

  override val mergeExpressions = {
    val least = Least(Seq(min.left, min.right))
    Seq(
      /* min = */ If(IsNull(min.right), min.left, If(IsNull(min.left), min.right, least))
    )
  }

  override val evaluateExpression = min
}

case class Sum(child: Expression) extends AlgebraicAggregate {

  override def children: Seq[Expression] = child :: Nil

  override def nullable: Boolean = true

  // Return data type.
  override def dataType: DataType = resultType

  // Expected input data type.
  // TODO: Right now, we replace old aggregate functions (based on AggregateExpression1) to the
  // new version at planning time (after analysis phase). For now, NullType is added at here
  // to make it resolved when we have cases like `select sum(null)`.
  // We can use our analyzer to cast NullType to the default data type of the NumericType once
  // we remove the old aggregate functions. Then, we will not need NullType at here.
  override def inputTypes: Seq[AbstractDataType] =
    Seq(TypeCollection(LongType, DoubleType, DecimalType, NullType))

  private val resultType = child.dataType match {
    case DecimalType.Fixed(precision, scale) =>
      DecimalType.bounded(precision + 10, scale)
    // TODO: Remove this line once we remove the NullType from inputTypes.
    case NullType => IntegerType
    case _ => child.dataType
  }

  private val sumDataType = resultType

  private val currentSum = AttributeReference("currentSum", sumDataType)()

  private val zero = Cast(Literal(0), sumDataType)

  override val bufferAttributes = currentSum :: Nil

  override val initialValues = Seq(
    /* currentSum = */ Literal.create(null, sumDataType)
  )

  override val updateExpressions = Seq(
    /* currentSum = */
    Coalesce(Seq(Add(Coalesce(Seq(currentSum, zero)), Cast(child, sumDataType)), currentSum))
  )

  override val mergeExpressions = {
    val add = Add(Coalesce(Seq(currentSum.left, zero)), Cast(currentSum.right, sumDataType))
    Seq(
      /* currentSum = */
      Coalesce(Seq(add, currentSum.left))
    )
  }

  override val evaluateExpression = Cast(currentSum, resultType)
}

/**
 * TODO
 *
 * Papers:
 * http://algo.inria.fr/flajolet/Publications/FlFuGaMe07.pdf
 * https://docs.google.com/document/d/1gyjfMHy43U9OWBXxfaeG-3MjGzejW1dlpyMwEYAAWEI/view?fullscreen#
 *
 * Note on provenance
 * - Clearspring:
 * - Aggregage Knowledge:
 * - Algebird:
 *
 * Note on naming: Tried to match the paper.
 *
 * Note on the use of longs for storage instead of something else.
 *
 * @param child
 * @param relativeSD
 */
case class HyperLogLog(child: Expression, relativeSD: Double = 0.05)
    extends AggregateFunction2 {
  /**
   * The size of a word used for storing registers.
   */
  private[this] val WORD_SIZE = java.lang.Long.SIZE

  /**
   * The number of bits that is required per register.
   *
   * This number is determined by the maximum number of leading binary zeros a hashcode can
   * produce. This is equal to the number of bits the hashcode returns. The current
   * implementation uses a 32-bit hashcode, this means 5-bits are (at most) needed to store the
   * number of leading zeros.
   *
   * One of the suggestions in the HyperLogLog++ is to use a 64-bit hashcode and to increase the
   * register size accordingly. This will be especially useful when cardinality will be larger
   * than 1E9.
   *
   * An interesting thought is that HHL always splits a hashcode into a bucket of p-bits and value
   * of (r-p)-bits. This means that the number of leading binary zeros can never reach the value
   * of 'r', and we therefore need fewer bits to store this value. Is this also in the HLL+ paper?
   */
  private[this] val REGISTER_SIZE = 5

  /**
   * Value used to mask a register stored in a word.
   */
  private[this] val REGISTER_WORD_MASK: Long = (1 << REGISTER_SIZE) - 1

  /**
   * The number of registers which can be stored in one word.
   */
  private[this] val REGISTERS_PER_WORD = WORD_SIZE / REGISTER_SIZE

  /**
   * The number of bits used for addressing.
   */
  private[this] val b = {
    val invRelativeSD = 1.106d / relativeSD
    (Math.log(invRelativeSD * invRelativeSD) / Math.log(2.0d)).toInt
  }

  /**
   * Shift used to extract the 'j' (the register) value from the hashed value.
   *
   * This assumes the use of 32-bit hashcodes.
   */
  private[this] val jShift = Integer.SIZE - b

  /**
   * Minimum 'w' value.
   */
  private[this] val wMin = 1 << (b - 1)

  /**
   * The number of registers used.
   */
  private[this] val m = 1 << b

  private[this] val alphaMM = b match {
    case 4 => 0.673d * m * m
    case 5 => 0.697d * m * m
    case 6 => 0.709d * m * m
    case _ => (0.7213d / (1.0d + 1.079d / m)) * m * m
  }

  /**
   * The number of words used to store the registers.
   */
  private[this] val numWords = m / REGISTERS_PER_WORD match {
    case x if m % REGISTERS_PER_WORD == 0 => x
    case x => x + 1
  }

  def children: Seq[Expression] = Seq(child)

  def nullable: Boolean = false

  def dataType: DataType = LongType

  def inputTypes: Seq[AbstractDataType] = Seq(AnyDataType)

  def bufferSchema: StructType = StructType.fromAttributes(bufferAttributes)

  def cloneBufferAttributes: Seq[Attribute] = bufferAttributes.map(_.newInstance())

  /** Allocate enough words to store all registers. */
  val bufferAttributes: Seq[AttributeReference] = Seq.tabulate(numWords) { i =>
    AttributeReference(s"MS[$i]", LongType)()
  }

  /** Fill all words with zeros. */
  def initialize(buffer: MutableRow): Unit = {
    var word = 0
    while (word < numWords) {
      buffer.setLong(mutableBufferOffset + word, 0)
      word += 1
    }
  }

  /**
   * Update the HLL buffer.
   *
   * Variable names in the paper match variable names in the code.
   */
  def update(buffer: MutableRow, input: InternalRow): Unit = {
    val v = child.eval(input)
    if (v != null) {
      // Create the hashed value 'x'.
      val x = MurmurHash.hash(v)

      // Determine which register 'j' we are going to use.
      val j = x >>> jShift

      // Determine the number of leading zeros in the remaining bits 'w'.
      val pw = Integer.numberOfLeadingZeros((x << b) | wMin) + 1L

      // Get the word containing the register we are interested in.
      val wordOffset = j / REGISTERS_PER_WORD
      val word = buffer.getLong(mutableBufferOffset + wordOffset)

      // Extract the M[J] register value from the word.
      val shift = REGISTER_SIZE * (j - (wordOffset * REGISTERS_PER_WORD))
      val mask = REGISTER_WORD_MASK << shift
      val Mj = (word & mask) >>> shift

      // Assign the maximum number of leading zeros to the register.
      if (pw > Mj) {
        buffer.setLong(mutableBufferOffset + wordOffset, (word & ~mask) | (pw << shift))
      }
    }
  }

  /**
   * Merge the HLL buffers by iterating through the registers in both buffers and select the
   * maximum number of leading zeros for each register.
   */
  def merge(buffer1: MutableRow, buffer2: InternalRow): Unit = {
    var j = 0
    var wordOffset = 0
    while (wordOffset < numWords) {
      val word1 = buffer1.getLong(mutableBufferOffset + wordOffset)
      val word2 = buffer2.getLong(inputBufferOffset + wordOffset)
      var word = 0L
      var i = 0
      var mask = REGISTER_WORD_MASK
      while (j < m && i < REGISTERS_PER_WORD) {
        word |= Math.max(word1 & mask, word2 & mask)
        mask <<= REGISTER_SIZE
        i += 1
        j += 1
      }
      buffer1.setLong(mutableBufferOffset + wordOffset, word)
      wordOffset += 1
    }
  }

  /**
   * Compute the HyperLogLog estimate.
   *
   * Variable names in the paper match variable names in the code.
   *
   * Contrary to the original paper we omit the large value correction. It has been proved that
   * this doesn't do any good (see the documentation of the ClearSpring HLL implementation).
   */
  def eval(buffer: InternalRow): Any = {
    // Compute the indicator value 'z' and count the number of zeros 'V'.
    var zInverse = 0.0d
    var V = 0.0d
    var j = 0
    var wordOffset = 0
    while (wordOffset < numWords) {
      val word = buffer.getLong(mutableBufferOffset + wordOffset)
      var i = 0
      var shift = 0
      while (j < m && i < REGISTERS_PER_WORD) {
        val Mj = (word >>> shift) & REGISTER_WORD_MASK
        zInverse += 1.0 / (1 << Mj)
        if (Mj == 0) {
          V += 1.0d
        }
        shift += REGISTER_SIZE
        i += 1
        j += 1
      }
      wordOffset += 1
    }
    val z = 1.0d / zInverse

    // Compute the raw HyperLogLog estimate.
    // This omits the large value correction.
    val estimate = alphaMM * z match {
      case e if e <= (5.0d / 2.0d) * m =>
        // Small value correction.
        m * Math.log(m / V)
      case e =>
        e
    }

    // Round to the nearest long value.
    Math.round(estimate)
  }
}
