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

import scala.util.Random

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.types._

/**
 * Computes an approximate histogram of a numerical column using a user-specified number of bins.
 *
 * The output is an array of (x,y) pairs as struct objects that represents the histogram's
 * bin centers and heights.
 *
 * Behavior:
 *  - null values are ignored
 *
 * References:
 *  -Yael Ben-Haim and Elad Tom-Tov.  "A streaming parallel decision tree algorithm",
 * J. Machine Learning Research 11 (2010), pp. 849--872
 *      http://www.jmlr.org/papers/volume11/ben-haim10a/ben-haim10a.pdf
 *
 * @param child to compute numeric histogram of.
 * @param nb number of bins
 */
@ExpressionDescription(
  usage = "_FUNC_(expr, nb) - " +
    "Returns the histogram of a numerical column using a user-specified number of bins.")
case class ImperativeNumericHistogram(child: Expression,
                                      nb: Expression,
                                      mutableAggBufferOffset: Int = 0,
                                      inputAggBufferOffset: Int = 0) extends ImperativeAggregate
{

  def this(child: Expression, nb: Expression) {
    this(child, nb, 0, 0)
  }

  override def children: Seq[Expression] = Seq(child)
  override def nullable: Boolean = false
  override def dataType: DataType = ArrayType(
    StructType(Seq(
      StructField("x", DoubleType, false),
      StructField("y", DoubleType, false))))
  override def inputTypes: Seq[AbstractDataType] = Seq(DoubleType, IntegerType)

  lazy val numOfBins = nb.eval(InternalRow.empty).asInstanceOf[Integer]

  override val aggBufferAttributes = (0 until 2 * numOfBins).
    map((i) => {
      if (i % 2 == 0) {
        AttributeReference(s"x${i / 2}", DoubleType)()
      }
      else {
        AttributeReference(s"y${i / 2 }", DoubleType)()
      }
    })

  override def aggBufferSchema: StructType = StructType.fromAttributes(aggBufferAttributes)

  override val inputAggBufferAttributes: Seq[AttributeReference] =
    aggBufferAttributes.map(_.newInstance())

  override def withNewMutableAggBufferOffset(
    newMutableAggBufferOffset: Int): ImperativeAggregate =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(
    newInputAggBufferOffset: Int): ImperativeAggregate =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override def initialize(buffer: MutableRow): Unit = {
    for (i <- 0 until numOfBins) {
      buffer.setNullAt(mutableAggBufferOffset + i)
    }
  }

  override def update(
   mutableAggBuffer: MutableRow,
   input: InternalRow): Unit = {
    val v = child.eval(input)
    val histogramBuffer = convertBufferToHistogram(mutableAggBuffer, mutableAggBufferOffset)
    val newElement = Seq(v.asInstanceOf[Double], 1d)
    val sortedHistogram = mergeSortedHistogram(histogramBuffer, List(newElement))
    val trimmedHistogram = trim(sortedHistogram, numOfBins)
    trimmedHistogram.zipWithIndex.foreach((e) => {
        val i = e._2
        val x = e._1(0)
        val y = e._1(1)
        mutableAggBuffer.setDouble(mutableAggBufferOffset + i * 2, x)
        mutableAggBuffer.setDouble(mutableAggBufferOffset + i * 2 + 1, y)
    })
  }

  override def merge(
    mutableAggBuffer: MutableRow,
    inputAggBuffer: InternalRow): Unit = {
    val histogramBuffer = convertBufferToHistogram(mutableAggBuffer, mutableAggBufferOffset)
    val histogramInput = convertBufferToHistogram(inputAggBuffer, inputAggBufferOffset)
    val sortedHistogram = mergeSortedHistogram(histogramBuffer, histogramInput)
    val trimmedHistogram = trim(sortedHistogram, numOfBins)
    trimmedHistogram.zipWithIndex.foreach((e) => {
      val i = e._2
      val x = e._1(0)
      val y = e._1(1)
      mutableAggBuffer.setDouble(mutableAggBufferOffset + i * 2, x)
      mutableAggBuffer.setDouble(mutableAggBufferOffset + i * 2 + 1, y)
    })
  }

  def convertBufferToHistogram(buffer: InternalRow, offset: Int): List[Seq[Double]] = {
    List.tabulate(numOfBins)(
      (i) => Seq(if (buffer.isNullAt(offset + i * 2)) null
      else buffer.getDouble(offset + i * 2)
        , if (buffer.isNullAt(offset + i * 2 + 1)) null
        else buffer.getDouble(offset + i * 2 + 1))).
      filterNot(_(0) == null).map(_.map(_.asInstanceOf[Double]))
  }

  def mergeSortedHistogram(xs: List[Seq[Double]], ys: List[Seq[Double]]): List[Seq[Double]] =
    (xs, ys) match {
      case(Nil, ys) => ys
      case(xs, Nil) => xs
      case(x :: xs1, y :: ys1) =>
        if (x.head < y.head) {x :: mergeSortedHistogram(xs1, ys)}
        else {y :: mergeSortedHistogram(xs, ys1)}
    }

  def trim(sortedList: List[Seq[Double]], limit: Int): List[Seq[Double]] = {
    if (sortedList.length > limit) {
      val leastDiffIndexs = sortedList.iterator.sliding(2).
        zipWithIndex.map((s) => {
          ((if (s._1.size < 2) s._1(0)(0) else s._1(1)(0))
              -
              s._1(0)(0),
            s._2)

      }).toList.groupBy(e => e._1)
        .minBy(_._1)._2.map(_._2)
      val leastDiffIndex = leastDiffIndexs(Random.nextInt(leastDiffIndexs.size))
      val trimmedHistogram =
        for ( e <- sortedList.zipWithIndex if e._2 != leastDiffIndex + 1) yield {
          if (e._2 == leastDiffIndex) {
            val q1 = e._1(0)
            val k1 = e._1(1)
            val q2 = sortedList(e._2 + 1)(0)
            val k2 = sortedList(e._2 + 1)(1)
            Seq((q1 * k1 + q2 * k2) / (k1 + k2), k1 + k2)
          } else {
            e._1
          }
        }
      trim(trimmedHistogram, limit)
    } else {
      sortedList
    }
  }

  override def eval(buffer: InternalRow): Any = {
    new GenericArrayData(Array.tabulate(numOfBins)((i) => {
      InternalRow(buffer.getDouble(mutableAggBufferOffset + i * 2),
        buffer.getDouble(mutableAggBufferOffset + i * 2 + 1))
    }))
  }
}



/**
 * Computes an approximate histogram of a numerical column using a user-specified number of bins.
 *
 * The output is an array of (x,y) pairs as struct objects that represents the histogram's
 * bin centers and heights.
 *
 * Behavior:
 *  - null values are ignored
 *
 * References:
 *  -Yael Ben-Haim and Elad Tom-Tov.  "A streaming parallel decision tree algorithm",
 * J. Machine Learning Research 11 (2010), pp. 849--872
 *      http://www.jmlr.org/papers/volume11/ben-haim10a/ben-haim10a.pdf
 *
 * @param child to compute numeric histogram of.
 * @param nb number of bins
 */
@ExpressionDescription(
  usage = "_FUNC_(expr, nb) - " +
    "Returns the histogram of a numerical column using a user-specified number of bins.")
case class NumericHistogram(child: Expression, nb: Expression) extends DeclarativeAggregate
  {

  override def children: Seq[Expression] = Seq(child)
  override def nullable: Boolean = false
  override def dataType: DataType = ArrayType(
    StructType(Seq(
      StructField("x", DoubleType, false),
      StructField("y", IntegerType, false))))
  override def inputTypes: Seq[AbstractDataType] = Seq(DoubleType, IntegerType)

  protected val histogram = AttributeReference("histogram",
    ArrayType(
      StructType(
        Seq(StructField("x", DoubleType),
          StructField("y", IntegerType)))))()
  override val aggBufferAttributes = Seq(histogram)

  override val initialValues: Seq[Expression] = Seq(
        CreateArray(
          Array.fill(0)(
            CreateNamedStructUnsafe(
              Seq(Literal("x"),
                Literal(null, DoubleType),
                Literal("y"),
                Literal(null, IntegerType))))))


  override val updateExpressions: Seq[Expression] = {
    val A = histogram
    val B = CreateArray(
      Seq(CreateNamedStructUnsafe(Seq(Literal("x"),
        child,
        Literal("y"),
        Literal(1)))))
    val sortedArray = MergeHistograms(A, B, nb)
    Seq(sortedArray)

  }

  override val mergeExpressions: Seq[Expression] = {
    val A = histogram.left
    val B = histogram.right
    val sortedArray = MergeHistograms(A, B, nb)
    Seq(sortedArray)
  }

  override val evaluateExpression: Expression = {
    histogram
  }
}

/**
 * Merge two histograms into nb bins.
 */
@ExpressionDescription(
  usage = "_FUNC_(histogram1, histogram2, nb) - Returns an merged histogram with nb bins.")
case class MergeHistograms(left: Expression,
                           right: Expression,
                           nb: Expression) extends BinaryExpression
  with CodegenFallback
{

  override def children: Seq[Expression] = Seq(left, right, nb)

  override def foldable: Boolean = children.forall(_.foldable)

  override def checkInputDataTypes(): TypeCheckResult = {
    if (nb.dataType != IntegerType) {
      TypeCheckResult.TypeCheckFailure("nb must be a integer")
    } else if (!left.dataType.isInstanceOf[ArrayType]
      || !left.dataType.asInstanceOf[ArrayType].elementType.isInstanceOf[StructType]
      || !left.dataType.asInstanceOf[ArrayType].
      elementType.asInstanceOf[StructType](0).dataType.isInstanceOf[NumericType]
      || left.dataType.asInstanceOf[ArrayType].
      elementType.asInstanceOf[StructType](1).dataType != IntegerType
    ) {
      TypeCheckResult.TypeCheckFailure(
        "left must be an array of struct with one numeric field and one integer field")
    } else if (!right.dataType.isInstanceOf[ArrayType]
      || !right.dataType.asInstanceOf[ArrayType].elementType.isInstanceOf[StructType]
      || !right.dataType.asInstanceOf[ArrayType].
      elementType.asInstanceOf[StructType](0).dataType.isInstanceOf[NumericType]
      || right.dataType.asInstanceOf[ArrayType].
      elementType.asInstanceOf[StructType](1).dataType != IntegerType
    ) {
      TypeCheckResult.TypeCheckFailure(
        "right must be an array of struct with one numeric field and one integer field")
    } else {
      TypeCheckResult.TypeCheckSuccess
    }
  }

  override def dataType: DataType = {
    ArrayType(
      StructType(Seq(StructField("x", DoubleType), StructField("y", IntegerType))),
      true)
  }

  override def nullable: Boolean = false

  override def eval(input: InternalRow): Any = {
    val leftList = left.eval(input).asInstanceOf[GenericArrayData].array.toList
    val rightList = right.eval(input).asInstanceOf[GenericArrayData].array.toList
    val numOfBins = nb.eval(input).asInstanceOf[Int]
    val mergedHistograms = mergeSortedHistogram(leftList, rightList)
    new GenericArrayData(
      trim(mergedHistograms, numOfBins))
  }

  def trim(sortedList: List[Any], limit: Int): List[Any] = {
    if (sortedList.length > limit) {
      val leastDiffIndexs = sortedList.iterator.sliding(2).
        zipWithIndex.map((s) => {
        ((if (s._1.size < 2) s._1(0).asInstanceOf[InternalRow].getDouble(0)
        else s._1(1).asInstanceOf[InternalRow].getDouble(0)) -
          s._1(0).asInstanceOf[InternalRow].getDouble(0), s._2)
      }).toList.groupBy(e => e._1)
        .minBy(_._1)._2.map(_._2)
      val leastDiffIndex = leastDiffIndexs(Random.nextInt(leastDiffIndexs.size))
      val trimmedHistogram =
        for ( e <- sortedList.zipWithIndex if e._2 != leastDiffIndex + 1) yield {
          if (e._2 == leastDiffIndex) {
            val q1 = e._1.asInstanceOf[InternalRow].getDouble(0)
            val k1 = e._1.asInstanceOf[InternalRow].getInt(1)
            val q2 = sortedList(e._2 + 1).asInstanceOf[InternalRow].getDouble(0)
            val k2 = sortedList(e._2 + 1).asInstanceOf[InternalRow].getInt(1)
            InternalRow((q1 * k1 + q2 * k2) / (k1 + k2), k1 + k2)
          } else {
            e._1
          }
        }
      trim(trimmedHistogram, limit)
    } else {
      sortedList
    }
  }

  def mergeSortedHistogram(xs: List[Any], ys: List[Any]): List[Any] =
    (xs, ys) match {
      case(Nil, ys) => ys
      case(xs, Nil) => xs
      case(x :: xs1, y :: ys1) =>
        if (x.asInstanceOf[InternalRow].getDouble(0) <
          y.asInstanceOf[InternalRow].getDouble(0)) {
          x :: mergeSortedHistogram(xs1, ys) }
        else y :: mergeSortedHistogram(xs, ys1)
    }

  override def prettyName: String = "numeric_histogram"
}

