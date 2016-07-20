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
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeGenerator, CodegenContext, CodegenFallback, ExprCode}
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.dsl.expressions._

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
case class DeclarativeAggregateNumericHistogram(child: Expression, nb: Expression)
  extends DeclarativeAggregate
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
case class CodeGenNumericHistogram(child: Expression, nb: Expression) extends DeclarativeAggregate
  {

  override def children: Seq[Expression] = Seq(child)
  override def nullable: Boolean = false
  override def dataType: DataType = ArrayType(DoubleType)
  override def inputTypes: Seq[AbstractDataType] = Seq(DoubleType, IntegerType)

  val numOfBins = nb.eval(InternalRow.empty).asInstanceOf[Int]

  override val aggBufferAttributes = Seq.tabulate(2 * numOfBins)(
    (i) => {
      if (i % 2 == 0) {
        AttributeReference(s"x${i / 2}", DoubleType)()
      }
      else {
        AttributeReference(s"y${i / 2 }", DoubleType)()
      }
    })


  override val initialValues: Seq[Expression] = Seq.tabulate(2 * numOfBins)(
    (_) => Literal(null, DoubleType))


  override val updateExpressions: Seq[Expression] = {
    val sortedArray = SortHistograms(aggBufferAttributes, Seq(child, Literal(1d)), nb)
    val sortedElements = for ( i <- 0 until 2 * numOfBins) yield
      GetArrayItem(sortedArray, Literal(i))
    sortedElements

  }

  override val mergeExpressions: Seq[Expression] = {
    val sortedArray = SortHistograms(aggBufferAttributes.map(_.left),
      aggBufferAttributes.map(_.right), nb)
    val sortedElements =
      for ( i <- 0 until 2 * numOfBins) yield GetArrayItem(sortedArray, Literal(i))
    sortedElements
  }

  override val evaluateExpression: Expression = {
    CreateArray(aggBufferAttributes)
  }
}

/**
 * Merge two histograms into nb bins.
 */
@ExpressionDescription(
  usage = "_FUNC_(histogram1, histogram2, nb) - Returns an merged histogram with nb bins.")
case class SortHistograms(left: Seq[Expression],
                          right: Seq[Expression],
                          nb: Expression) extends Expression
{

  override def children: Seq[Expression] = left ++ right

  override def foldable: Boolean = children.forall(_.foldable)

  override def checkInputDataTypes(): TypeCheckResult = {
      TypeCheckResult.TypeCheckSuccess
  }

  override def dataType: DataType = {
    ArrayType(DoubleType)
  }

  override def nullable: Boolean = false

  override def eval(input: InternalRow): Any = {
    val leftList = left.map(_.eval(input)).grouped(2).
      map((s) => InternalRow(Seq(s(0), s(1)))).filterNot(_.isNullAt(0)).toList
    val rightList = right.map(_.eval(input)).grouped(2).
      map((s) => InternalRow(Seq(s(0), s(1)))).filterNot(_.isNullAt(0)).toList
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

  lazy val numOfBins = nb.eval(InternalRow.empty)

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val arrayClass = classOf[GenericArrayData].getName
    val values = ctx.freshName("values")
    val mergeFunc = ctx.freshName("merge")
    val tmpArray = ctx.freshName("tmpArray")
    ctx.addNewFunction(mergeFunc,
      s"""
        |private static void $mergeFunc(Object[] array,
        | Object[] workArray,
        | int leftStart,
        | int leftCount,
        | int rightStart,
        | int rightCount)
        |{
        |    int i = leftStart;
        |    int j = rightStart;
        |    int leftBound = leftStart + leftCount;
        |    int rightBound = rightStart + rightCount;
        |    int index = leftStart;
        |    while (i < leftBound || j < rightBound)
        |    {
        |        if (i < leftBound && j < rightBound)
        |        {
        |            if (array[i * 2] == null) {
        |              workArray[index * 2] = array[j * 2];
        |              workArray[index * 2 + 1] = array[j * 2 + 1];
        |              j++;
        |            }
        |            else if(array[j * 2] == null) {
        |              workArray[index * 2] = array[i * 2];
        |              workArray[index * 2 + 1] = array[i * 2 + 1];
        |              i++;
        |            }
        |            else {
        |              if (((Comparable)array[j * 2]).compareTo((Comparable)array[i * 2]) < 0) {
        |                workArray[index * 2] = array[j * 2];
        |                workArray[index * 2 + 1] = array[j * 2 + 1];
        |                j++;
        |              } else {
        |                workArray[index * 2] = array[i * 2];
        |                workArray[index * 2 + 1] = array[i * 2 + 1];
        |                i++;
        |              }
        |            }
        |        }
        |        else if (i < leftBound) {
        |            workArray[index * 2] = array[i * 2];
        |            workArray[index * 2 + 1] = array[i * 2 + 1];
        |            i++;
        |        } else {
        |            workArray[index * 2] = array[j * 2];
        |            workArray[index * 2 + 1] = array[j * 2 + 1];
        |            j++;
        |        }
        |        ++index;
        |    }
        |    for (i = leftStart; i < index * 2; ++i)
        |        array[i] = workArray[i];
        |}
      """.stripMargin.trim
    )
    val trimFunc = ctx.freshName("trim")
    val min = ctx.freshName("min")
    val minIndex = ctx.freshName("minIndex")
    val iterationIndex = ctx.freshName("i")
    val notNullCount = ctx.freshName("notNullCount")
    val localArray = ctx.freshName("localArray")
    val q1 = ctx.freshName("q1")
    val k1 = ctx.freshName("k1")
    val q2 = ctx.freshName("q2")
    val k2 = ctx.freshName("k2")
    ctx.addNewFunction(trimFunc,
      s"""
        | Object[] $trimFunc(Object[] $localArray, int limit) {
        |
        |     int $notNullCount = 0;
        |     for (int $iterationIndex = 0; $iterationIndex < $localArray.length / 2; $iterationIndex++) {
        |
        |       if($localArray[$iterationIndex * 2] != null)
        |          $notNullCount++;
        |     }
        |     if($notNullCount > limit) {
        |       Object[] result = new Object[$localArray.length];
        |       Double $min = Double.MAX_VALUE;
        |       int $minIndex = -1;
        |       for (int $iterationIndex = 0; $iterationIndex < ($localArray.length / 2 - 1); $iterationIndex++) {
        |         if($localArray[($iterationIndex + 1) * 2] == null || $localArray[$iterationIndex * 2] == null) {
        |           continue;
        |         }
        |         if((Double)$localArray[($iterationIndex + 1) * 2] - (Double)$localArray[($iterationIndex) * 2] < $min) {
        |           $min = (Double)$localArray[($iterationIndex + 1) * 2] - (Double)$localArray[$iterationIndex * 2];
        |           $minIndex = $iterationIndex;
        |         }
        |       }
        |       for(int $iterationIndex = 0; $iterationIndex < $localArray.length / 2; $iterationIndex++) {
        |         if($iterationIndex == $minIndex) {
        |           Double $q1 = (Double)$localArray[$iterationIndex * 2];
        |           Double $k1 = (Double)$localArray[$iterationIndex * 2 + 1];
        |           Double $q2 = (Double)$localArray[($iterationIndex + 1) * 2];
        |           Double $k2 = (Double)$localArray[($iterationIndex + 1) * 2 + 1];
        |           result[$iterationIndex * 2] = ($q1 * $k1 + $q2 * $k2) / ($k1 + $k2);
        |           result[$iterationIndex * 2 + 1] = $k1 + $k2;
        |         } else if($iterationIndex == $minIndex + 1) {
        |           continue;
        |         } else if($iterationIndex < $minIndex) {
        |           result[$iterationIndex * 2] = $localArray[$iterationIndex * 2];
        |           result[$iterationIndex * 2 + 1] = $localArray[$iterationIndex * 2 + 1];
        |         } else if( $iterationIndex > $minIndex) {
        |           result[($iterationIndex - 1) * 2] = $localArray[$iterationIndex * 2];
        |           result[($iterationIndex - 1) * 2 + 1] = $localArray[$iterationIndex * 2 + 1];
        |         }
        |     }
        |       return $trimFunc(result, limit);
        |     } else {
        |       return $localArray;
        |     }
        |
        | }
      """.stripMargin)
    ctx.addMutableState(s"Object[]", values, s"this.$values = null;")
    ctx.addMutableState(s"Object[]", tmpArray, s"this.$tmpArray = null;")

    ev.copy(code = s"""
      final boolean ${ev.isNull} = false;
      this.$values = new Object[${children.size}];""" +
      ctx.splitExpressions(
        ctx.INPUT_ROW,
        children.zipWithIndex.map { case (e, i) =>
          val eval = e.genCode(ctx)
          eval.code + s"""
            if (${eval.isNull}) {
              $values[$i] = null;
            } else {
              $values[$i] = ${eval.value};
            }
           """
        }) +
      s"""
        this.$tmpArray = new Object[${children.size}];
        this.$mergeFunc($values, $tmpArray, 0, ${left.size / 2}, ${left.size / 2}, ${right.size / 2});
        $values = $trimFunc($values, $numOfBins);
        final ArrayData ${ev.value} = new $arrayClass($values);
        this.$values = null;
        this.$tmpArray = null;
      """)
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
case class NumericHistogram(
                             child: Expression, nb: Expression) extends DeclarativeAggregate
{

  override def children: Seq[Expression] = Seq(child)
  override def nullable: Boolean = false
  override def dataType: DataType = ArrayType(
    StructType(Seq(
      StructField("x", DoubleType),
      StructField("y", DoubleType))))
  override def inputTypes: Seq[AbstractDataType] = Seq(DoubleType, IntegerType)

  val numOfBins = nb.eval(InternalRow.empty).asInstanceOf[Int]

  val histogram = AttributeReference("histogram",
    ArrayType(
      StructType(Seq(
        StructField("x", DoubleType),
        StructField("y", DoubleType)))))()

  override val aggBufferAttributes = Seq(histogram)


  override val initialValues: Seq[Expression] = Seq(
    CreateArray(Array.fill(0)(
      CreateStructUnsafe(Seq(
        Literal(0d),
        Literal(0d))))))


  override val updateExpressions: Seq[Expression] = {
    val sortedArray =
      CombineHistograms(histogram,
        CreateArray(Seq(CreateStruct(Seq(child, Literal(1d))))), nb)
    Seq(sortedArray)

  }

  override val mergeExpressions: Seq[Expression] = {
    val sortedArray =
      CombineHistograms(histogram.left, histogram.right, nb)
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
case class CombineHistograms(left: Expression,
                             right: Expression,
                             nb: Expression) extends Expression
{

  override def children: Seq[Expression] = left :: right :: nb :: Nil

  override def foldable: Boolean = children.forall(_.foldable)

  override def checkInputDataTypes(): TypeCheckResult = {
    if (nb.dataType != IntegerType) {
      TypeCheckResult.TypeCheckFailure("nb must be a integer")
    } else if (!left.dataType.isInstanceOf[ArrayType]
      || !left.dataType.asInstanceOf[ArrayType].elementType.isInstanceOf[StructType]
      || left.dataType.asInstanceOf[ArrayType].
      elementType.asInstanceOf[StructType](0).dataType != DoubleType
      || left.dataType.asInstanceOf[ArrayType].
      elementType.asInstanceOf[StructType](1).dataType != DoubleType
    ) {
      TypeCheckResult.TypeCheckFailure(
        "left must be an array of struct with one numeric field and one integer field")
    } else if (!right.dataType.isInstanceOf[ArrayType]
      || !right.dataType.asInstanceOf[ArrayType].elementType.isInstanceOf[StructType]
      || right.dataType.asInstanceOf[ArrayType].
      elementType.asInstanceOf[StructType](0).dataType != DoubleType
      || right.dataType.asInstanceOf[ArrayType].
      elementType.asInstanceOf[StructType](1).dataType != DoubleType
    ) {
      TypeCheckResult.TypeCheckFailure(
        "right must be an array of struct with one numeric field and one integer field")
    } else {
      TypeCheckResult.TypeCheckSuccess
    }
  }

  override def dataType: DataType = {
    ArrayType(DoubleType)
  }

  override def nullable: Boolean = false

  override def eval(input: InternalRow): Any = {
    val leftList = left.map(_.eval(input)).grouped(2).
      map((s) => InternalRow(Seq(s(0), s(1)))).filterNot(_.isNullAt(0)).toList
    val rightList = right.map(_.eval(input)).grouped(2).
      map((s) => InternalRow(Seq(s(0), s(1)))).filterNot(_.isNullAt(0)).toList
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

  lazy val numOfBins = nb.eval(InternalRow.empty)

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val arrayClass = classOf[GenericArrayData].getName
    val values = ctx.freshName("values")
    val mergeFunc = ctx.freshName("merge")
    val trimFunc = ctx.freshName("trim")
    val min = ctx.freshName("min")
    val minIndex = ctx.freshName("minIndex")
    val iterationIndex = ctx.freshName("i")
    val localArray = ctx.freshName("localArray")
    val localLeft = ctx.freshName("localLeft")
    val localRight = ctx.freshName("localRight")
    val localResult = ctx.freshName("localResult")
    val q1 = ctx.freshName("q1")
    val k1 = ctx.freshName("k1")
    val q2 = ctx.freshName("q2")
    val k2 = ctx.freshName("k2")
    val tmpRow = ctx.freshName("row")
    ctx.addNewFunction(mergeFunc,
      s"""
         |private static void $mergeFunc(ArrayData $localLeft,
         | ArrayData $localRight,
         | InternalRow[] $localResult)
         |{
         |  int iFirst = 0;
         |  int iSecond = 0;
         |  int iMerged = 0;
         |
         |  while (iFirst < $localLeft.numElements() && iSecond < $localRight.numElements())
         |  {
         |    if ($localLeft.getStruct(iFirst, 2).getDouble(0) <
         |      $localRight.getStruct(iSecond, 2).getDouble(0))
         |    {
         |       $localResult[iMerged] = $localLeft.getStruct(iFirst, 2);
         |       iFirst++;
         |    }
         |    else
         |    {
         |       $localResult[iMerged] = $localRight.getStruct(iSecond, 2);
         |       iSecond++;
         |    }
         |    iMerged++;
         |  }
         |  for(int $iterationIndex = iFirst;
         |    $iterationIndex < $localLeft.numElements();
         |    $iterationIndex++) {
         |    $localResult[iMerged + $iterationIndex - iFirst] =
         |      $localLeft.getStruct($iterationIndex, 2);
         |  }
         |  for(int $iterationIndex = iSecond;
         |    $iterationIndex < $localRight.numElements();
         |    $iterationIndex++) {
         |    $localResult[iMerged + $iterationIndex - iSecond] =
         |      $localRight.getStruct($iterationIndex, 2);
         |  }
         |}
      """.stripMargin.trim
    )
    ctx.addNewFunction(trimFunc,
      s"""
         | InternalRow[] $trimFunc(InternalRow[] $localArray, int limit) {
         |
         |     if($localArray.length > limit) {
         |       InternalRow[] result = new InternalRow[$localArray.length - 1];
         |       double $min = Double.MAX_VALUE;
         |       int $minIndex = 0;
         |       for (int $iterationIndex = 0;
         |         $iterationIndex < ($localArray.length - 1);
         |         $iterationIndex++) {
         |         if($localArray[$iterationIndex + 1].getDouble(0) -
         |           $localArray[$iterationIndex].getDouble(0) < $min) {
         |           $min = $localArray[$iterationIndex + 1].getDouble(0) -
         |             $localArray[$iterationIndex].getDouble(0);
         |           $minIndex = $iterationIndex;
         |         }
         |       }
         |       for(int $iterationIndex = 0;
         |         $iterationIndex < $localArray.length;
         |         $iterationIndex++) {
         |         if($iterationIndex == $minIndex) {
         |           double $q1 = $localArray[$iterationIndex].getDouble(0);
         |           double $k1 = $localArray[$iterationIndex].getDouble(1);
         |           double $q2 = $localArray[$iterationIndex + 1].getDouble(0);
         |           double $k2 = $localArray[$iterationIndex + 1].getDouble(1);
         |           InternalRow $tmpRow =
         |            new GenericInternalRow(
         |              new Object[]{($q1 * $k1 + $q2 * $k2) / ($k1 + $k2), $k1 + $k2});
         |           result[$iterationIndex] = $tmpRow;
         |         } else if($iterationIndex == $minIndex + 1) {
         |           continue;
         |         } else if($iterationIndex < $minIndex) {
         |           result[$iterationIndex] = $localArray[$iterationIndex];
         |         } else if($iterationIndex > $minIndex) {
         |           result[$iterationIndex - 1] = $localArray[$iterationIndex];
         |         }
         |     }
         |       return $trimFunc(result, limit);
         |     } else {
         |       return $localArray;
         |     }
         |
         | }
      """.stripMargin)
    ctx.addMutableState(s"InternalRow[]", values, s"this.$values = null;")
    val leftCode = left.genCode(ctx)
    val rightCode = right.genCode(ctx)
    val numOfBins = nb.genCode(ctx)

    ev.copy(code = s"""
      ${leftCode.code}
      ${rightCode.code}
      ${numOfBins.code}
      final boolean ${ev.isNull} = false;
      $values = new InternalRow[${leftCode.value}.numElements() + ${rightCode.value}.numElements()];
      this.$mergeFunc(${leftCode.value},
        ${rightCode.value},
        $values);
      $values = $trimFunc($values, ${numOfBins.value});
      final ArrayData ${ev.value} = new $arrayClass($values);
      this.$values = null;
      """)
  }
}