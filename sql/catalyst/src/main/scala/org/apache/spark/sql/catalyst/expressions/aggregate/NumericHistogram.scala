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
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
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
    val iLeft = ctx.freshName("iLeft")
    val iRight = ctx.freshName("iRight")
    val iMerged = ctx.freshName("iMerged")
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
         |  int $iLeft = 0;
         |  int $iRight = 0;
         |  int $iMerged = 0;
         |
         |  while ($iLeft < $localLeft.numElements() && $iRight < $localRight.numElements())
         |  {
         |    if ($localLeft.getStruct($iLeft, 2).getDouble(0) <
         |      $localRight.getStruct($iRight, 2).getDouble(0))
         |    {
         |       $localResult[$iMerged] = $localLeft.getStruct($iLeft, 2);
         |       $iLeft++;
         |    }
         |    else
         |    {
         |       $localResult[$iMerged] = $localRight.getStruct($iRight, 2);
         |       $iRight++;
         |    }
         |    $iMerged++;
         |  }
         |  for(int $iterationIndex = $iLeft;
         |    $iterationIndex < $localLeft.numElements();
         |    $iterationIndex++) {
         |    $localResult[$iMerged + $iterationIndex - $iLeft] =
         |      $localLeft.getStruct($iterationIndex, 2);
         |  }
         |  for(int $iterationIndex = $iRight;
         |    $iterationIndex < $localRight.numElements();
         |    $iterationIndex++) {
         |    $localResult[$iMerged + $iterationIndex - $iRight] =
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
         |       InternalRow[] $localResult = new InternalRow[$localArray.length - 1];
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
         |           $localResult[$iterationIndex] = $tmpRow;
         |         } else if($iterationIndex == $minIndex + 1) {
         |           continue;
         |         } else if($iterationIndex < $minIndex) {
         |           $localResult[$iterationIndex] = $localArray[$iterationIndex];
         |         } else if($iterationIndex > $minIndex) {
         |           $localResult[$iterationIndex - 1] = $localArray[$iterationIndex];
         |         }
         |     }
         |       return $trimFunc($localResult, limit);
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