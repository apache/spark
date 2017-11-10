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

package org.apache.spark.ml.feature

import scala.annotation.tailrec
import scala.collection.immutable.Queue

import org.apache.spark.annotation.Since
import org.apache.spark.ml.UnaryTransformer
import org.apache.spark.ml.param._
import org.apache.spark.ml.util._
import org.apache.spark.sql.types.{ArrayType, DataType, StringType}

/**
 * A feature transformer that converts the input array of strings into an array of n-grams. Null
 * values in the input array are ignored.
 * It returns an array of n-grams where each n-gram is represented by a space-separated string of
 * words.
 *
 * When the input is empty, an empty array is returned.
 * When the input array length is less than n (number of elements per n-gram), no n-grams are
 * returned.
 */
@Since("1.5.0")
class NGram @Since("1.5.0") (@Since("1.5.0") override val uid: String)
  extends UnaryTransformer[Seq[String], Seq[String], NGram] with DefaultParamsWritable {

  @Since("1.5.0")
  def this() = this(Identifiable.randomUID("ngram"))

  /**
   * Minimum n-gram length, greater than or equal to 1.
   * All values of m such that n <= m <= maxN will be used.
   * Default: 2, bigram features
   * @group param
   */
  @Since("1.5.0")
  val n: IntParam = new IntParam(this, "n", "minimum number of elements per n-gram (>=1)",
    ParamValidators.gtEq(1))

  /**
   * Maximum n-gram length, greater than or equal to `n`.
   * All values of m such that n <= m <= maxN will be used.
   * If maxN is not set, only n-grams with length on `n`
   * will be considered.
   * Default: 2, bigram features
   * @group param
   */
  @Since("which version?")
  val maxN: IntParam = new IntParam(this, "maxN", "maximum number elements per n-gram (>=n)",
    ParamValidators.gtEq(1))

  /** @group setParam */
  @Since("1.5.0")
  def setN(value: Int): this.type = set(n, value)

  /** @group getParam */
  @Since("1.5.0")
  def getN: Int = $(n)

  /** @group setParam */
  @Since("which version?")
  def setMaxN(value: Int): this.type = set(maxN, value)

  /** @group getParam */
  @Since("which version?")
  def getMaxN: Int = Math.max($(maxN), $(n))

  setDefault(n -> 2)
  setDefault(maxN -> 2)

  override protected def createTransformFunc: Seq[String] => Seq[String] = { input =>
    import NGram.multiSliding
    val (min, max) = ($(n), getMaxN)
    multiSliding(input, min, max).map(_.mkString(" "))
  }

  override protected def validateInputType(inputType: DataType): Unit = {
    require(inputType.sameType(ArrayType(StringType)),
      s"Input type must be ArrayType(StringType) but got $inputType.")
  }

  override protected def outputDataType: DataType = new ArrayType(StringType, false)

}

@Since("1.6.0")
object NGram extends DefaultParamsReadable[NGram] {

  @Since("1.6.0")
  override def load(path: String): NGram = super.load(path)

  /**
   * Calculates sliding windows over multiple length parameters simultaneously.
   * @param x    the input sequence over which sliding windows will be collected
   * @param min  the inclusive minimal length of the window to be collected
   * @param max  the inclusive maximal length of the window to be collected
   * @return     the collected windows
   *
   * @example {{{
   * multiSliding(1 to 5, min = 2, max = 4) == Seq(
   *   Seq(1, 2), Seq(1, 2, 3), Seq(1, 2, 3, 4),
   *   Seq(2, 3), Seq(2, 3, 4), Seq(2, 3, 4, 5),
   *   Seq(3, 4), Seq(3, 4, 5),
   *   Seq(4, 5)
   * )
   *
   * multiSliding(1 to 10, min = 2, max = 5) == Seq(
   *   Seq(1, 2), Seq(1, 2, 3), Seq(1, 2, 3, 4), Seq(1, 2, 3, 4, 5),
   *   Seq(2, 3), Seq(2, 3, 4), Seq(2, 3, 4, 5), Seq(2, 3, 4, 5, 6),
   *   Seq(3, 4), Seq(3, 4, 5), Seq(3, 4, 5, 6), Seq(3, 4, 5, 6, 7),
   *   Seq(4, 5), Seq(4, 5, 6), Seq(4, 5, 6, 7), Seq(4, 5, 6, 7, 8),
   *   Seq(5, 6), Seq(5, 6, 7), Seq(5, 6, 7, 8), Seq(5, 6, 7, 8, 9),
   *   Seq(6, 7), Seq(6, 7, 8), Seq(6, 7, 8, 9), Seq(6, 7, 8, 9, 10),
   *   Seq(7, 8), Seq(7, 8, 9), Seq(7, 8, 9, 10),
   *   Seq(8, 9), Seq(8, 9, 10),
   *   Seq(9, 10)
   * )
   * }}}
   */
  private[ml] def multiSliding[A](x: Seq[A], min: Int, max: Int): Seq[Seq[A]] = {
    type B = Seq[A]

    def addWindowsFromBuffer(acc: List[B], buffer: Queue[A]): List[B] = {
      buffer.drop(min - 1).foldLeft((acc, buffer.take(min - 1))) {
        case ((a, b), current) =>
          val newB = b.enqueue(current)
          (newB :: a, newB)
      }._1
    }

    @tailrec
    def addWindowsFromFinalBuffer(acc: List[B], buffer: Queue[A]): List[B] = {
      buffer.dequeueOption match {
        case Some((_, tail)) => addWindowsFromFinalBuffer(addWindowsFromBuffer(acc, tail), tail)
        case None => acc
      }
    }

    def calculateMultiSliding(): List[B] = {
      val (accumulated, finalBuffer) = x.foldLeft((List.empty[B], Queue.empty[A])) {

        case ((acc, buffer), current) if buffer.length < min - 1 =>
          (acc, buffer.enqueue(current))

        case ((acc, buffer), current) if buffer.length == min - 1 =>
          val newBuffer = buffer.enqueue(current)
          (newBuffer :: acc, newBuffer)

        case ((acc, buffer), current) if buffer.length >= min && buffer.length < max =>
          val newBuffer = buffer.enqueue(current)
          (newBuffer :: acc, newBuffer)

        case ((acc, buffer), current) if buffer.length == max =>
          val (_, newBuffer) = buffer.enqueue(current).dequeue
          (addWindowsFromBuffer(acc, newBuffer), newBuffer)

        case ((acc, buffer), _) if buffer.length > max =>
          (acc, buffer)

      }

      addWindowsFromFinalBuffer(accumulated, finalBuffer).reverse
    }

    def calculateSingleSliding(): Seq[B] = {
      x.iterator.sliding(min).withPartial(false).toSeq
    }

    ((1 <= min) && (min <= max), min == max) match {
      case (false, _) => Seq.empty
      case (true, true) => calculateSingleSliding()
      case (true, false) => calculateMultiSliding()
    }
  }

}
