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

package org.apache.spark.ml.extensions

import scala.annotation.tailrec
import scala.collection.immutable.Queue

import org.apache.spark.annotation.Since

package object seq {

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
  @Since("From which version?")
  def multiSliding[A](x: Seq[A], min: Int, max: Int): Seq[Seq[A]] = {
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

    (1 <= min) && (min <= max) match {
      case true => calculateMultiSliding()
      case false => Seq.empty
    }
  }

  @Since("From which version?")
  implicit class SeqOps[A](val x: Seq[A]) extends AnyVal {

    /**
     * Calculates sliding windows over multiple length parameters
     * simultaneously.
     * @see `org.apache.spark.ml.extensions.seq.multiSliding`
     */
    @Since("From which version?")
    def multiSliding[B](min: Int, max: Int): Seq[Seq[A]] =
      seq.multiSliding(x, min, max)

  }

}
