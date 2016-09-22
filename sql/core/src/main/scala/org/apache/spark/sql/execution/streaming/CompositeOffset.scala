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

package org.apache.spark.sql.execution.streaming

/**
 * An ordered collection of offsets, used to track the progress of processing data from one or more
 * [[Source]]s that are present in a streaming query. This is similar to simplified, single-instance
 * vector clock that must progress linearly forward.
 */
case class CompositeOffset(offsets: Seq[Option[Offset]]) extends Offset {
  /**
   * Returns a negative integer, zero, or a positive integer as this object is less than, equal to,
   * or greater than the specified object.
   */
  override def compareTo(other: Offset): Int = other match {
    case otherComposite: CompositeOffset if otherComposite.offsets.size == offsets.size =>
      val comparisons = offsets.zip(otherComposite.offsets).map {
        case (Some(a), Some(b)) => a compareTo b
        case (None, None) => 0
        case (None, _) => -1
        case (_, None) => 1
      }
      val nonZeroSigns = comparisons.map(sign).filter(_ != 0).toSet
      nonZeroSigns.size match {
        case 0 => 0                       // if both empty or only 0s
        case 1 => nonZeroSigns.head       // if there are only (0s and 1s) or (0s and -1s)
        case _ =>                         // there are both 1s and -1s
          throw new IllegalArgumentException(
            s"Invalid comparison between non-linear histories: $this <=> $other")
      }
    case _ =>
      throw new IllegalArgumentException(s"Cannot compare $this <=> $other")
  }

  private def sign(num: Int): Int = num match {
    case i if i < 0 => -1
    case i if i == 0 => 0
    case i if i > 0 => 1
  }

  /**
   * Unpacks an offset into [[StreamProgress]] by associating each offset with the order list of
   * sources.
   *
   * This method is typically used to associate a serialized offset with actual sources (which
   * cannot be serialized).
   */
  def toStreamProgress(sources: Seq[Source]): StreamProgress = {
    assert(sources.size == offsets.size)
    new StreamProgress ++ sources.zip(offsets).collect { case (s, Some(o)) => (s, o) }
  }

  override def toString: String =
    offsets.map(_.map(_.toString).getOrElse("-")).mkString("[", ", ", "]")
}

object CompositeOffset {
  /**
   * Returns a [[CompositeOffset]] with a variable sequence of offsets.
   * `nulls` in the sequence are converted to `None`s.
   */
  def fill(offsets: Offset*): CompositeOffset = {
    CompositeOffset(offsets.map(Option(_)))
  }
}
