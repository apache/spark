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
 * A watermark is a monotonically increasing metric used to track progress in the computation of a
 * stream.  In addition to being comparable, a [[Watermark]] must have a notion of being empty
 * which is used to denote a stream where no processing has yet occured.
 */
trait Watermark {
  def isEmpty: Boolean

  def >(other: Watermark): Boolean

  def <(other: Watermark): Boolean
}

object LongWatermark {
  val empty = LongWatermark(-1)
}

case class LongWatermark(offset: Long) extends Watermark {
  def isEmpty: Boolean = offset == -1

  def >(other: Watermark): Boolean = other match {
    case l: LongWatermark => offset > l.offset
    case _ =>
      throw new IllegalArgumentException(s"Invalid comparison of $getClass with ${other.getClass}")
  }
  def <(other: Watermark): Boolean = other match {
    case l: LongWatermark => offset < l.offset
    case _ =>
      throw new IllegalArgumentException(s"Invalid comparison of $getClass with ${other.getClass}")
  }

  def +(increment: Long): LongWatermark = new LongWatermark(offset + increment)
  def -(decrement: Long): LongWatermark = new LongWatermark(offset - decrement)
}