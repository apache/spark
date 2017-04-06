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

package org.apache.spark.ml.recommendation

import scala.collection.mutable.ArrayBuilder
import scala.reflect.ClassTag

/**
 * Mixin to hold the `RatingBlock` class, used by the `RatingBlocks` companion object.  It is pulled
 * out into this mixin simply to modularize the code into two files instead of one big one.
 */
private[recommendation] trait RatingBlockMixin {

  /**
   * A class holding ratings using primitive arrays.
   */
  case class RatingBlock[@specialized(Int, Long) ID: ClassTag](
      srcIds: Array[ID],
      dstIds: Array[ID],
      ratings: Array[Float]) {
    /** Size of the block. */
    def size: Int = srcIds.length
    require(dstIds.length == srcIds.length)
    require(ratings.length == srcIds.length)
  }

  object RatingBlock {

    /** Builder for [[RatingBlock]]. `ArrayBuilder` is used to avoid boxing/unboxing. */
    class Builder[@specialized(Int, Long) ID: ClassTag] extends Serializable {

      private val srcIds = ArrayBuilder.make[ID]
      private val dstIds = ArrayBuilder.make[ID]
      private val ratings = ArrayBuilder.make[Float]
      var size = 0

      /** Adds a rating. */
      def add(r: Rating[ID]): this.type = {
        size += 1
        srcIds += r.user
        dstIds += r.item
        ratings += r.rating
        this
      }

      /** Merges another [[RatingBlock.Builder]]. */
      def merge(other: RatingBlock[ID]): this.type = {
        size += other.srcIds.length
        srcIds ++= other.srcIds
        dstIds ++= other.dstIds
        ratings ++= other.ratings
        this
      }

      /** Builds a [[RatingBlock]]. */
      def build(): RatingBlock[ID] = {
        RatingBlock[ID](srcIds.result(), dstIds.result(), ratings.result())
      }
    }

  }

}
