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

package org.apache.spark.ml.rdd

import scala.language.implicitConversions
import scala.reflect.ClassTag

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.ml.linalg.ElementwiseSlicing
import org.apache.spark.rdd.RDD
import org.apache.spark.util.Utils

/**
 * Machine learning specific RDD functions.
 */
@DeveloperApi
class RDDFunctions[T: ClassTag](self: RDD[T]) extends Serializable {

  /**
   * Reduce the slicing element of RDD.
   * @param numSlices the number of slice that element to be sliced
   */

  def sliceReduce(f: (T, T) => T, numSlices: Int)
                 (implicit slicing: ElementwiseSlicing[Option[T]]): T = {
    val cleanF = self.context.clean(f)
    val reducePartition: Iterator[T] => Option[T] = iter => {
      if (iter.hasNext) {
        Some(iter.reduceLeft(cleanF))
      } else {
        None
      }
    }
    val partiallyReduced = self.mapPartitions(it => Iterator(reducePartition(it)))

    val op: (Option[T], Option[T]) => Option[T] = (c, x) => {
      if (c.isDefined && x.isDefined) {
        Some(cleanF(c.get, x.get))
      } else if (c.isDefined) {
        c
      } else if (x.isDefined) {
        x
      } else {
        None
      }
    }

    RDDFunctions.fromRDD(partiallyReduced).sliceAggregate(Option.empty[T])(op, op, numSlices)
      .getOrElse(throw new UnsupportedOperationException("empty collection"))
  }

  /**
   * Aggregate the slicing element of RDD.
   *
   * @param zeroValue the zero value of aggregate
   * @param seqOp sequence operation between T(the element of RDD) and U
   * @param sliceCombOp combine operation between corresponding slices
   * @param numSlices the number of slice that element to be sliced
   * @param slicing implicit slicing value
   */
  def sliceAggregate[U: ClassTag](zeroValue: U)(
    seqOp: (U, T) => U,
    sliceCombOp: (U, U) => U,
    numSlices: Int)(implicit slicing: ElementwiseSlicing[U]): U = {

    require(numSlices > 0, s"sliceAggregate numSlices must greater than 0, but which is $numSlices")
    if (self.partitions.length == 0) {
      Utils.clone(zeroValue, self.context.env.closureSerializer.newInstance())
    } else if (numSlices == 1) {
      self.treeAggregate(zeroValue)(seqOp, sliceCombOp)
    } else {
      val cleanSeqOp = self.context.clean(seqOp)
      val cleanCombOp = self.context.clean(sliceCombOp)
      val aggregatePartition =
        (it: Iterator[T]) => it.foldLeft(zeroValue)(cleanSeqOp)
      val partiallyAggregated = self.mapPartitions(it => Iterator(aggregatePartition(it)))

      val slicesWithIndex = partiallyAggregated.flatMap { record =>
        var id = -1
        slicing.slice(record, numSlices).map { slice =>
          id += 1
          id -> slice
        }
      }.reduceByKey(cleanCombOp)
        .collect()

      val length = slicesWithIndex.length
      val slices = new Array[U](length)
      slicesWithIndex.foreach { case (index, slice) => slices(index) = slice }

      slicing.compose(slices.toIterator)
    }
  }
}

@DeveloperApi
object RDDFunctions {

  /** Implicit conversion from an RDD to RDDFunctions. */
  implicit def fromRDD[T: ClassTag](rdd: RDD[T]): RDDFunctions[T] = new RDDFunctions[T](rdd)

}
