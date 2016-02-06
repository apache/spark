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

package org.apache.spark

import scala.collection.mutable

/**
 * Structure for keeping track of the values being accumulated.
 * When collecting updates from the workers values are accumulated in pending hash map of
 * (rddId, splitId) -> T. When merging values on the driver the total is collected in value and
 * before merging in each pending record, checked against processed.
 */
private[spark] case class UpdateTracking[T](
    pending: mutable.HashMap[(Int, Int), T],
    processed: mutable.HashMap[Int, mutable.BitSet],
    var value: T) extends Serializable {

  def this(value: T) = {
    this(new mutable.HashMap[(Int, Int), T](), new mutable.HashMap[Int, mutable.BitSet](), value)
  }
}

/**
 * A consistent version of [[Accumulator]] where the result will not be added to
 * multiple times for the same partition/rdd.
 *
 * {{{
 * scala> val accum = sc.consistentAccumulator(0)
 * accum: spark.ConsistentAccumulator[Int] = 0
 *
 * scala> val data = Array(1, 2, 3, 4)
 * scala> val rdd = sc.parallelize(data).mapWithAccumulator{case (ui, x) => accum += (ui, x)}
 * scala> rdd.count()
 * scala> rdd.count()
 * ...
 * 10/09/29 18:41:08 INFO SparkContext: Tasks finished in 0.317106 s
 *
 * scala> accum.value
 * res2: Int = 10
 * }}}
 *
 * @param initialValue initial value of accumulator
 * @param param helper object defining how to add elements of type `T`
 * @tparam T result type
 */
class ConsistentAccumulator[T] private[spark] (
    @transient private[spark] val initialValue: UpdateTracking[T],
    param: ConsistentAccumulatorParam[T],
    name: Option[String],
    internal: Boolean)
  extends GenericAccumulable[T, UpdateTracking[T], T](
    Accumulators.newId(), initialValue, param, name, internal, countFailedValues = false,
    process = {x => x.value}, consistent = true) {

  def this(initialValue: T, param: AccumulatorParam[T], name: Option[String]) = {
    this(new UpdateTracking(initialValue),
      new ConsistentAccumulatorParam(param), name, false)
  }

  def this(initialValue: T, param: AccumulatorParam[T]) = {
    this(initialValue, param, None)
  }
}

/**
 * A consistent wrapper of [[org.apache.spark.AccumulatorParam]] where we keep track of
 * RDD/partitions which have already been processed
 *
 * @tparam T type of value to accumulate
 */
class ConsistentAccumulatorParam[T](accumulatorParam: AccumulatorParam[T])
    extends AccumulableParam[UpdateTracking[T], T] {

  /**
   * Add additional value to the current accumulator. No consistency checking is performed at this
   * stage as addAccumulator may be called multiple times inside the partition.
   *
   * @param r the current value of the accumulator
   * @param t the data to be added to the accumulator
   * @return the new value of the accumulator
   */
  def addAccumulator(r: UpdateTracking[T], t: T): UpdateTracking[T] = {
    val updateInfo = TaskContext.get().getRDDPartitionInfo()
    val v = r.pending.get(updateInfo).map(
      accumulatorParam.addAccumulator(_, t)
    ).getOrElse(t)
    r.pending(updateInfo) = v
    // We don't need to set the value here since we always have a merge with the zero UpdateTracking
    r
  }

  /**
   * Merge pending updates into the current accumulator.
   * Checks to make sure that each pending update has not
   * already been processed before updating.
   *
   * @param r1 local source of accumulated
   * @param r2 set pending updates
   * @return both data sets merged together
   */
  def addInPlace(r1: UpdateTracking[T], r2: UpdateTracking[T]): UpdateTracking[T] = {
    r2.pending.foreach{case ((rddId, splitId), v) =>
      val splits = r1.processed.getOrElseUpdate(rddId, new mutable.BitSet())
      if (!splits.contains(splitId)) {
        splits += splitId
        r1.value = accumulatorParam.addInPlace(r1.value, v)
      }
    }
    r1
  }

  def zero(initialValue: UpdateTracking[T]): UpdateTracking[T] = {
    new UpdateTracking(accumulatorParam.zero(initialValue.value))
  }
}
