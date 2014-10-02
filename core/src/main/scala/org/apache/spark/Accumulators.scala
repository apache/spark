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

import java.io.{ObjectInputStream, Serializable}

import scala.collection.generic.Growable
import scala.collection.mutable.Map
import scala.reflect.ClassTag

import org.apache.spark.serializer.JavaSerializer

/**
 * A data type that can be accumulated, ie has an commutative and associative "add" operation,
 * but where the result type, `R`, may be different from the element type being added, `T`.
 *
 * You must define how to add data, and how to merge two of these together.  For some data types,
 * such as a counter, these might be the same operation. In that case, you can use the simpler
 * [[org.apache.spark.Accumulator]]. They won't always be the same, though -- e.g., imagine you are
 * accumulating a set. You will add items to the set, and you will union two sets together.
 *
 * @param initialValue initial value of accumulator
 * @param param helper object defining how to add elements of type `R` and `T`
 * @param name human-readable name for use in Spark's web UI
 * @param sc the [[SparkContext]] that created this accumulable
 * @tparam R the full accumulated data (result type)
 * @tparam T partial data that can be added in
 */
class Accumulable[R, T] (
    @transient initialValue: R,
    param: AccumulableParam[R, T],
    val name: Option[String],
    @transient val sc: SparkContext)
  extends Serializable {

  def this(@transient initialValue: R, param: AccumulableParam[R, T], sc: SparkContext) =
    this(initialValue, param, None, sc)

  val id: Long = Accumulators.newId

  @transient private var value_ = initialValue // Current value on master
  val zero = param.zero(initialValue)  // Zero value to be passed to workers
  private var deserialized = false

  Accumulators.register(this, true)

  /**
   * Add more data to this accumulator / accumulable
   * @param term the data to add
   */
  def += (term: T) { value_ = param.addAccumulator(value_, term) }

  /**
   * Add more data to this accumulator / accumulable
   * @param term the data to add
   */
  def add(term: T) { value_ = param.addAccumulator(value_, term) }

  /**
   * Merge two accumulable objects together
   *
   * Normally, a user will not want to use this version, but will instead call `+=`.
   * @param term the other `R` that will get merged with this
   */
  def ++= (term: R) { value_ = param.addInPlace(value_, term)}

  /**
   * Merge two accumulable objects together
   *
   * Normally, a user will not want to use this version, but will instead call `add`.
   * @param term the other `R` that will get merged with this
   */
  def merge(term: R) { value_ = param.addInPlace(value_, term)}

  /**
   * Access the accumulator's current value; only allowed on master.
   */
  def value: R = {
    if (!deserialized) {
      value_
    } else {
      throw new UnsupportedOperationException("Can't read accumulator value in task")
    }
  }

  /**
   * Get the current value of this accumulator from within a task.
   *
   * This is NOT the global value of the accumulator.  To get the global value after a
   * completed operation on the dataset, call `value`.
   *
   * The typical use of this method is to directly mutate the local value, eg., to add
   * an element to a Set.
   */
  def localValue = value_

  /**
   * Set the accumulator's value; only allowed on master.
   */
  def value_= (newValue: R) {
    if (!deserialized) {
      value_ = newValue
    } else {
      throw new UnsupportedOperationException("Can't assign accumulator value in task")
    }
  }

  /**
   * Set the accumulator's value; only allowed on master
   */
  def setValue(newValue: R) {
    this.value = newValue
  }

  // Called by Java when deserializing an object
  private def readObject(in: ObjectInputStream) {
    in.defaultReadObject()
    value_ = zero
    deserialized = true
    Accumulators.register(this, false)
  }

  override def toString = if (value_ == null) "null" else value_.toString
}

/**
 * Helper object defining how to accumulate values of a particular type. An implicit
 * AccumulableParam needs to be available when you create [[Accumulable]]s of a specific type.
 *
 * @tparam R the full accumulated data (result type)
 * @tparam T partial data that can be added in
 */
trait AccumulableParam[R, T] extends Serializable {
  /**
   * Add additional data to the accumulator value. Is allowed to modify and return `r`
   * for efficiency (to avoid allocating objects).
   *
   * @param r the current value of the accumulator
   * @param t the data to be added to the accumulator
   * @return the new value of the accumulator
   */
  def addAccumulator(r: R, t: T): R

  /**
   * Merge two accumulated values together. Is allowed to modify and return the first value
   * for efficiency (to avoid allocating objects).
   *
   * @param r1 one set of accumulated data
   * @param r2 another set of accumulated data
   * @return both data sets merged together
   */
  def addInPlace(r1: R, r2: R): R

  /**
   * Return the "zero" (identity) value for an accumulator type, given its initial value. For
   * example, if R was a vector of N dimensions, this would return a vector of N zeroes.
   */
  def zero(initialValue: R): R
}

private[spark] class
GrowableAccumulableParam[R <% Growable[T] with TraversableOnce[T] with Serializable: ClassTag, T]
  extends AccumulableParam[R, T] {

  def addAccumulator(growable: R, elem: T): R = {
    growable += elem
    growable
  }

  def addInPlace(t1: R, t2: R): R = {
    t1 ++= t2
    t1
  }

  def zero(initialValue: R): R = {
    // We need to clone initialValue, but it's hard to specify that R should also be Cloneable.
    // Instead we'll serialize it to a buffer and load it back.
    val ser = new JavaSerializer(new SparkConf(false)).newInstance()
    val copy = ser.deserialize[R](ser.serialize(initialValue))
    copy.clear()   // In case it contained stuff
    copy
  }
}

/**
 * A simpler value of [[Accumulable]] where the result type being accumulated is the same
 * as the types of elements being merged, i.e. variables that are only "added" to through an
 * associative operation and can therefore be efficiently supported in parallel. They can be used
 * to implement counters (as in MapReduce) or sums. Spark natively supports accumulators of numeric
 * value types, and programmers can add support for new types.
 *
 * An accumulator is created from an initial value `v` by calling [[SparkContext#accumulator]].
 * Tasks running on the cluster can then add to it using the [[Accumulable#+=]] operator.
 * However, they cannot read its value. Only the driver program can read the accumulator's value,
 * using its value method.
 *
 * The interpreter session below shows an accumulator being used to add up the elements of an array:
 *
 * {{{
 * scala> val accum = sc.accumulator(0)
 * accum: spark.Accumulator[Int] = 0
 *
 * scala> sc.parallelize(Array(1, 2, 3, 4)).foreach(x => accum += x)
 * ...
 * 10/09/29 18:41:08 INFO SparkContext: Tasks finished in 0.317106 s
 *
 * scala> accum.value
 * res2: Int = 10
 * }}}
 *
 * @param initialValue initial value of accumulator
 * @param param helper object defining how to add elements of type `T`
 * @param sc the [[SparkContext]] that created this accumulable
 * @tparam T result type
 */
class Accumulator[T](@transient initialValue: T, param: AccumulatorParam[T], name: Option[String],
    @transient sc: SparkContext) extends Accumulable[T,T](initialValue, param, name, sc) {
  def this(initialValue: T, param: AccumulatorParam[T], sc: SparkContext) = this(initialValue,
      param, None, sc)
}

/**
 * A simpler version of [[org.apache.spark.AccumulableParam]] where the only data type you can add
 * in is the same type as the accumulated value. An implicit AccumulatorParam object needs to be
 * available when you create Accumulators of a specific type.
 *
 * @tparam T type of value to accumulate
 */
trait AccumulatorParam[T] extends AccumulableParam[T, T] {
  def addAccumulator(t1: T, t2: T): T = {
    addInPlace(t1, t2)
  }
}

/**
 * Provides the ability to look-up [[Accumulable]]s by name when performing operations on RDDs.
 *
 * For the correct Accumulable to be returned, the RDD used must have been created
 * by the same [[SparkContext]] as the Accumulable. It is not possible to create Accumulables
 * using one [[SparkContext]] and look them up when performing operations on an RDD created by
 * a different [[SparkContext]].
 *
 * Note that named Accumulables cannot be looked-up in the driver program; they can only be
 * obtained while an operation is being performed on an RDD (a task is executing).
 *
 * If multiple Accumulables have been created with the same name using the same [[SparkContext]]
 * then the one that was created most recently is returned.
 */
object AccumulableRegistry {
  /**
   * Returns the [[Accumulable]] with the specified name, if it exists.
   * @param name The name of the [[Accumulable]]
   * @return The accumulable with the specified name, or [[None]] if one does not exist,
   *         or if there is not a currently-executing task
   */
  def get(name: String): Option[Accumulable[_, _]] = {
    Accumulators.getLocalAccumulable(name)
  }
}

// TODO: The multi-thread support in accumulators is kind of lame; check
// if there's a more intuitive way of doing it right
private object Accumulators {
  // TODO: Use soft references? => need to make readObject work properly then
  val originals = Map[Long, Accumulable[_, _]]()
  val localAccums = Map[Thread, LocalAccumulables]()
  var lastId: Long = 0

  def newId: Long = synchronized {
    // Note that we currently rely on the current ID generation scheme (accumulables that are
    // created later have higher IDs) to ensure that we always get the most recently-created
    // accumulable when we look them up by name.
    lastId += 1
    lastId
  }

  def register(a: Accumulable[_, _], original: Boolean): Unit = synchronized {
    if (original) {
      originals(a.id) = a
    } else {
      val accums = localAccums.getOrElseUpdate(Thread.currentThread, new LocalAccumulables())
      accums.add(a)
    }
  }

  // Clear the local (non-original) accumulators for the current thread
  def clear() {
    synchronized {
      localAccums.remove(Thread.currentThread)
    }
  }

  // Get the values of the local accumulators for the current thread (by ID)
  def values: Map[Long, Any] = synchronized {
    val ret = Map[Long, Any]()
    for ((id, accum) <- localAccums.getOrElse(Thread.currentThread, new LocalAccumulables()).byId) {
      ret(id) = accum.localValue
    }
    return ret
  }

  // Get the local accumulable with the specified name
  def getLocalAccumulable(name: String): Option[Accumulable[_, _]] = {
    val accums = localAccums.getOrElseUpdate(Thread.currentThread, new LocalAccumulables())
    accums.byName.get(name)
  }

  // Add values to the original accumulators with some given IDs
  def add(values: Map[Long, Any]): Unit = synchronized {
    for ((id, value) <- values) {
      if (originals.contains(id)) {
        originals(id).asInstanceOf[Accumulable[Any, Any]] ++= value
      }
    }
  }

  def stringifyPartialValue(partialValue: Any) = "%s".format(partialValue)
  def stringifyValue(value: Any) = "%s".format(value)
}

private class LocalAccumulables(val byId: Map[Long, Accumulable[_, _]] = Map(),
                                val byName: Map[String, Accumulable[_, _]] = Map()) {
  def add(accumulable: Accumulable[_, _]) {
    // It is possible to have two accumulables with the same ID. This is caused when
    // a named accumulable is broadcast, but is also explicitly passed-in to a task.
    // In this case we want the explicitly passed-in version to replace the broadcast version,
    // since this is the version that the function references. Since explicitly passed accumulables
    // are deserialized after broadcast ones, we always replace accumulables with the same ID here.
    byId(accumulable.id) = accumulable
    accumulable.name.foreach(name => {
      val existing = byName.get(name)
      // If there are two accumulables with the same name, we want to use the one that was
      // defined later, which we assume is the one with the higher ID. Note that this relies
      // on the implementation of the ID generation for accumulables. We also need to make sure to
      // replace existing any accumulables with the same ID.
      if (!existing.isDefined || existing.get.id <= accumulable.id) {
        byName(name) = accumulable
      }
    })
  }

}
