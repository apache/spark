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

package org.apache.spark.mllib.linalg

import java.util.NoSuchElementException

import scala.collection.mutable

/**
 * Accumulates {@code double} values identified by indices. The size of the vector indicates how many values it can
 * store, though the sparse implementation does not necessarily store default values. The default value is normally
 * zero, but can be set to a different value when creating instances.
 * <p>
 * Operations are optimized as much as possible to allow random access (when getting or incrementing values) in O(1)
 * time, but this isn't guaranteed.
 */
sealed trait VectorBuilder extends Serializable {

  /**
   * The size of this accum vector. This is the capacity, but for sparse implementation the number of active indices is
   * expected to be fewer.
   *
   * @return the size
   */
  def size: Int

  /**
   * Returns the value at the given index, which is the default value (zero unless otherwise specified) if no value has
   * been explicitly set.
   *
   * @param index at which the value is retrieved
   * @return the value at the index, or the default value if no value has been set for this index
   * @throws IndexOutOfBoundsException if the index is not in [0, size)
   */
  def apply(index: Int): Double

  /**
   * Returns the value at the index `size - 1`, or the default value (zero unless otherwise specified) if no value has
   * been explicitly set at that index.
   *
   * @return the value at the index, or the default value if no value has been set for this index
   * @throws NoSuchElementException if the size is zero
   */
  def last: Double

  /**
   * Sets the value at the given index, replacing any value currently stored for that index.
   *
   * @param index at which the value is set
   * @param value to be set
   * @throws IndexOutOfBoundsException if the index is not in [0, size)
   */
  def set(index: Int, value: Double): Unit

  /**
   * Adds the value to the current value at the given index. If no value has been explicitly set for this index, it is
   * treated as zero (i.e. the value for this index will be set to {@code value}.
   *
   * @param index at which the value is added
   * @param value to be added to the current value at that index
   * @throws IndexOutOfBoundsException if the index is not in [0, size)
   */
  def add(index: Int, value: Double): Unit

  /**
   * Adds each active value from the other `VectorBuilder` to the value at the same index in this instance. Both instance
   * must have the same size.
   *
   * @param other containing values to be added to this instance
   * @throws IllegalArgumentException if the size of `other` is not equal to the size of `this`
   */
  def addAll(other: VectorBuilder): Unit = other.foreachActive((i, v) => this.add(i, v))

  /**
   * Creates a copy of this `VectorBuilder` with a size reduced by `n`. The values at the highest `n` indices are dropped,
   * regardless of whether they were active. If the original size is less than `n`, the new copy will have a size of 0.
   *
   * @param n the number of elements to drop; may not be negative
   * @return the new `VectorBuilder`
   * @throws IllegalArgumentException if `n < 0`
   */
  def dropRight(n: Int): VectorBuilder

  /**
   * Applies a function `f` to all the active elements of dense and sparse vector. This is not guaranteed to be done in
   * index order.
   * <p>
   * An "active entry" is an element which is explicitly stored, regardless of its value. Note that inactive entries
   * have the default value (zero unless otherwise specified).
   *
   * @param f the function takes two parameters where the first parameter is the index of
   *          the vector with type `Int`, and the second parameter is the corresponding value
   *          with type `Double`.
   */
  def foreachActive(f: (Int, Double) => Unit): Unit

  /**
   * Replaces each active value with the value computed by a function `f`. The function `f` is given the index and value
   * of each active element. This is not guaranteed to be done in index order.
   * <p>
   * An "active entry" is an element which is explicitly stored, regardless of its value. Note that inactive entries
   * have the default value (zero unless otherwise specified).
   *
   * @param f the function takes two parameters where the first parameter is the index of
   *          the vector with type `Int`, and the second parameter is the corresponding value
   *          with type `Double`. It returns the new value to be stored at the index.
   * @return `this`
   */
  def mapActive(f: (Int, Double) => Double): VectorBuilder = {
    this.foreachActive((i, v) => this.set(i, f(i, v)))
    return this
  }

  /**
   * Creates a new Spark ML vector with the same active values as this accum vector. The vector will be dense if this is
   * dense, sparse if this is sparse. The new vector contains a <i>copy</i> of the data.
   * <p>
   * WARNING: When the default value has been set to something other than zero (`0`), dense and sparse vectors will
   * exhibit slightly different behavior. Dense vectors will include the default value (since all values are active),
   * while sparse vectors will omit the default value and therefore these will be treated as zero in the vector.
   *
   * @return the new vector.
   */
  def toVector: Vector

  /**
   * Checks the index to ensure it is valid, throwing an exception if it is not.
   *
   * @param i the index to check
   * @throws IndexOutOfBoundsException if the index is not in [0, size)
   */
  protected final def checkIndex(i: Int): Unit = {
    if (i < 0) throw new IndexOutOfBoundsException(s"Invalid index (cannot be negative): $i")
    if (i >= size) throw new IndexOutOfBoundsException(s"Index $i exceeds size of $size")
  }

  override def clone(): VectorBuilder = throw new CloneNotSupportedException() // overridden in children
}

class DenseVectorBuilder(val size: Int, defaultValue: Double = 0) extends VectorBuilder {

  private val data = Array.fill[Double](size)(defaultValue)

  override def apply(index: Int): Double = { checkIndex(index); data(index) }

  override def last: Double = data.last

  override def set(index: Int, value: Double): Unit = { checkIndex(index); data(index) = value }

  override def add(index: Int, value: Double): Unit = { checkIndex(index); data(index) += value }

  override def dropRight(n: Int): VectorBuilder = {
    if (n < 0) throw new IllegalArgumentException(s"Number of elements to drop cannot be negative, was $n")
    if (n >= size) return new DenseVectorBuilder(0, defaultValue)

    val copy = new DenseVectorBuilder(size - n, defaultValue)
    val dataCopy = data.dropRight(n)
    for (i <- 0 until dataCopy.length) copy.set(i, dataCopy(i))
    return copy
  }

  override def foreachActive(f: (Int, Double) => Unit): Unit = for (i <- 0 until data.size) f(i, data(i))

  override def toVector: Vector = new DenseVector(data.clone())

  override def clone(): DenseVectorBuilder = {
    val clone = new DenseVectorBuilder(size, defaultValue)
    clone.addAll(this)
    return clone
  }
}

class SparseVectorBuilder(val size: Int, defaultValue: Double = 0) extends VectorBuilder {

  val data = new mutable.HashMap[Int, Double]()

  override def apply(index: Int): Double = { checkIndex(index); data.getOrElse(index, defaultValue) }

  override def last: Double = {
    if (size == 0) throw new NoSuchElementException

    data.getOrElse(size - 1, defaultValue)
  }

  override def set(index: Int, value: Double): Unit = { checkIndex(index); data.put(index, value) }

  override def add(index: Int, value: Double): Unit = { checkIndex(index); data.put(index, this(index) + value) }

  override def dropRight(n: Int): VectorBuilder = {
    if (n < 0) throw new IllegalArgumentException(s"Number of elements to drop cannot be negative, was $n")
    if (n >= size) return new SparseVectorBuilder(0, defaultValue)

    val copy = new SparseVectorBuilder(size - n, defaultValue)
    data.keySet.filter(_ < (size - n)).foreach(i => copy.set(i, data(i)))
    return copy
  }

  override def foreachActive(f: (Int, Double) => Unit): Unit = data.foreach { case (i, v) => f(i, v) }

  override def toVector: Vector = {
    val indices = data.keySet.toArray.sorted
    val values = Array.ofDim[Double](indices.size)
    for (i <- 0 until indices.size) values(i) = data(indices(i))
    return new SparseVector(size, indices, values)
  }

  override def clone(): SparseVectorBuilder = {
    val clone = new SparseVectorBuilder(size, defaultValue)
    clone.addAll(this)
    return clone
  }
}

object VectorBuilders {

  /**
   * Creates a new accum vector containing all the values from the given vector.
   *
   * @param v vector from which the values are copied
   * @return a new accum vector
   */
  def fromVector(v: Vector): VectorBuilder = {
    val av = v match {
      case dv: DenseVector => new DenseVectorBuilder(v.size)
      case sv: SparseVector => new SparseVectorBuilder(v.size)
    }
    v.foreachActive((i, v) => av.add(i, v))
    return av
  }

  def create(size: Int, sparse: Boolean, defaultValue: Double = 0): VectorBuilder = {
    if (sparse) new SparseVectorBuilder(size, defaultValue) else new DenseVectorBuilder(size, defaultValue)
  }
}