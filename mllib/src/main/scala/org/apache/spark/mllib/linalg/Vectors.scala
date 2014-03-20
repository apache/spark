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

import java.lang.{Iterable => JavaIterable}

import scala.collection.JavaConverters._

import breeze.linalg.{Vector => BreezeVector, DenseVector => BreezeDenseVector,
  SparseVector => BreezeSparseVector}

/**
 * Represents a numeric vector, whose index type is Int and value type is Double.
 */
trait Vector extends Serializable {

  /**
   * Size of the vector.
   */
  def size: Int

  /**
   * Converts the instance to a breeze vector.
   */
  private[mllib] def toBreeze: BreezeVector[Double]
}

/**
 * Represents a vector with random access to its elements.
 *
 */
trait RandomAccessVector extends Vector {
  // empty
}

/**
 * Factory methods for [[org.apache.spark.mllib.linalg.Vector]].
 */
object Vectors {

  /** Creates a dense vector. */
  def dense(values: Array[Double]): Vector = new DenseVector(values)

  /**
   * Creates a sparse vector providing its index array and value array.
   *
   * @param size vector size.
   * @param indices index array, must be strictly increasing.
   * @param values value array, must have the same length as indices.
   */
  def sparse(size: Int, indices: Array[Int], values: Array[Double]): Vector =
    new SparseVector(size, indices, values)

  /**
   * Creates a sparse vector using unordered (index, value) pairs.
   *
   * @param size vector size.
   * @param elements vector elements in (index, value) pairs.
   */
  def sparse(size: Int, elements: Seq[(Int, Double)]): Vector = {

    require(size > 0)

    val (indices, values) = elements.sortBy(_._1).unzip
    var prev = -1
    indices.foreach { i =>
      require(prev < i, "Found duplicate indices: " + i)
      prev = i
    }
    require(prev < size)

    new SparseVector(size, indices.toArray, values.toArray)
  }

  /**
   * Creates a sparse vector using unordered (index, value) pairs.
   *
   * @param size vector size.
   * @param elements vector elements in (index, value) pairs.
   */
  def sparse(size: Int, elements: JavaIterable[(Int, Double)]): Vector =
    sparse(size, elements.asScala.toSeq)

  /**
   * Creates a vector instance from a breeze vector.
   */
  private[mllib] def fromBreeze(breezeVector: BreezeVector[Double]): Vector = {
    breezeVector match {
      case v: BreezeDenseVector[Double] => {
        require(v.offset == 0, s"Do not support non-zero offset ${v.offset}.")
        require(v.stride == 1, s"Do not support stride other than 1, but got ${v.stride}.")
        new DenseVector(v.data)
      }
      case v: BreezeSparseVector[Double] => {
        new SparseVector(v.length, v.index, v.data)
      }
      case v: BreezeVector[_] => {
        sys.error("Unsupported Breeze vector type: " + v.getClass.getName)
      }
    }
  }
}

/**
 * A dense vector represented by a value array.
 *
 * @param values
 */
class DenseVector(var values: Array[Double]) extends RandomAccessVector {

  override def size: Int = values.length

  override def toString = values.mkString("[", ",", "]")

  private[mllib] override def toBreeze = new BreezeDenseVector[Double](values)
}

/**
 * A sparse vector represented by an index array and an value array.
 *
 * @param n size of the vector.
 * @param indices index array, assume to be strictly increasing.
 * @param values value array, must have the same length as the index array.
 */
class SparseVector(var n: Int, var indices: Array[Int], var values: Array[Double]) extends Vector {

  override def size: Int = n

  override def toString = {
    "(" + n + "," + indices.zip(values).mkString("[", "," ,"]") + ")"
  }

  private[mllib] override def toBreeze = new BreezeSparseVector[Double](indices, values, n)
}
