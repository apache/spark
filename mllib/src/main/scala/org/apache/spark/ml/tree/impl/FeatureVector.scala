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

package org.apache.spark.ml.tree.impl

import org.apache.spark.util.collection.{SortDataFormat, Sorter}

/**
 * Feature vector types are based on (feature type, representation).
 * The feature type can be continuous or categorical.
 *
 * Features are sorted by value, so we must store indices + values.
 * These values are currently stored in a dense representation only.
 * TODO: Support sparse storage (to optimize deeper levels of the tree), and maybe compressed
 *       storage (to optimize upper levels of the tree).
 *
 * @param featureArity  For categorical features, this gives the number of categories.
 *                      For continuous features, this should be set to 0.
 */
private[impl] class FeatureVector(
    val featureIndex: Int,
    val featureArity: Int,
    val values: Array[Int],
    val indices: Array[Int])
  extends Serializable {

  def isCategorical: Boolean = featureArity > 0

  /** For debugging */
  override def toString: String = {
    "  FeatureVector(" +
      s"    featureIndex: $featureIndex,\n" +
      s"    featureType: ${if (featureArity == 0) "Continuous" else "Categorical"},\n" +
      s"    featureArity: $featureArity,\n" +
      s"    values: ${values.mkString(", ")},\n" +
      s"    indices: ${indices.mkString(", ")},\n" +
      "  )"
  }

  def deepCopy(): FeatureVector =
    new FeatureVector(featureIndex, featureArity, values.clone(), indices.clone())

  override def equals(other: Any): Boolean = {
    other match {
      case o: FeatureVector =>
        featureIndex == o.featureIndex && featureArity == o.featureArity &&
          values.sameElements(o.values) && indices.sameElements(o.indices)
      case _ => false
    }
  }

  override def hashCode: Int = {
    com.google.common.base.Objects.hashCode(
      featureIndex: java.lang.Integer,
      featureArity: java.lang.Integer,
      values,
      indices)
    }
  }

private[impl] object FeatureVector {
  /** Store column sorted by feature values. */
  def fromOriginal(
      featureIndex: Int,
      featureArity: Int,
      values: Array[Int]): FeatureVector = {
    val indices = values.indices.toArray
    val fv = new FeatureVector(featureIndex, featureArity, values, indices)
    val sorter = new Sorter(new FeatureVectorSortByValue(featureIndex, featureArity))
    sorter.sort(fv, 0, values.length, Ordering[KeyWrapper])
    fv
  }
}

/**
 * Sort FeatureVector by values column; @see [[FeatureVector.fromOriginal()]]
 *
 * @param featureIndex @param featureArity Passed in so that, if a new
 *                     FeatureVector is allocated during sorting, that new object
 *                     also has the same featureIndex and featureArity
 */
private[impl] class FeatureVectorSortByValue(featureIndex: Int, featureArity: Int)
  extends SortDataFormat[KeyWrapper, FeatureVector] {

  override def newKey(): KeyWrapper = new KeyWrapper()

  override def getKey(data: FeatureVector, pos: Int, reuse: KeyWrapper): KeyWrapper = {
    if (reuse == null) {
      new KeyWrapper().setKey(data.values(pos))
    } else {
      reuse.setKey(data.values(pos))
    }
  }

  override def getKey(data: FeatureVector, pos: Int): KeyWrapper = {
    getKey(data, pos, null)
  }

  private def swapElements(data: Array[Double], pos0: Int, pos1: Int): Unit = {
    val tmp = data(pos0)
    data(pos0) = data(pos1)
    data(pos1) = tmp
  }

  private def swapElements(data: Array[Int], pos0: Int, pos1: Int): Unit = {
    val tmp = data(pos0)
    data(pos0) = data(pos1)
    data(pos1) = tmp
  }

  override def swap(data: FeatureVector, pos0: Int, pos1: Int): Unit = {
    swapElements(data.values, pos0, pos1)
    swapElements(data.indices, pos0, pos1)
  }

  override def copyRange(
      src: FeatureVector,
      srcPos: Int,
      dst: FeatureVector,
      dstPos: Int,
      length: Int): Unit = {
    System.arraycopy(src.values, srcPos, dst.values, dstPos, length)
    System.arraycopy(src.indices, srcPos, dst.indices, dstPos, length)
  }

  override def allocate(length: Int): FeatureVector = {
    new FeatureVector(featureIndex, featureArity,
      new Array[Int](length), new Array[Int](length))
  }

  override def copyElement(
      src: FeatureVector,
      srcPos: Int,
      dst: FeatureVector,
      dstPos: Int): Unit = {
    dst.values(dstPos) = src.values(srcPos)
    dst.indices(dstPos) = src.indices(srcPos)
  }
}
