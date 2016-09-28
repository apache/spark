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

package org.apache.spark.ml.feature.lsh

import scala.util.Random

import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.util.Identifiable

class MinHashModel(override val uid: String, hashFunctions: Seq[Int => Long])
  extends LSHModel[MinHashModel] {

  override protected[this] val hashFunction: Vector => Vector = {
    elems: Vector =>
      Vectors.dense(hashFunctions.map(
        func => elems.toSparse.indices.toList.map(func).min.toDouble
      ).toArray)
  }

  /**
   * :: DeveloperApi ::
   *
   * Calculate the distance between two different keys using the distance metric corresponding
   * to the hashFunction
   *
   * @param x One of the point in the metric space
   * @param y Another the point in the metric space
   * @return The distance between x and y in double
   */
  override protected[ml] def keyDistance(x: Vector, y: Vector): Double = {
    val xSet = x.toSparse.indices.toSet
    val ySet = y.toSparse.indices.toSet
    1 - xSet.intersect(ySet).size.toDouble / xSet.union(ySet).size.toDouble
  }
}

/**
 * LSH class for Jaccard distance
 * The input set should be represented in sparse vector form. For example,
 *    Vectors.sparse(10, Array[(2, 1.0), (3, 1.0), (5, 1.0)])
 * means there are 10 elements in the space. This set contains elem 2, elem 3 and elem 5
 * @param uid
 */
class MinHash(override val uid: String) extends LSH[MinHashModel] {

  protected[this] val prime = 2038074743

  private[this] lazy val randSeq: Seq[Int] = {
    Seq.fill($(outputDim))(1 + Random.nextInt(prime - 1)).take($(outputDim))
  }

  def this() = {
    this(Identifiable.randomUID("min hash"))
  }

  override protected[this] def createRawLSHModel(inputDim: Int): MinHashModel = {
    val hashFunctions: Seq[Int => Long] = {
      (0 until $(outputDim)).map {
        i: Int => {
          // Perfect Hash function, use 2n buckets to reduce collision.
          elem: Int => (1 + elem) * randSeq(i).toLong % prime % (inputDim * 2)
        }
      }
    }
    new MinHashModel(uid, hashFunctions)
  }
}
