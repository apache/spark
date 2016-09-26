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
import org.apache.spark.ml.param.{IntParam, Params, ParamValidators}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.Dataset

/**
 * Params for [[MinHash]].
 */
private[ml] trait MinHashModelParams extends Params {
  protected[this] val prime = 2038074743

  val numIndex: IntParam = new IntParam(this, "numIndex", "the number of index",
    ParamValidators.inRange(0, prime, lowerInclusive = false, upperInclusive = false))
}

class MinHashModel(override val uid: String, hashFunctions: Seq[Double => Double])
  extends LSHModel[Seq[Double], MinHashModel] with MinHashModelParams {

  override protected[this] val hashFunction: Seq[Double] => Vector = {
    elems: Seq[Double] =>
      Vectors.dense(hashFunctions.map(
        func => elems.map(func).min
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
  override protected[ml] def keyDistance(x: Seq[Double], y: Seq[Double]): Double = {
    val xSet = x.toSet
    val ySet = y.toSet
    1 - xSet.intersect(ySet).size.toDouble / xSet.union(ySet).size.toDouble
  }
}

/**
 * LSH class for Jaccard distance
 * @param uid
 */
class MinHash(override val uid: String) extends LSH[Seq[Double], MinHashModel]
  with MinHashModelParams {

  private[this] lazy val randSeq: Seq[Int] = {
    Seq.fill($(outputDim))(1 + Random.nextInt(prime - 1)).take($(outputDim))
  }

  private[this] lazy val hashFunctions: Seq[Double => Double] = {
    (0 until $(outputDim)).map {
      i: Int => {
        // Perfect Hash function, use 2n buckets to reduce collision.
        elem: Double => (1 + elem) * randSeq(i).toLong % prime % ($(numIndex) * 2)
      }
    }
  }

  def this() = {
    this(Identifiable.randomUID("min hash"))
  }

  override protected[this] def createRawLSHModel(dataset: Dataset[_]): MinHashModel = {
    new MinHashModel(uid, hashFunctions)
  }

  /** @group setParam */
  def setNumIndex(value: Int): this.type = set(numIndex, value)
}
