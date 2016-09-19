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

package org.apache.spark.ml.lsh

import scala.util.Random

import breeze.linalg.normalize

import org.apache.spark.ml.linalg.{BLAS, Vector, Vectors}
import org.apache.spark.ml.param.{DoubleParam, Params, ParamValidators}
import org.apache.spark.ml.util.Identifiable

/**
 * Params for [[RandomProjection]].
 */
private[ml] trait RandomProjectionParams extends Params {
  val bucketLength: DoubleParam = new DoubleParam(this, "bucketLength",
    "the length of each hash bucket", ParamValidators.gt(0))
}

class RandomProjectionModel(
    override val uid: String,
    val randUnitVectors: Array[Vector])
  extends LSHModel[Vector, RandomProjectionModel] with RandomProjectionParams {

  override protected[this] val hashFunction: (Vector) => Vector = {
    key: Vector => {
      val hashValues: Array[Double] = randUnitVectors.map({
        randUnitVector => Math.floor(BLAS.dot(key, randUnitVector) / $(bucketLength))
      })
      Vectors.dense(hashValues)
    }
  }

  override protected[ml] def keyDistance(x: Vector, y: Vector): Double = {
    Math.sqrt(Vectors.sqdist(x, y))
  }
}

class RandomProjection(override val uid: String) extends LSH[Vector, RandomProjectionModel]
  with RandomProjectionParams {

  private[this] var inputDim = -1

  private[this] lazy val randUnitVectors: Array[Vector] = {
    Array.fill($(outputDim)) {
      val randArray = Array.fill(inputDim)(Random.nextGaussian())
      Vectors.fromBreeze(normalize(breeze.linalg.Vector(randArray)))
    }
  }

  def this() = {
    this(Identifiable.randomUID("random projection"))
  }

  /** @group setParam */
  def setBucketLength(value: Double): this.type = set(bucketLength, value)

  override protected[this] def createRawLSHModel(inputDim: Int): RandomProjectionModel = {
    this.inputDim = inputDim
    new RandomProjectionModel(uid, randUnitVectors)
  }
}
