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
package org.apache.spark.mllib.nlp

import scala.collection.mutable.ArrayBuffer

private[mllib] class Node extends Serializable {
  var x: Int = 0
  var y: Int = 0
  var alpha: Double = 0.0
  var beta: Double = 0.0
  var cost: Double = 0.0
  var bestCost: Double = 0.0
  var prev: Node = _
  var fvector: Int = 0
  var fIdx: Int = 0
  var lpath: ArrayBuffer[Path] = new ArrayBuffer[Path]()
  var rpath: ArrayBuffer[Path] = new ArrayBuffer[Path]()
  val MINUS_LOG_EPSILON = 50
  var featureCache: ArrayBuffer[Int] = new ArrayBuffer[Int]()

  object Node {
    val node = new Node

    def getInstance: Node = {
      node
    }
  }

  def logsumexp(x: Double, y: Double, flg: Boolean): Double = {
    if (flg) return y
    val vMin: Double = math.min(x, y)
    val vMax: Double = math.max(x, y)
    if (vMax > vMin + MINUS_LOG_EPSILON) {
      vMax
    } else {
      vMax + math.log(math.exp(vMin - vMax) + 1.0)
    }
  }

  def calcAlpha(): Unit = {
    var i: Int = 0
    alpha = 0
    while (i < lpath.length) {
      alpha = logsumexp(alpha, lpath(i).cost + lpath(i).lnode.alpha, i == 0)
      i += 1
    }
    alpha += cost
  }

  def calcBeta(): Unit = {
    var i: Int = 0
    beta = 0
    while (i < rpath.length) {
      beta = logsumexp(beta, rpath(i).cost + rpath(i).rnode.beta, i == 0)
      i += 1
    }
    beta += cost
  }

  def calExpectation(expected: ArrayBuffer[Double], Z: Double, size: Int,
                     featureIdx: FeatureIndex): Unit = {
    val c: Double = math.exp(alpha + cost + beta - Z)
    var pathObj: Path = new Path()
    var idx: Int = featureIdx.getFeatureCacheIdx(fvector)
    var i: Int = 0
    featureCache = featureIdx.getFeatureCache()
    while (featureCache(idx) != -1) {
      expected.update(featureCache(idx) + y, c)
      idx += 1
    }
    while (i < lpath.length) {
      pathObj = lpath(i)
      pathObj.calExpectation(expected, Z, size, featureCache, featureIdx)
      i += 1
    }
  }

  def clear(): Unit = {
    x = 0
    y = 0
    alpha = 0
    beta = 0
    cost = 0
  }
}
