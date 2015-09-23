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

package org.apache.spark.mllib.clustering

import org.apache.spark.SparkFunSuite
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.mllib.util.TestingUtils._

class DpMeansSuite  extends SparkFunSuite with MLlibTestSparkContext {

  test("Single cluster") {
    val data = sc.parallelize(Array(
      Vectors.dense(0.1, 0.5, 0.7),
      Vectors.dense(0.2, 0.6, 0.8),
      Vectors.dense(0.3, 0.7, 0.9)
    ))
    val center = Vectors.dense(0.2, 0.6, 0.8)

    val model = new DpMeans().setLambdaValue(2.0).setConvergenceTol(1.0).run(data)
    assert(model.clusterCenters.head ~== center absTol 1E-5)
  }

  test("Two clusters") {
    val data = sc.parallelize(DpMeansSuite.data)
    val model = new DpMeans().setLambdaValue(12.0).setConvergenceTol(1.0).run(data)
    val predictedClusters = model.predict(data).collect()

    assert(predictedClusters(0) === predictedClusters(1))
    assert(predictedClusters(0) === predictedClusters(2))
    assert(predictedClusters(6) === predictedClusters(14))
    assert(predictedClusters(8) === predictedClusters(9))
    assert(predictedClusters(0) != predictedClusters(7))
  }

  test("Intra-cluster distance"){
    val data = sc.parallelize(DpMeansSuite.data)
    val lambdaVal = 12.0
    val model = new DpMeans().setLambdaValue(lambdaVal).setConvergenceTol(1.0).run(data)
    val norms = data.map(Vectors.norm(_, 2.0))
    val zippedData = data.zip(norms).map {
      case (v, norm) => new VectorWithNorm(v, norm)
    }
    val centers = model.clusterCenters.map { c =>
      new VectorWithNorm(c, Vectors.norm(c, 2))
    }
    val distanceInCluster = zippedData.map { pt =>
      val label = model.predict(pt.vector)
      DpMeans.squaredDistance(pt, centers(label))
    }.collect()
    distanceInCluster.foreach(i => assert(i < lambdaVal))
  }

  test("Single cluster with sparse data") {
    val n = 10000
    val data = sc.parallelize((1 to 100).flatMap { i =>
      val x = i / 1000.0
      Array(
        Vectors.sparse(n, Seq((0, 1.0 + x), (1, 2.0), (2, 6.0))),
        Vectors.sparse(n, Seq((0, 1.0 - x), (1, 2.0), (2, 6.0))),
        Vectors.sparse(n, Seq((0, 1.0), (1, 3.0 + x))),
        Vectors.sparse(n, Seq((0, 1.0), (1, 3.0 - x))),
        Vectors.sparse(n, Seq((0, 1.0), (1, 4.0), (2, 6.0 + x))),
        Vectors.sparse(n, Seq((0, 1.0), (1, 4.0), (2, 6.0 - x)))
      )
    }, 4)
    data.persist()

    val center = Vectors.sparse(n, Seq((0, 1.0), (1, 3.0), (2, 4.0)))

    val model = new DpMeans().setLambdaValue(40.0).setConvergenceTol(1.0).run(data)
    assert(model.clusterCenters.head == center)
  }

  object DpMeansSuite {

    val data = Array(
     Vectors.dense(-5.1971), Vectors.dense(-2.5359), Vectors.dense(-3.8220),
     Vectors.dense(-5.2211), Vectors.dense(-5.0602), Vectors.dense(-4.7118),
     Vectors.dense(6.8989), Vectors.dense(3.4592), Vectors.dense(4.6322),
     Vectors.dense(5.7048), Vectors.dense(4.6567), Vectors.dense(5.5026),
     Vectors.dense(4.5605), Vectors.dense(5.2043), Vectors.dense(6.2734)
    )

  }
}
