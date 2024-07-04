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

package org.apache.spark.mllib.feature

import org.apache.spark.SparkFunSuite
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.mllib.util.TestingUtils._
import org.apache.spark.util.ArrayImplicits._

class PCASuite extends SparkFunSuite with MLlibTestSparkContext {

  private val data = Array(
    Vectors.sparse(5, Seq((1, 1.0), (3, 7.0))),
    Vectors.dense(2.0, 0.0, 3.0, 4.0, 5.0),
    Vectors.dense(4.0, 0.0, 0.0, 6.0, 7.0)
  )

  private lazy val dataRDD = sc.parallelize(data.toImmutableArraySeq, 2)

  test("Correct computing use a PCA wrapper") {
    val k = dataRDD.count().toInt
    val pca = new PCA(k).fit(dataRDD)

    val mat = new RowMatrix(dataRDD)
    val (pc, explainedVariance) = mat.computePrincipalComponentsAndExplainedVariance(k)

    val pca_transform = pca.transform(dataRDD).collect()
    val mat_multiply = mat.multiply(pc).rows.collect()

    pca_transform.zip(mat_multiply).foreach { case (calculated, expected) =>
      assert(calculated ~== expected relTol 1e-8)
    }
    assert(pca.explainedVariance ~== explainedVariance relTol 1e-8)
  }

  test("memory cost computation") {
    assert(PCAUtil.memoryCost(10, 100) < Int.MaxValue)
    // check overflowing
    assert(PCAUtil.memoryCost(40000, 60000) > Int.MaxValue)
  }

  test("number of features more than 65535") {
    val data1 = sc.parallelize(Seq(
      Vectors.dense(Array.fill(100000)(2.0)),
      Vectors.dense(Array.fill(100000)(0.0))
    ), 2)

    val pca = new PCA(2).fit(data1)
    // Eigen values should not be negative
    assert(pca.explainedVariance.values.forall(_ >= 0))
    // Norm of the principal component should be 1.0
    assert(Math.sqrt(pca.pc.values.slice(0, 100000)
      .map(Math.pow(_, 2)).sum) ~== 1.0 relTol 1e-8)
    // Leading explainedVariance is 1.0
    assert(pca.explainedVariance(0) ~== 1.0 relTol 1e-12)

    // Leading principal component is '1' vector
    val firstValue = pca.pc.values(0)
    pca.pc.values.slice(0, 100000).map(values =>
      assert(values ~== firstValue relTol 1e-12))
  }
}
