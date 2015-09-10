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

class PCASuite extends SparkFunSuite with MLlibTestSparkContext {

  private val data = Array(
    Vectors.sparse(5, Seq((1, 1.0), (3, 7.0))),
    Vectors.dense(2.0, 0.0, 3.0, 4.0, 5.0),
    Vectors.dense(4.0, 0.0, 0.0, 6.0, 7.0)
  )

  private lazy val dataRDD = sc.parallelize(data, 2)

  test("Correct computing use a PCA wrapper") {
    val k = dataRDD.count().toInt
    val pca = new PCA(k).fit(dataRDD)

    val mat = new RowMatrix(dataRDD)
    val pc = mat.computePrincipalComponents(k)

    val pca_transform = pca.transform(dataRDD).collect()
    val mat_multiply = mat.multiply(pc).rows.collect()

    assert(pca_transform.toSet === mat_multiply.toSet)
  }
}
