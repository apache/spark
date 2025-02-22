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

package org.apache.spark.ml

import scala.util.Random

import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.linalg._
import org.apache.spark.ml.util.MLTest
import org.apache.spark.util.SizeEstimator

// scalastyle:off println
class ModelSizeEstimationSuite extends MLTest {

  import testImplicits._

  test("estimate vector") {
    Seq(100, 1000, 10000, 100000).foreach { n =>
      val vec = Vectors.dense(Array.fill(n)(0.1))
      // This is used to estimate a vector, with the assumption that it is dense
      val size1 = Vectors.getDenseSize(n)
      // This is used to estimate a vector in a model
      val size2 = SizeEstimator.estimate(vec)

      // currently the Vectors.getDenseSize is slightly smaller than SizeEstimator.estimate
      // (n, size1, size2)
      // (100,808,832)
      // (1000,8008,8032)
      // (10000,80008,80032)
      // (100000,800008,800032)
      val rel = (size1 - size2).toDouble / size2
      assert(math.abs(rel) < 0.05, (n, size1, size2))
    }
  }

  test("estimate matrix") {
    Seq(10, 100, 1000).foreach { m =>
      Seq(10, 100, 1000).foreach { n =>
        val mat = Matrices.dense(m, n, Array.fill(m * n)(0.1))
        // This is used to estimate a matrix, with the assumption that it is dense
        val size1 = Matrices.getDenseSize(m, n)
        // This is used to estimate a matrix in a model
        val size2 = SizeEstimator.estimate(mat)

        // currently the Matrices.getDenseSize is slightly smaller than SizeEstimator.estimate
        // (m,n, size1)
        // (10,10,821,848)
        // (10,100,8021,8048)
        // (10,1000,80021,80048)
        // (100,10,8021,8048)
        // (100,100,80021,80048)
        // (100,1000,800021,800048)
        // (1000,10,80021,80048)
        // (1000,100,800021,800048)
        // (1000,1000,8000021,8000048)
        println((m, n, size1, size2))
        val rel = (size1 - size2).toDouble / size2
        assert(math.abs(rel) < 0.05, (m, n, size1, size2))
      }
    }
  }

  test("test binary logistic regression: dense") {
    val rng = new Random(1)

    Seq(10, 100, 1000, 10000, 100000).foreach { n =>
      val df = Seq(
        (Vectors.dense(Array.fill(n)(rng.nextDouble())), 0.0),
        (Vectors.dense(Array.fill(n)(rng.nextDouble())), 1.0)
      ).toDF("features", "label")

      val lor = new LogisticRegression().setMaxIter(1)
      val size1 = lor.estimateModelSize(df)
      val model = lor.fit(df)
      assert(model.coefficientMatrix.isInstanceOf[DenseMatrix])
      val size2 = model.estimatedSize

      // the model is dense, the estimation should be relatively accurate
      // (n, size1, size2)
      // (10,7021,7120)   <- when the model is small, model.params matters
      // (100,7741,7840)
      // (1000,14941,15040)
      // (10000,86941,87040)
      // (100000,806941,807040)
      val rel = (size1 - size2).toDouble / size2
      assert(math.abs(rel) < 0.05, (n, size1, size2))
    }
  }

  test("test binary logistic regression: sparse") {
    val rng = new Random(1)

    Seq(100, 1000, 10000, 100000).foreach { n =>
      val df = Seq(
        (Vectors.sparse(n, Array.range(0, 10), Array.fill(10)(rng.nextDouble())), 0.0),
        (Vectors.sparse(n, Array.range(0, 10), Array.fill(10)(rng.nextDouble())), 1.0)
      ).toDF("features", "label")

      val lor = new LogisticRegression().setMaxIter(1).setRegParam(10.0)
      val size1 = lor.estimateModelSize(df)
      val model = lor.fit(df)
      assert(model.coefficientMatrix.isInstanceOf[SparseMatrix])
      val size2 = model.estimatedSize

      // the model is sparse, the estimated size should be larger
      // (n, size1, size2)
      // (100,7741,7208)
      // (1000,14941,7208)
      // (10000,86941,7208)
      // (100000,806941,7208)
      assert(size1 > size2, (n, size1, size2))
    }
  }

  test("test multinomial logistic regression: dense") {
    val rng = new Random(1)

    Seq(10, 100, 1000, 10000, 100000).foreach { n =>
      val df = Seq(
        (Vectors.dense(Array.fill(n)(rng.nextDouble())), 0.0),
        (Vectors.dense(Array.fill(n)(rng.nextDouble())), 1.0),
        (Vectors.dense(Array.fill(n)(rng.nextDouble())), 2.0)
      ).toDF("features", "label")

      val lor = new LogisticRegression().setMaxIter(1)
      val size1 = lor.estimateModelSize(df)
      val model = lor.fit(df)
      assert(model.coefficientMatrix.isInstanceOf[DenseMatrix])
      val size2 = model.estimatedSize

      // the model is dense, the estimation should be relatively accurate
      // (n, size1, size2)
      // (10,7197,7296)   <- when the model is small, model.params matters
      // (100,9357,9456)
      // (1000,30957,31056)
      // (10000,246957,247056)
      // (100000,2406957,2407056)
      val rel = (size1 - size2).toDouble / size2
      assert(math.abs(rel) < 0.05, (n, size1, size2))
    }
  }

  test("test multinomial logistic regression: sparse") {
    val rng = new Random(1)

    Seq(100, 1000, 10000, 100000).foreach { n =>
      val df = Seq(
        (Vectors.sparse(n, Array.range(0, 10), Array.fill(10)(rng.nextDouble())), 0.0),
        (Vectors.sparse(n, Array.range(0, 10), Array.fill(10)(rng.nextDouble())), 1.0),
        (Vectors.sparse(n, Array.range(0, 10), Array.fill(10)(rng.nextDouble())), 2.0)
      ).toDF("features", "label")

      val lor = new LogisticRegression().setMaxIter(1).setRegParam(10.0)
      val size1 = lor.estimateModelSize(df)
      val model = lor.fit(df)
      assert(model.coefficientMatrix.isInstanceOf[SparseMatrix])
      val size2 = model.estimatedSize

      // the model is sparse, the estimated size should be larger
      // (100,9357,7472)
      // (1000,30957,7472)
      // (10000,246957,7472)
      // (100000,2406957,7472)
      assert(size1 > size2, (n, size1, size2))
    }
  }
}
