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

package org.apache.spark.ml.feature

import org.apache.spark.ml.linalg._
import org.apache.spark.ml.param.ParamsSuite
import org.apache.spark.ml.util.{DefaultReadWriteTest, MLTest, MLTestingUtils}
import org.apache.spark.ml.util.TestingUtils._
import org.apache.spark.mllib.linalg.{Vectors => OldVectors}
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.sql.Row
import org.apache.spark.util.ArrayImplicits._

class PCASuite extends MLTest with DefaultReadWriteTest {

  import testImplicits._

  test("params") {
    ParamsSuite.checkParams(new PCA)
    val mat = Matrices.dense(2, 2, Array(0.0, 1.0, 2.0, 3.0)).asInstanceOf[DenseMatrix]
    val explainedVariance = Vectors.dense(0.5, 0.5).asInstanceOf[DenseVector]
    val model = new PCAModel("pca", mat, explainedVariance)
    ParamsSuite.checkParams(model)
  }

  test("pca") {
    val data = Array(
      Vectors.sparse(5, Seq((1, 1.0), (3, 7.0))),
      Vectors.dense(2.0, 0.0, 3.0, 4.0, 5.0),
      Vectors.dense(4.0, 0.0, 0.0, 6.0, 7.0)
    )

    val dataRDD = sc.parallelize(data.toImmutableArraySeq, 2)

    val mat = new RowMatrix(dataRDD.map(OldVectors.fromML))
    val pc = mat.computePrincipalComponents(3)
    val expected = mat.multiply(pc).rows.map(_.asML)

    val df = dataRDD.zip(expected).toDF("features", "expected")

    val pca = new PCA()
      .setInputCol("features")
      .setOutputCol("pca_features")
      .setK(3)

    val pcaModel = pca.fit(df)
    val transformed = pcaModel.transform(df)
    checkVectorSizeOnDF(transformed, "pca_features", pcaModel.getK)

    MLTestingUtils.checkCopyAndUids(pca, pcaModel)
    testTransformer[(Vector, Vector)](df, pcaModel, "pca_features", "expected") {
      case Row(result: Vector, expected: Vector) =>
        assert(result ~== expected absTol 1e-5,
          "Transformed vector is different with expected vector.")
    }
  }

  test("dataset with dense vectors and sparse vectors should produce same results") {
    val data1 = Array(
      Vectors.sparse(5, Seq((1, 1.0), (3, 7.0))),
      Vectors.dense(2.0, 0.0, 3.0, 4.0, 5.0),
      Vectors.dense(4.0, 0.0, 0.0, 6.0, 7.0)
    )
    val data2 = Array(
      Vectors.dense(0.0, 1.0, 0.0, 7.0, 0.0),
      Vectors.dense(2.0, 0.0, 3.0, 4.0, 5.0),
      Vectors.dense(4.0, 0.0, 0.0, 6.0, 7.0)
    )
    val data3 = data1.map(_.toSparse)
    val data4 = data1.map(_.toDense)

    val df1 = spark.createDataFrame(data1.map(Tuple1.apply).toImmutableArraySeq).toDF("features")
    val df2 = spark.createDataFrame(data2.map(Tuple1.apply).toImmutableArraySeq).toDF("features")
    val df3 = spark.createDataFrame(data3.map(Tuple1.apply).toImmutableArraySeq).toDF("features")
    val df4 = spark.createDataFrame(data4.map(Tuple1.apply).toImmutableArraySeq).toDF("features")

    val pca = new PCA()
      .setInputCol("features")
      .setOutputCol("pcaFeatures")
      .setK(3)
    val pcaModel1 = pca.fit(df1)
    val pcaModel2 = pca.fit(df2)
    val pcaModel3 = pca.fit(df3)
    val pcaModel4 = pca.fit(df4)

    assert(pcaModel1.explainedVariance == pcaModel2.explainedVariance)
    assert(pcaModel1.explainedVariance == pcaModel3.explainedVariance)
    assert(pcaModel1.explainedVariance == pcaModel4.explainedVariance)
    assert(pcaModel1.pc === pcaModel2.pc)
    assert(pcaModel1.pc === pcaModel3.pc)
    assert(pcaModel1.pc === pcaModel4.pc)
  }

  test("PCA read/write") {
    val t = new PCA()
      .setInputCol("myInputCol")
      .setOutputCol("myOutputCol")
      .setK(3)
    testDefaultReadWrite(t)
  }

  test("PCAModel read/write") {
    val instance = new PCAModel("myPCAModel",
      Matrices.dense(2, 2, Array(0.0, 1.0, 2.0, 3.0)).asInstanceOf[DenseMatrix],
      Vectors.dense(0.5, 0.5).asInstanceOf[DenseVector])
    val newInstance = testDefaultReadWrite(instance)
    assert(newInstance.pc === instance.pc)
  }
}
