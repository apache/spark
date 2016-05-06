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

import org.apache.spark.annotation.Since
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.rdd.RDD

/**
 * A feature transformer that projects vectors to a low-dimensional space using PCA.
 *
 * @param k number of principal components
 */
@Since("1.4.0")
class PCA @Since("1.4.0") (@Since("1.4.0") val k: Int) {
  require(k > 0,
    s"Number of principal components must be positive but got ${k}")

  /**
   * Computes a [[PCAModel]] that contains the principal components of the input vectors.
   *
   * @param sources source vectors
   */
  @Since("1.4.0")
  def fit(sources: RDD[Vector]): PCAModel = {
    require(k <= sources.first().size,
      s"source vector size is ${sources.first().size} must be greater than k=$k")

    val mat = new RowMatrix(sources)
    val (pc, explainedVariance) = mat.computePrincipalComponentsAndExplainedVariance(k)
    val densePC = pc match {
      case dm: DenseMatrix =>
        dm
      case sm: SparseMatrix =>
        /* Convert a sparse matrix to dense.
         *
         * RowMatrix.computePrincipalComponents always returns a dense matrix.
         * The following code is a safeguard.
         */
        sm.toDense
      case m =>
        throw new IllegalArgumentException("Unsupported matrix format. Expected " +
          s"SparseMatrix or DenseMatrix. Instead got: ${m.getClass}")

    }
    val denseExplainedVariance = explainedVariance match {
      case dv: DenseVector =>
        dv
      case sv: SparseVector =>
        sv.toDense
    }
    new PCAModel(k, densePC, denseExplainedVariance)
  }

  /**
   * Java-friendly version of [[fit()]]
   */
  @Since("1.4.0")
  def fit(sources: JavaRDD[Vector]): PCAModel = fit(sources.rdd)
}

/**
 * Model fitted by [[PCA]] that can project vectors to a low-dimensional space using PCA.
 *
 * @param k number of principal components.
 * @param pc a principal components Matrix. Each column is one principal component.
 */
@Since("1.4.0")
class PCAModel private[spark] (
    @Since("1.4.0") val k: Int,
    @Since("1.4.0") val pc: DenseMatrix,
    @Since("1.6.0") val explainedVariance: DenseVector) extends VectorTransformer {
  /**
   * Transform a vector by computed Principal Components.
   *
   * @param vector vector to be transformed.
   *               Vector must be the same length as the source vectors given to [[PCA.fit()]].
   * @return transformed vector. Vector will be of length k.
   */
  @Since("1.4.0")
  override def transform(vector: Vector): Vector = {
    vector match {
      case dv: DenseVector =>
        pc.transpose.multiply(dv)
      case SparseVector(size, indices, values) =>
        /* SparseVector -> single row SparseMatrix */
        val sm = Matrices.sparse(size, 1, Array(0, indices.length), indices, values).transpose
        val projection = sm.multiply(pc)
        Vectors.dense(projection.values)
      case _ =>
        throw new IllegalArgumentException("Unsupported vector format. Expected " +
          s"SparseVector or DenseVector. Instead got: ${vector.getClass}")
    }
  }
}
