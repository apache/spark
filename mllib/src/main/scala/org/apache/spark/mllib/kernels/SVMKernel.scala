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
package org.apache.spark.mllib.kernels

import breeze.linalg.{DenseVector, DenseMatrix}
import org.apache.spark.Logging
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
/**
 * Defines an abstract class outlines the basic
 * functionality requirements of an SVM Kernel
 */
abstract class SVMKernel[T] extends Kernel with Logging with Serializable {

  /**
   * Build the kernel matrix of the prototype vectors
   *
   * @param mappedData The prototype vectors/points
   *
   * @param length The number of points
   *
   * @return A [[KernelMatrix]] object
   *
   *
   * */
  def buildKernelMatrixasRDD(
      mappedData: RDD[(Long, LabeledPoint)],
      length: Long): KernelMatrix[T]

  /**
   * Builds an approximate nonlinear feature map
   * which corresponds to an SVM Kernel. This is
   * done using the Nystrom method i.e. approximating
   * the eigenvalues and eigenvectors of the Kernel
   * matrix of a given RDD
   *
   * For each data point,
   * calculate m dimensions of the
   * feature map where m is the number
   * of eigenvalues/vectors obtained from
   * the Eigen Decomposition.
   *
   * phi_i(x) = (1/sqrt(eigenvalue(i)))*Sum(k, 1, m, K(k, x)*eigenvector(i)(k))
   *
   * @param decomposition The Eigenvalue decomposition calculated
   *                      from the kernel matrix of the prototype
   *                      subset.
   * @param prototypes The prototype subset.
   *
   * @param data  The dataset [[RDD]] on which the feature map
   *              is to be applied.
   *
   * */
  def featureMapping(decomposition: (DenseVector[Double], DenseMatrix[Double]))
                    (prototypes: RDD[(Long, LabeledPoint)])
                    (data: RDD[(Long, LabeledPoint)])
  : RDD[(Long, LabeledPoint)] = {

    logInfo("Calculating the Non Linear feature map of data set")

    data.cartesian(prototypes)
      .map((couple) => {
      val y: DenseVector[Double] = DenseVector.tabulate(decomposition._1.length){i =>
        var eigenvector = 0.0
        if (couple._2._1.toInt < decomposition._1.length) {
          eigenvector = decomposition._2(couple._2._1.toInt, i)
        }

        val eigenvalue = decomposition._1(i)
        this.evaluate(couple._1._2, couple._2._2) * eigenvector/Math.sqrt(eigenvalue)
      }
        (couple._1._1, (couple._1._2.label, y))
      }).reduceByKey((veca, vecb) => (veca._1, veca._2 + vecb._2))
      .map((p) => (p._1, new LabeledPoint(p._2._1, Vectors.fromBreeze(p._2._2))))
  }
}

/**
 * Defines a global singleton object
 * [[SVMKernel]] which has useful functions
 * while working with [[RDD]] of [[LabeledPoint]]
 *
 * */
object SVMKernel extends Logging with Serializable {

  /**
   * Defines a function value which
   * calculates the multiplication of
   * the Kernel Matrix with a Breeze
   * Vector and returns the result as a
   * Breeze DenseVector.
   * */
  def multiplyKernelMatrixBy(kernel: RDD[((Long, Long), Double)])
                            (v :breeze.linalg.DenseVector[Double]):
  DenseVector[Double] = {
      val vbr = kernel.context.broadcast(v)
      val result: DenseVector[Double] =
        DenseVector.tabulate(v.length)(
          (i) => {
            //Get row number i of kernel
            val row = DenseVector.apply(kernel
              .filter((point) => i == point._1._1)
              .map((p) => p._2)
              .collect())
            //dot product with v
            vbr.value.t * row
          }
        )
      result
    }

  /**
   * Returns an indexed [[RDD]] from a non indexed [[RDD]] of [[LabeledPoint]]
   *
   * @param data : An [[RDD]] of [[LabeledPoint]]
   *
   * @return An (Int, LabeledPoint) Key-Value RDD indexed
   *         from 0 to data.count() - 1
   * */
  def indexedRDD[T](data: RDD[T]): RDD[(Long, T)] = 
    data.zipWithIndex().map((p) => (p._2, p._1))

  /**
   * This function constructs an [[SVMKernelMatrix]]
   *
   * @param mappedData The indexed [[RDD]] of [[LabeledPoint]]
   * @param length Length of the indexed [[RDD]]
   * @param eval A function which calculates the value of the Kernel
   *             given two Labeled Points [[LabeledPoint]].
   *
   * @return An [[SVMKernelMatrix]] object.
   *
   * */
  def buildSVMKernelMatrix(
      mappedData: RDD[(Long, LabeledPoint)],
      length: Long,
      eval: (LabeledPoint, LabeledPoint) =>  Double):
  KernelMatrix[RDD[((Long, Long), Double)]] = {

    logInfo("Constructing key-value representation of kernel matrix.")
    logInfo("Dimension: " + length + " x " + length)

    val labels = mappedData.map((p) => (p._1, p._2.label))
    val kernel = mappedData.cartesian(mappedData)
      .map((prod) => ((prod._1._1, prod._2._1),
      eval(prod._1._2, prod._2._2)))
    kernel.cache()
    new SVMKernelMatrix(kernel, length, labels)
  }

  def zipVectorsWithLabels(
      mappedData: RDD[(Long, Vector)],
      labels: RDD[(Long, Double)]): RDD[LabeledPoint] = 
    mappedData.join(labels).map((point) =>
    new LabeledPoint(point._2._2, point._2._1))

  def unzipIndexedData(mappedData: RDD[(Long, LabeledPoint)]):
  RDD[LabeledPoint] = mappedData.map((p) => p._2)
}

/**
 * Defines a trait which outlines the basic
 * functionality of Kernel Matrices.
 * */
trait KernelMatrix[T] extends Serializable {
  protected val kernel: T

  def eigenDecomposition(dimensions: Int): (DenseVector[Double], DenseMatrix[Double])

  def getKernelMatrix(): T = this.kernel
}

class SVMKernelMatrix(
    override protected val kernel: RDD[((Long, Long), Double)],
    private val dimension: Long,
    private val labels: RDD[(Long, Double)])
  extends KernelMatrix[RDD[((Long, Long), Double)]]
  with Logging
  with Serializable {

  /**
   * Builds an approximate nonlinear feature map
   * which corresponds to an SVM Kernel. This is
   * done using the Nystrom method i.e. approximating
   * the eigenvalues and eigenvectors of the Kernel
   * matrix of a given RDD
   *
   * @param dimensions The effective number of dimensions
   *                   to be calculated in the feature map
   *
   * @return An RDD containing the non linear feature map
   *         of all the data points passed to the function.
   *
   * */
  override def eigenDecomposition(dimensions: Int = this.dimension.toInt):
  (DenseVector[Double], DenseMatrix[Double]) = {
    logInfo("Eigenvalue decomposition of the kernel matrix using ARPACK.")
    EigenValueDecomposition
      .symmetricEigs(
        SVMKernel.multiplyKernelMatrixBy(kernel),
        dimension.toInt, dimensions,
        0.0001, 300)
  }
}
