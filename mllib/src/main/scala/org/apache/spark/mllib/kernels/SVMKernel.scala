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

import breeze.linalg.DenseVector
import org.apache.spark.mllib.linalg
import org.apache.spark.{SparkContext, Logging}
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
/**
 * Defines an abstract class outlines the basic
 * functionality requirements of an SVM Kernel
 */
abstract class SVMKernel[T] extends Kernel with Logging with Serializable {

  def buildKernelMatrixasRDD(mappedData: RDD[(Long, LabeledPoint)],
                             length: Long): KernelMatrix[T]

}

/**
 * Defines a global singleton object
 * [[SVMKernel]] which has useful functions
 * while working with [[RDD]] of [[LabeledPoint]]
 *
 * */
object SVMKernel extends Logging with Serializable{

  /**
   * Returns an indexed [[RDD]] from a non indexed [[RDD]] of [[LabeledPoint]]
   *
   * @param data : An [[RDD]] of [[LabeledPoint]]
   *
   * @return An (Int, LabeledPoint) Key-Value RDD indexed
   *         from 0 to data.count() - 1
   * */
  def indexedRDD[T](data: RDD[T]): RDD[(Long, T)] = {
    val sc = data.context
    val i: org.apache.spark.Accumulator[Long] = sc.accumulator(-1, "Raw Data Index")

    data.map((point) => {
      i+=1
      (i.localValue, point)
    })
  }


  /**
   * This function constructs an [[SVMKernelMatrix]]
   *
   * @param mappedData The indexed [[RDD]] of [[LabeledPoint]]
   * @param length Length of the indexed [[RDD]]
   * @param eval A function which calculates the value of the Kernel
   *             given two Vectors [[linalg.Vector]].
   *
   * @return An [[SVMKernelMatrix]] object.
   *
   * */
  def buildSVMKernelMatrix(mappedData: RDD[(Long, LabeledPoint)],
                           length: Long,
                           eval: (linalg.Vector, linalg.Vector) =>  Double):
  KernelMatrix[RDD[((Long, Long), Double)]] = {

    logInfo("Constructing key-value representation of kernel matrix.")
    logInfo("Dimension: " + length + " x " + length)

    val labels = mappedData.map((p) => (p._1, p._2.label))
    val kernel = mappedData.cartesian(mappedData)
      .map((prod) => ((prod._1._1, prod._2._1),
      eval(prod._1._2.features, prod._2._2.features)))
    kernel.cache()
    new SVMKernelMatrix(kernel, length, labels)
  }

}

/**
 * Defines a trait which outlines the basic
 * functionality of Kernel Matrices.
 * */
trait KernelMatrix[T] extends Serializable{
  protected val kernel: T
  def buildFeatureMap(dimensions: Int): RDD[LabeledPoint]
  def getKernelMatrix(): T = this.kernel
}

class SVMKernelMatrix(protected override val kernel: RDD[((Long, Long), Double)],
                      private val dimension: Long,
                      private val labels: RDD[(Long, Double)])
  extends KernelMatrix[RDD[((Long, Long), Double)]] with Logging with Serializable {

  override def getKernelMatrix():RDD[((Long, Long), Double)] = this.kernel

  /**
   * Defines a function value which
   * calculates the multiplication of
   * the Kernel Matrix with a Breeze
   * Vector and returns the result as a
   * Breeze DenseVector.
   * */
  val multiplyKernelMatrixOn =
    (v :breeze.linalg.DenseVector[Double]) => {
      val vbr = kernel.context.broadcast(v)
      v.mapPairs((i, _) => {
        //Get row number i of kernel
        val row = kernel.filter((point) => i == point._1._1)
        //multiply with v
        var sum = kernel.context.accumulator(0.00, "Multiplication product, vector")
        row.foreach((rownum) => {
          sum += rownum._2*vbr.value(rownum._1._2.toInt)
        })
        sum.value
      })
    }

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
  def buildFeatureMap(dimensions: Int): RDD[LabeledPoint] = {


    logInfo("Eigenvalue decomposition of the kernel matrix using ARPACK.")
    val decomposition = EigenValueDecomposition
      .symmetricEigs(
        multiplyKernelMatrixOn,
        dimension.toInt, dimensions,
        0.0001, 300)

    logInfo("Applying Nystrom formula to calculate feature map of kernel matrix")

    /*
    * Get row number i of the
    * Kernel Matrix
    * */
    val rows = kernel.groupBy((couple) => {
      couple._1._1
    })

    /*
    * Join the each row i with the
    * target label for point i.
    * */
    val temp = labels.join(rows)

    /*
    * Now for each data point,
    * calculate n dimensions of the
    * feature map where n is the number
    * of eigenvalues/vectors obtained from
    * the Eigen Decomposition.
    *
    * phi_i(x) = (1/sqrt(eigenvalue(i)))*Sum(k, 1, n, K(k, x)*eigenvector(i)(k))
    * */
    temp.map((datapoint) => {
      val y: DenseVector[Double] = DenseVector.tabulate(decomposition._1.length){i =>
        val eigenvector = decomposition._2(::, i)
        val eigenvalue = decomposition._1(i)
        var acc = 0.0
        datapoint._2._2.foreach((p) =>
          acc += (p._2 * eigenvector(p._1._2.toInt)/Math.sqrt(eigenvalue))
        )
        acc
      }
      new LabeledPoint(datapoint._2._1, Vectors.fromBreeze(y))
    })

  }
}
