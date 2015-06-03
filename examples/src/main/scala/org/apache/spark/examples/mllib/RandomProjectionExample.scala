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

package org.apache.spark.examples.mllib

import org.apache.spark.mllib.feature.RandomProjection
import org.apache.spark.mllib.linalg.distributed.{BlockMatrix, CoordinateMatrix, MatrixEntry}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Show how to use RandomProjection as part of a ML pipeline
 *
 * Run with
 * {{{
 * bin/run-example ml.RandomProjectionExample
 * }}}
 */
object RandomProjectionExample {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("RandomProjectionExample")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val response = RPDataHelper.getMatrix(sc)
    // project into this dimension
    val newDimension = 3
    val rp = new RandomProjection(newDimension)
    val randomMatrix = rp.computeRPMatrix(sc, response.dimension).transpose
    val reduced = randomMatrix.multiply(response.data.transpose).transpose
    println(reduced.numRows())
    println(reduced.numCols())
  }
}

case class RPDataHelperResponse(data: BlockMatrix, dimension: Int)

object RPDataHelper{

  def getMatrix(sc: SparkContext): RPDataHelperResponse = {
    /**
     * sample copied from:
     *  Lichman, M. (2013). UCI Machine Learning Repository [http://archive.ics.uci.edu/ml].
     *  Irvine, CA: University of California, School of Information and Computer Science.
     */
    val file = List(
      "0,17.99,10.38,122.8,1001,0.1184,0.2776,0.3001,0.1471,0.2419,0.07871,1.095,0.9053,8.589," +
        "153.4,0.006399,0.04904,0.05373,0.01587,0.03003,0.006193,25.38,17.33,184.6,2019,0.1622," +
        "0.6656,0.7119,0.2654,0.4601,0.1189",
      "0,20.57,17.77,132.9,1326,0.08474,0.07864,0.0869,0.07017,0.1812,0.05667,0.5435,0.7339," +
        "3.398,74.08,0.005225,0.01308,0.0186,0.0134,0.01389,0.003532,24.99,23.41,158.8,1956," +
        "0.1238,0.1866,0.2416,0.186,0.275,0.08902",
      "0,19.69,21.25,130,1203,0.1096,0.1599,0.1974,0.1279,0.2069,0.05999,0.7456,0.7869," +
        "4.585,94.03,0.00615,0.04006,0.03832,0.02058,0.0225,0.004571,23.57,25.53,152.5,1709," +
        "0.1444,0.4245,0.4504,0.243,0.3613,0.08758",
      "1,11.42,20.38,77.58,386.1,0.1425,0.2839,0.2414,0.1052,0.2597,0.09744,0.4956,1.156," +
        "3.445,27.23,0.00911,0.07458,0.05661,0.01867,0.05963,0.009208,14.91,26.5,98.87,567.7," +
        "0.2098,0.8663,0.6869,0.2575,0.6638,0.173",
      "1,11.42,20.38,77.58,386.1,0.1425,0.2839,0.2414,0.1052,0.2597,0.09744,0.4956,1.156,3.445," +
        "27.23,0.00911,0.07458,0.05661,0.01867,0.05963,0.009208,14.91,26.5,98.87,567.7,0.2098," +
        "0.8663,0.6869,0.2575,0.6638,0.173",
      "1,11.42,20.38,77.58,386.1,0.1425,0.2839,0.2414,0.1052,0.2597,0.09744,0.4956,1.156,3.445," +
        "27.23,0.00911,0.07458,0.05661,0.01867,0.05963,0.009208,14.91,26.5,98.87,567.7,0.2098," +
        "0.8663,0.6869,0.2575,0.6638,0.173",
      "1,11.42,20.38,77.58,386.1,0.1425,0.2839,0.2414,0.1052,0.2597,0.09744,0.4956,1.156," +
        "3.445,27.23,0.00911,0.07458,0.05661,0.01867,0.05963,0.009208,14.91,26.5,98.87,567.7," +
        "0.2098,0.8663,0.6869,0.2575,0.6638,0.173")

    val matrix = file.zipWithIndex.map{ line =>
      val rowIndex = line._2
      line._1.split(",").tail.zipWithIndex.map{ item =>
        val colIndex = item._2
        MatrixEntry(rowIndex, colIndex, item._1.toDouble)
      }
    }
    val entries: RDD[MatrixEntry] = sc.parallelize(matrix.flatten)
    val coordMat: CoordinateMatrix = new CoordinateMatrix(entries)
    val matA: BlockMatrix = coordMat.toBlockMatrix().cache()

    val origDimension = file.head.split(",").tail.length
    RPDataHelperResponse(matA, origDimension)
  }

}
