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

import scala.beans.BeanInfo
import scala.collection.mutable.ArrayBuffer

import edu.emory.mathcs.jtransforms.dct.DoubleDCT_1D

import org.apache.spark.SparkFunSuite
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.sql.{DataFrame, Row}

@BeanInfo
case class DCTTestData[T](vec: Array[T], wantedVec: Array[Double])

class DiscreteCosineTransformerSuite extends SparkFunSuite with MLlibTestSparkContext {
  import org.apache.spark.ml.feature.DiscreteCosineTransformerSuite._

  test("forward transform of discrete cosine matches jTransforms result") {
    val transformer = new DiscreteCosineTransformer[Double]()
      .setInputCol("vec")
      .setOutputCol("resultVec")
      .setInverse(false)
    val data = (0 until 128).map(_ => 2D * math.random - 1D).toArray
    val expectedResult = data.clone()
    (new DoubleDCT_1D(data.length)).forward(expectedResult, true)
    val dataset = sqlContext.createDataFrame(Seq(
      DCTTestData[Double](data, expectedResult)
    ))
    testDCT(transformer, dataset)
  }

  test("inverse transform of discrete cosine matches jTransforms result") {
    val transformer = new DiscreteCosineTransformer[Double]()
      .setInputCol("vec")
      .setOutputCol("resultVec")
      .setInverse(true)
    val data = (0 until 128).map(_ => 2D * math.random - 1D).toArray
    val expectedResult = data.clone()
    (new DoubleDCT_1D(data.length)).inverse(expectedResult, true)
    val dataset = sqlContext.createDataFrame(Seq(
      DCTTestData[Double](data, expectedResult)
    ))
    testDCT(transformer, dataset)
  }

  test("handle float datatype") {
    val transformer = new DiscreteCosineTransformer[Float]()
      .setInputCol("vec")
      .setOutputCol("resultVec")
      .setInverse(true)
    val data = (0 until 128).map(_ => (2D * math.random - 1D).toFloat).toArray
    val expectedResult = data.clone().map(_.toDouble)
    (new DoubleDCT_1D(data.length)).inverse(expectedResult, true)
    val dataset = sqlContext.createDataFrame(Seq(
      DCTTestData[Float](data, expectedResult)
    ))
    testDCT(transformer, dataset)
  }

  test("handle integer datatype") {
    val transformer = new DiscreteCosineTransformer[Int]()
      .setInputCol("vec")
      .setOutputCol("resultVec")
      .setInverse(true)
    val data = (0 until 128).map(_ => (2D * math.random - 1D).toInt).toArray
    val expectedResult = data.clone().map(_.toDouble)
    (new DoubleDCT_1D(data.length)).inverse(expectedResult, true)
    val dataset = sqlContext.createDataFrame(Seq(
      DCTTestData[Int](data, expectedResult)
    ))
    testDCT(transformer, dataset)
  }
}

object DiscreteCosineTransformerSuite extends SparkFunSuite {

  def testDCT(t: DiscreteCosineTransformer[_], dataset: DataFrame): Unit = {
    t.transform(dataset)
      .select("resultVec", "wantedVec")
      .collect()
      .foreach { case Row(resultVec: ArrayBuffer[Double], wantedVec : ArrayBuffer[Double]) =>
        assert(resultVec.zip(wantedVec).map(x => math.pow(x._1 - x._2, 2)).sum < 1e-4)
      }
  }
}
