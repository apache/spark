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

package org.apache.spark.ml.stat


import org.apache.spark.SparkFunSuite
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.mllib.util.TestingUtils._
import org.apache.spark.sql.{DataFrame, Dataset, Row}


class SummarizerSuite extends SparkFunSuite with MLlibTestSparkContext {

  import testImplicits._
  import Summarizer._

  private val seed = 42
  private val eps: Double = 1e-5

  private def denseData(input: Seq[Seq[Double]]): DataFrame = {
    val data = input.map(_.toArray).map(Vectors.dense).map(Tuple1.apply)
    sc.parallelize(data).toDF("features")
  }

  private def compare(df: DataFrame, exp: Seq[Any]): Unit = {
    val res = df.first().toSeq
    val names = df.schema.fieldNames.zipWithIndex.map { case (n, idx) => s"$n ($idx)" }
    assert(res.size === exp.size)
    for (((x1, x2), name) <- res.zip(exp).zip(names)) {
      (x1, x2) match {
        case (d1: Double, d2: Double) =>
          assert(Vectors.dense(d1) ~== Vectors.dense(d2) absTol 1e-4, name)
      }
    }
  }

  test("basic error handling") {
    val df = denseData(Nil)
    val c = df.col("features")
    val res = df.select(metrics("mean").summary(c), mean(c))
    intercept[IllegalArgumentException] {
      compare(res, Seq.empty)
    }
  }
}
