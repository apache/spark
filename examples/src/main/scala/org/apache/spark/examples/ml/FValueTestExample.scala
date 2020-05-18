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

// scalastyle:off println
package org.apache.spark.examples.ml

// $example on$
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.stat.FValueTest
// $example off$
import org.apache.spark.sql.SparkSession

/**
 * An example for FValue testing.
 * Run with
 * {{{
 * bin/run-example ml.FValueTestExample
 * }}}
 */
object FValueTestExample {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("FValueTestExample")
      .getOrCreate()
    import spark.implicits._

    // $example on$
    val data = Seq(
      (4.6, Vectors.dense(6.0, 7.0, 0.0, 7.0, 6.0, 0.0)),
      (6.6, Vectors.dense(0.0, 9.0, 6.0, 0.0, 5.0, 9.0)),
      (5.1, Vectors.dense(0.0, 9.0, 3.0, 0.0, 5.0, 5.0)),
      (7.6, Vectors.dense(0.0, 9.0, 8.0, 5.0, 6.0, 4.0)),
      (9.0, Vectors.dense(8.0, 9.0, 6.0, 5.0, 4.0, 4.0)),
      (9.0, Vectors.dense(8.0, 9.0, 6.0, 4.0, 0.0, 0.0))
    )

    val df = data.toDF("label", "features")
    val fValue = FValueTest.test(df, "features", "label").head
    println(s"pValues ${fValue.getAs[Vector](0)}")
    println(s"degreesOfFreedom ${fValue.getSeq[Int](1).mkString("[", ",", "]")}")
    println(s"fValues ${fValue.getAs[Vector](2)}")
    // $example off$

    spark.stop()
  }
}
// scalastyle:on println
