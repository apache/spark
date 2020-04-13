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
import org.apache.spark.ml.stat.ANOVATest
// $example off$
import org.apache.spark.sql.SparkSession

/**
 * An example for ANOVA testing.
 * Run with
 * {{{
 * bin/run-example ml.ANOVATestExample
 * }}}
 */
object ANOVATestExample {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("ANOVATestExample")
      .getOrCreate()
    import spark.implicits._

    // $example on$
    val data = Seq(
      (3.0, Vectors.dense(1.7, 4.4, 7.6, 5.8, 9.6, 2.3)),
      (2.0, Vectors.dense(8.8, 7.3, 5.7, 7.3, 2.2, 4.1)),
      (1.0, Vectors.dense(1.2, 9.5, 2.5, 3.1, 8.7, 2.5)),
      (2.0, Vectors.dense(3.7, 9.2, 6.1, 4.1, 7.5, 3.8)),
      (4.0, Vectors.dense(8.9, 5.2, 7.8, 8.3, 5.2, 3.0)),
      (4.0, Vectors.dense(7.9, 8.5, 9.2, 4.0, 9.4, 2.1))
    )

    val df = data.toDF("label", "features")
    val anova = ANOVATest.test(df, "features", "label").head
    println(s"pValues = ${anova.getAs[Vector](0)}")
    println(s"degreesOfFreedom ${anova.getSeq[Int](1).mkString("[", ",", "]")}")
    println(s"fValues ${anova.getAs[Vector](2)}")
    // $example off$

    spark.stop()
  }
}
// scalastyle:on println
