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

package org.apache.spark.sql

import org.apache.spark.sql.test.SharedSparkSession

class DataFrameImplicitsSuite extends QueryTest with SharedSparkSession {
  import testImplicits._

  test("RDD of tuples") {
    checkAnswer(
      sparkContext.parallelize(1 to 10).map(i => (i, i.toString)).toDF("intCol", "strCol"),
      (1 to 10).map(i => Row(i, i.toString)))
  }

  test("Seq of tuples") {
    checkAnswer(
      (1 to 10).map(i => (i, i.toString)).toDF("intCol", "strCol"),
      (1 to 10).map(i => Row(i, i.toString)))
  }

  test("RDD[Int]") {
    checkAnswer(
      sparkContext.parallelize(1 to 10).toDF("intCol"),
      (1 to 10).map(i => Row(i)))
  }

  test("RDD[Long]") {
    checkAnswer(
      sparkContext.parallelize(1L to 10L).toDF("longCol"),
      (1L to 10L).map(i => Row(i)))
  }

  test("RDD[String]") {
    checkAnswer(
      sparkContext.parallelize(1 to 10).map(_.toString).toDF("stringCol"),
      (1 to 10).map(i => Row(i.toString)))
  }

  test("SPARK-19959: df[java.lang.Long].collect includes null throws NullPointerException") {
    checkAnswer(sparkContext.parallelize(Seq[java.lang.Integer](0, null, 2), 1).toDF(),
      Seq(Row(0), Row(null), Row(2)))
    checkAnswer(sparkContext.parallelize(Seq[java.lang.Long](0L, null, 2L), 1).toDF(),
      Seq(Row(0L), Row(null), Row(2L)))
    checkAnswer(sparkContext.parallelize(Seq[java.lang.Float](0.0F, null, 2.0F), 1).toDF(),
      Seq(Row(0.0F), Row(null), Row(2.0F)))
    checkAnswer(sparkContext.parallelize(Seq[java.lang.Double](0.0D, null, 2.0D), 1).toDF(),
      Seq(Row(0.0D), Row(null), Row(2.0D)))
  }
}
