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

import org.apache.spark.sql.test.SharedSQLContext

class DataFrameImplicitsSuite extends QueryTest with SharedSQLContext {
  import testImplicits._

  test("RDD of tuples") {
    checkAnswer(
      ctx.sparkContext.parallelize(1 to 10).map(i => (i, i.toString)).toDF("intCol", "strCol"),
      (1 to 10).map(i => Row(i, i.toString)))
  }

  test("Seq of tuples") {
    checkAnswer(
      (1 to 10).map(i => (i, i.toString)).toDF("intCol", "strCol"),
      (1 to 10).map(i => Row(i, i.toString)))
  }

  test("RDD[Int]") {
    checkAnswer(
      ctx.sparkContext.parallelize(1 to 10).toDF("intCol"),
      (1 to 10).map(i => Row(i)))
  }

  test("RDD[Long]") {
    checkAnswer(
      ctx.sparkContext.parallelize(1L to 10L).toDF("longCol"),
      (1L to 10L).map(i => Row(i)))
  }

  test("RDD[String]") {
    checkAnswer(
      ctx.sparkContext.parallelize(1 to 10).map(_.toString).toDF("stringCol"),
      (1 to 10).map(i => Row(i.toString)))
  }
}
