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

import org.apache.spark.sql.functions._
import org.apache.spark.sql.test.SharedSQLContext

/**
 * A test suite to test DataFrame/SQL functionalities with complex types (i.e. array, struct, map).
 */
class DataFrameComplexTypeSuite extends QueryTest with SharedSQLContext {
  import testImplicits._

  test("UDF on struct") {
    val f = udf((a: String) => a)
    val df = sqlContext.sparkContext.parallelize(Seq((1, 1))).toDF("a", "b")
    df.select(struct($"a").as("s")).select(f($"s.a")).collect()
  }

  test("UDF on named_struct") {
    val f = udf((a: String) => a)
    val df = sqlContext.sparkContext.parallelize(Seq((1, 1))).toDF("a", "b")
    df.selectExpr("named_struct('a', a) s").select(f($"s.a")).collect()
  }

  test("UDF on array") {
    val f = udf((a: String) => a)
    val df = sqlContext.sparkContext.parallelize(Seq((1, 1))).toDF("a", "b")
    df.select(array($"a").as("s")).select(f(expr("s[0]"))).collect()
  }
}
