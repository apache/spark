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

class CsvFunctionsSuite extends QueryTest with SharedSQLContext with FunctionsTests {
  import testImplicits._

  test("from_csv with empty options") {
    testEmptyOptions(from_csv, "1")
  }

  test("from_csv with option") {
    testOptions(from_csv, "26/08/2015 18:00")
  }

  test("checking the columnNameOfCorruptRecord option") {
    val invalid = "2013-111-11 12:13:14"
    testCorruptColumn(from_csv, corrupted = invalid, corruptColumn = invalid)
  }

  test("schema_of_csv - infers schemas") {
    val csv = "0.1,1234567890123"
    testSchemaInferring(schema_of_csv, lit(csv), schema_of_csv, csv)
  }

  test("schema_of_csv - infers schemas using options") {
    testSchemaInferringOpts(schema_of_csv, Map("sep" -> " "), input = "0.1 1234567890123")
  }

  test("to_csv - struct") {
    testToStruct(to_csv, "1")
  }

  test("to_csv with option") {
    testToStructOpts(to_csv, input = "2015-08-26 18:00:00.0", expected = "26/08/2015 18:00")
  }

  test("from_csv invalid csv - check modes") {
    testModesInFrom(from_csv, "2,12", "\"")
  }

  test("from_csv uses DDL strings for defining a schema - java") {
    testDDLSchema(from_csv, """1,"haa"""")
  }

  test("roundtrip to_csv -> from_csv") {
    testToFrom(to_csv, from_csv)
  }

  test("roundtrip from_csv -> to_csv") {
    testFromTo(from_csv, to_csv, "1")
  }

  test("infers schemas of a CSV string and pass to to from_csv") {
    testFromSchemaOf(
      from_csv,
      schema_of_csv,
      input = """0.123456789,1,"San Francisco"""",
      example = "0.1,9876543210123456,a")
  }

  test("Support to_csv in SQL") {
    val df1 = Seq(Tuple1(Tuple1(1))).toDF("a")
    checkAnswer(df1.selectExpr("to_csv(a)"), Row("1") :: Nil)
  }

  test("parse timestamps with locale") {
    testLocaleTimestamp(from_csv, identity)
  }
}
