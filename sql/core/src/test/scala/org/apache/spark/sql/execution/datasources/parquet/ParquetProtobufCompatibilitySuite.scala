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

package org.apache.spark.sql.execution.datasources.parquet

import org.apache.spark.sql.Row
import org.apache.spark.sql.test.SharedSQLContext

class ParquetProtobufCompatibilitySuite extends ParquetCompatibilityTest with SharedSQLContext {
  test("unannotated array of primitive type") {
    checkAnswer(readResourceParquetFile("test-data/old-repeated-int.parquet"), Row(Seq(1, 2, 3)))
  }

  test("unannotated array of struct") {
    checkAnswer(
      readResourceParquetFile("test-data/old-repeated-message.parquet"),
      Row(
        Seq(
          Row("First inner", null, null),
          Row(null, "Second inner", null),
          Row(null, null, "Third inner"))))

    checkAnswer(
      readResourceParquetFile("test-data/proto-repeated-struct.parquet"),
      Row(
        Seq(
          Row("0 - 1", "0 - 2", "0 - 3"),
          Row("1 - 1", "1 - 2", "1 - 3"))))

    checkAnswer(
      readResourceParquetFile("test-data/proto-struct-with-array-many.parquet"),
      Seq(
        Row(
          Seq(
            Row("0 - 0 - 1", "0 - 0 - 2", "0 - 0 - 3"),
            Row("0 - 1 - 1", "0 - 1 - 2", "0 - 1 - 3"))),
        Row(
          Seq(
            Row("1 - 0 - 1", "1 - 0 - 2", "1 - 0 - 3"),
            Row("1 - 1 - 1", "1 - 1 - 2", "1 - 1 - 3"))),
        Row(
          Seq(
            Row("2 - 0 - 1", "2 - 0 - 2", "2 - 0 - 3"),
            Row("2 - 1 - 1", "2 - 1 - 2", "2 - 1 - 3")))))
  }

  test("struct with unannotated array") {
    checkAnswer(
      readResourceParquetFile("test-data/proto-struct-with-array.parquet"),
      Row(10, 9, Seq.empty, null, Row(9), Seq(Row(9), Row(10))))
  }

  test("unannotated array of struct with unannotated array") {
    checkAnswer(
      readResourceParquetFile("test-data/nested-array-struct.parquet"),
      Seq(
        Row(2, Seq(Row(1, Seq(Row(3))))),
        Row(5, Seq(Row(4, Seq(Row(6))))),
        Row(8, Seq(Row(7, Seq(Row(9)))))))
  }

  test("unannotated array of string") {
    checkAnswer(
      readResourceParquetFile("test-data/proto-repeated-string.parquet"),
      Seq(
        Row(Seq("hello", "world")),
        Row(Seq("good", "bye")),
        Row(Seq("one", "two", "three"))))
  }
}
