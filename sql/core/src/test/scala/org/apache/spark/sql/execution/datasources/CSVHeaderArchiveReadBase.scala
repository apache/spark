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

package org.apache.spark.sql.execution.datasources

/**
 * [[CSVArchiveReadBase]] reading CSV files that carry a header row, plus header-specific archive
 * tests (mismatched headers, and delimiter/multiline cases whose first row is a header).
 */
trait CSVHeaderArchiveReadBase extends CSVArchiveReadBase {

  import testImplicits._

  override protected def header: Boolean = true

  test("CSV: entries with mismatched headers behave like standalone files") {
    assertArchiveMatchesDir(
      Seq(
        entryName(0) -> encodeFile(sampleDf((1, "Alice"), (2, "Bob"))),
        // A different second-column header: the schema's "name" column is absent from this entry.
        entryName(1) -> encodeFile(Seq((3, "Carol")).toDF("id", "nickname"))))
  }

  test("CSV: custom delimiter matches a directory read") {
    assertArchiveMatchesDir(
      Seq("a.csv" -> csvBytes("id;name\n1;Alice\n2;Bob\n")),
      extraOptions = Map("delimiter" -> ";"))
  }

  test("CSV: multiline quoted fields with embedded newlines match a directory read") {
    assertArchiveMatchesDir(
      Seq(
        "a.csv" -> csvBytes("id,note\n1,\"line1\nline2\"\n2,\"plain\"\n"),
        "b.csv" -> csvBytes("id,note\n3,\"a\nb\nc\"\n")),
      extraOptions = Map("multiLine" -> "true"),
      schema = "id INT, note STRING")
  }
}
