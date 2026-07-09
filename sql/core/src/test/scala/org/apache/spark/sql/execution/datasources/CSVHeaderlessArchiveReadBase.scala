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
 * [[CSVArchiveReadBase]] reading headerless CSV files (columns are positional), plus headerless
 * delimiter/multiline archive tests. The shared archive tests from [[ArchiveReadSuiteBase]] cover
 * the common headerless read paths.
 */
trait CSVHeaderlessArchiveReadBase extends CSVArchiveReadBase {

  override protected def header: Boolean = false

  test("CSV: headerless custom delimiter matches a directory read") {
    assertArchiveMatchesDir(
      Seq("a.csv" -> csvBytes("1;Alice\n2;Bob\n"), "b.csv" -> csvBytes("3;Carol\n")),
      extraOptions = Map("delimiter" -> ";"))
  }

  test("CSV: headerless multiline quoted fields with embedded newlines match a directory read") {
    assertArchiveMatchesDir(
      Seq(
        "a.csv" -> csvBytes("1,\"line1\nline2\"\n2,\"plain\"\n"),
        "b.csv" -> csvBytes("3,\"a\nb\nc\"\n")),
      extraOptions = Map("multiLine" -> "true"),
      schema = "id INT, note STRING")
  }
}
