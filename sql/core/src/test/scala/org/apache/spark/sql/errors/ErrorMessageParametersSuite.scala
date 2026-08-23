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

package org.apache.spark.sql.errors

import org.apache.spark.{SparkFunSuite, SparkRuntimeException}
import org.apache.spark.sql.catalyst.catalog.InvalidUDFClassException
import org.apache.spark.sql.execution.streaming.state.StateStoreColumnFamilyMismatch

class ErrorMessageParametersSuite extends SparkFunSuite {

  test("SPARK-58945: invalid writer commit message reports detail") {
    checkError(
      exception = QueryExecutionErrors.invalidWriterCommitMessageError("zero")
        .asInstanceOf[SparkRuntimeException],
      condition = "INVALID_WRITER_COMMIT_MESSAGE",
      parameters = Map("detail" -> "zero"))
  }

  test("SPARK-58945: invalid UDF class error reports clazz") {
    checkError(
      exception = QueryCompilationErrors.invalidUDFClassError("example.InvalidFunction")
        .asInstanceOf[InvalidUDFClassException],
      condition = "_LEGACY_ERROR_TEMP_2450",
      parameters = Map("clazz" -> "example.InvalidFunction"))
  }

  test("SPARK-58945: state store mismatch reports schema details") {
    checkError(
      exception = new StateStoreColumnFamilyMismatch(
        "state", "old_schema", "new_schema"),
      condition = "STATE_STORE_COLUMN_FAMILY_SCHEMA_INCOMPATIBLE",
      parameters = Map(
        "colFamilyName" -> "state",
        "oldSchema" -> "old_schema",
        "newSchema" -> "new_schema"))
  }
}
