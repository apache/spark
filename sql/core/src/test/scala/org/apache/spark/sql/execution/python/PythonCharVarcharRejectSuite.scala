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

package org.apache.spark.sql.execution.python

import org.apache.spark.api.python.PythonEvalType
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.util.CharVarcharUtils
import org.apache.spark.sql.test.{ExamplePointUDT, SharedSparkSession}
import org.apache.spark.sql.types._

class PythonCharVarcharRejectSuite extends SharedSparkSession {

  private class CharStorageUDT extends ExamplePointUDT {
    override def sqlType: DataType = CharType(3)
  }

  private def dummyPythonUdf(
      dataType: DataType,
      bufferType: DataType = null): UserDefinedPythonFunction = {
    UserDefinedPythonFunction(
      name = "dummyUDF",
      func = new DummyUDF,
      dataType = dataType,
      pythonEvalType = PythonEvalType.SQL_GROUPED_AGG_ARROW_INCREMENTAL_FINAL_UDF,
      udfDeterministic = true,
      bufferType = bufferType)
  }

  test("hasCharVarcharIncludingUDT unwraps UserDefinedType storage") {
    assert(!CharVarcharUtils.hasCharVarchar(new CharStorageUDT()))
    assert(CharVarcharUtils.hasCharVarcharIncludingUDT(new CharStorageUDT()))
    assert(!CharVarcharUtils.hasCharVarcharIncludingUDT(StringType))
  }

  test("builder rejects CHAR return types separately from STRING buffers") {
    val udf = dummyPythonUdf(CharType(3), StringType)
    checkError(
      exception = intercept[AnalysisException] { udf.builder(Nil) },
      condition = "CHAR_VARCHAR_NOT_SUPPORTED_IN_PYTHON",
      parameters = Map(
        "feature" -> "Python UDF return types",
        "data_type" -> "char(3)"))
  }

  test("builder rejects CHAR/VARCHAR buffer schemas with buffer feature name") {
    val buffer = StructType(StructField("s", VarcharType(3)) :: Nil)
    val udf = dummyPythonUdf(StringType, buffer)
    checkError(
      exception = intercept[AnalysisException] { udf.builder(Nil) },
      condition = "CHAR_VARCHAR_NOT_SUPPORTED_IN_PYTHON",
      parameters = Map(
        "feature" -> "Python UDAF buffer schemas",
        "data_type" -> buffer.catalogString))
  }

  test("builder rejects UDT storage that is CHAR") {
    val udf = dummyPythonUdf(new CharStorageUDT())
    checkError(
      exception = intercept[AnalysisException] { udf.builder(Nil) },
      condition = "CHAR_VARCHAR_NOT_SUPPORTED_IN_PYTHON",
      parameters = Map(
        "feature" -> "Python UDF return types",
        "data_type" -> "char(3)"))
  }

  test("UDTF builder rejects CHAR return types") {
    val schema = StructType(StructField("c", CharType(3)) :: Nil)
    val udtf = UserDefinedPythonTableFunction(
      name = "dummyUDTF",
      func = new DummyUDF,
      returnType = Some(schema),
      pythonEvalType = PythonEvalType.SQL_TABLE_UDF,
      udfDeterministic = true)
    checkError(
      exception = intercept[AnalysisException] {
        udtf.builder(Nil, spark.sessionState.sqlParser)
      },
      condition = "CHAR_VARCHAR_NOT_SUPPORTED_IN_PYTHON",
      parameters = Map(
        "feature" -> "Python UDTF return types",
        "data_type" -> schema.catalogString))
  }
}
