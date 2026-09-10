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
package org.apache.spark.sql.execution.externalUDF

import org.apache.spark.api.python.PythonEvalType
import org.apache.spark.sql.catalyst.expressions.{ExternalUserDefinedFunction, PythonUDF}
import org.apache.spark.udf.worker.UDFWorkerSpecification

/**
 * Adapts PySpark UDF metadata to the language-neutral external UDF representation.
 *
 * This helper only constructs protocol-facing metadata. Execution remains in the generic
 * external UDF framework.
 */
private[externalUDF] object PythonExternalUDFAdapter {
  private val PAYLOAD_FORMAT: String = "pyspark-udf-experimental"

  /**
   * Creates a scalar Python external UDF while leaving worker launch policy to the caller. Python
   * metadata stays inside the opaque payload; Init construction remains language-independent.
   */
  def toExternalUDF(
      udf: PythonUDF,
      workerSpec: UDFWorkerSpecification): ExternalUserDefinedFunction = {
    require(
      udf.evalType == PythonEvalType.SQL_ARROW_BATCHED_UDF,
      s"Unsupported Python external UDF eval type: ${udf.evalType}")

    ExternalUserDefinedFunction(
      name = Option(udf.name),
      workerSpec = workerSpec,
      payload = PythonUDFPayload.encode(udf.func),
      dataType = udf.dataType,
      children = udf.children,
      inputTypes = None,
      udfDeterministic = udf.udfDeterministic,
      udfNullable = udf.nullable,
      resultId = udf.resultId,
      payloadFormat = PAYLOAD_FORMAT,
      evalType = Some(udf.evalType.toString))
  }
}
