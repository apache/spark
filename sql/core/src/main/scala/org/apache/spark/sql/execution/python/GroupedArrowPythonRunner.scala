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

import java.io.DataOutputStream

import org.apache.spark.api.python.ChainedPythonFunctions
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.types.StructType



/**
 * Python UDF Runner for grouped udfs.
 */
class GroupedArrowPythonRunner(
    funcs: Seq[(ChainedPythonFunctions, Long)],
    evalType: Int,
    argOffsets: Array[Array[Int]],
    schema: StructType,
    timeZoneId: String,
    largeVarTypes: Boolean,
    arrowMaxRecordsPerBatch: Int,
    conf: Map[String, String],
    pythonMetrics: Map[String, SQLMetric],
    jobArtifactUUID: Option[String],
    profiler: Option[String])
  extends BaseGroupedArrowPythonRunner[Iterator[InternalRow]](
    funcs, evalType, argOffsets, timeZoneId, largeVarTypes, arrowMaxRecordsPerBatch, conf,
    pythonMetrics, jobArtifactUUID, profiler) {

  override protected def writeNextGroup(
      group: Iterator[InternalRow],
      dataOut: DataOutputStream): Unit = {
    dataOut.writeInt(1)
    writeSingleGroup(group, schema, dataOut, "batch")
  }
}
