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

import java.io._
import java.nio.charset.StandardCharsets

import org.json4s._
import org.json4s.jackson.JsonMethods._

import org.apache.spark.api.python._
import org.apache.spark.sql.Row
import org.apache.spark.sql.api.python.PythonSQLUtils
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.execution.streaming.GroupStateImpl
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * [[ArrowPythonRunner]] with [[org.apache.spark.sql.streaming.GroupState]].
 */
class ArrowPythonRunnerWithState(
    funcs: Seq[ChainedPythonFunctions],
    evalType: Int,
    argOffsets: Array[Array[Int]],
    protected override val schema: StructType,
    protected override val timeZoneId: String,
    protected override val workerConf: Map[String, String],
    oldState: GroupStateImpl[Row],
    deserializer: ExpressionEncoder.Deserializer[Row],
    stateType: StructType)
  extends BasePythonRunner[Iterator[InternalRow], ColumnarBatch](funcs, evalType, argOffsets)
  with PythonArrowInput
  with PythonArrowOutput {

  override val simplifiedTraceback: Boolean = SQLConf.get.pysparkSimplifiedTraceback

  override val bufferSize: Int = SQLConf.get.pandasUDFBufferSize
  require(
    bufferSize >= 4,
    "Pandas execution requires more than 4 bytes. Please set higher buffer. " +
      s"Please change '${SQLConf.PANDAS_UDF_BUFFER_SIZE.key}'.")

  var newGroupState: GroupStateImpl[Row] = _

  protected override def handleMetadataBeforeExec(stream: DataOutputStream): Unit = {
    super.handleMetadataBeforeExec(stream)

    // 1. Send JSON-serialized GroupState
    PythonRDD.writeUTF(oldState.json(), stream)

    // 2. Send pickled Row from the GroupState
    val rowInState = oldState.getOption.map(PythonSQLUtils.toPyRow).getOrElse(Array.empty)
    stream.writeInt(rowInState.length)
    if (rowInState.length > 0) {
      stream.write(rowInState)
    }

    // 3. Send the state type to serialize the output state back from Python.
    PythonRDD.writeUTF(stateType.json, stream)
  }

  protected override def handleMetadataAfterExec(stream: DataInputStream): Unit = {
    super.handleMetadataAfterExec(stream)

    implicit val formats = org.json4s.DefaultFormats

    // 1. Receive JSON-serialized GroupState
    val jsonStr = new Array[Byte](stream.readInt())
    stream.readFully(jsonStr)
    val properties = parse(new String(jsonStr, StandardCharsets.UTF_8))

    // 2. Receive and deserialized pickled Row to JVM Row.
    val length = stream.readInt()
    val maybeRow = if (length > 0) {
      val pickledRow = new Array[Byte](length)
      stream.readFully(pickledRow)
      Some(PythonSQLUtils.toJVMRow(pickledRow, stateType, deserializer))
    } else {
      None
    }

    // 3. Create a group state.
    newGroupState = GroupStateImpl.fromJson(maybeRow, properties)
  }
}
