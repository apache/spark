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

import java.io.{DataInputStream, DataOutputStream}

import scala.collection.mutable.ArrayBuffer

import net.razorvine.pickle.Pickler

import org.apache.spark.api.python.{PythonFunction, PythonWorkerUtils, SpecialLengths}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.catalyst.plans.logical.PythonDataSource
import org.apache.spark.sql.catalyst.types.DataTypeUtils.toAttributes
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.types.StructType

/**
 * A user-defined Python data source. This is used by the Python API.
 */
case class UserDefinedPythonDataSource(
    dataSource: PythonFunction,
    schema: StructType) {
  def apply(session: SparkSession): DataFrame = {
    val source = PythonDataSource(dataSource, schema, output = toAttributes(schema))
    Dataset.ofRows(session, source)
  }
}

case class PythonDataSourceReadInfo(
    func: Array[Byte],
    partitions: Seq[Array[Byte]])

class UserDefinedPythonDataSourceReadRunner(
    func: PythonFunction,
    schema: StructType) extends PythonPlannerRunner[PythonDataSourceReadInfo](func) {

  // See the logic in `pyspark.sql.worker.plan_data_source_read.py`.
  override val workerModule = "pyspark.sql.worker.plan_data_source_read"

  override protected def writeToPython(dataOut: DataOutputStream, pickler: Pickler): Unit = {
    // Send Python data source
    PythonWorkerUtils.writePythonFunction(func, dataOut)

    // Send schema
    PythonWorkerUtils.writeUTF(schema.json, dataOut)
  }

  override protected def receiveFromPython(dataIn: DataInputStream): PythonDataSourceReadInfo = {
    // Receive the picked reader or an exception raised in Python worker.
    val length = dataIn.readInt()
    if (length == SpecialLengths.PYTHON_EXCEPTION_THROWN) {
      val msg = PythonWorkerUtils.readUTF(dataIn)
      throw QueryCompilationErrors.failToPlanDataSourceError("read", msg)
    }

    // Receive the pickled 'read' function.
    val pickledFunction: Array[Byte] = PythonWorkerUtils.readBytes(length, dataIn)

    // Receive the list of partitions, if any.
    val pickledPartitions = ArrayBuffer.empty[Array[Byte]]
    val numPartitions = dataIn.readInt()
    for (_ <- 0 until numPartitions) {
      val pickledPartition: Array[Byte] = PythonWorkerUtils.readBytes(dataIn)
      pickledPartitions.append(pickledPartition)
    }

    PythonDataSourceReadInfo(
      func = pickledFunction,
      partitions = pickledPartitions.toSeq)
  }
}
