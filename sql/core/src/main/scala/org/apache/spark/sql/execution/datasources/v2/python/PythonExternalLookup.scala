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
package org.apache.spark.sql.execution.datasources.v2.python

import org.apache.spark.{JobArtifactSet, SparkException}
import org.apache.spark.api.python.PythonFunction
import org.apache.spark.sql.classic.SparkSession
import org.apache.spark.sql.connector.read.{ExternalLookup, ExternalLookupReader, ExternalLookupReaderFactory}
import org.apache.spark.sql.execution.python.ArrowPythonRunner
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataType, StructType}

/**
 * The external lookup of a Python data source, planned on the driver by
 * [[PythonScanBuilder.selectLookupPredicates]].
 *
 * @param source the Python data source
 * @param plan the lookup planned in Python
 * @param readSchema the schema of the rows that the lookups return, the schema of the table
 */
class PythonExternalLookup(
    source: UserDefinedPythonDataSource,
    plan: PythonDataSourceLookupPlan,
    readSchema: StructType) extends ExternalLookup {
  // Captured on the driver, as `PythonBatch` does, for the Python workers on the executors.
  private val jobArtifactUUID = JobArtifactSet.getCurrentJobArtifactState.map(_.uuid)
  private val sessionUUID = {
    SparkSession.getActiveSession.collect {
      case session if session.sessionState.conf.pythonWorkerLoggingEnabled =>
        session.sessionUUID
    }
  }

  override def maxParameterRowsPerRequest(): Int = plan.maxParameterRowsPerRequest

  override def createReaderFactory(): ExternalLookupReaderFactory = {
    val conf = SQLConf.get
    new PythonExternalLookupReaderFactory(
      source.createPythonFunction(plan.reader),
      plan.parameterTypes,
      readSchema,
      conf.arrowMaxRecordsPerBatch,
      ArrowPythonRunner.getPythonRunnerConfMap(conf),
      jobArtifactUUID,
      sessionUUID)
  }
}

/**
 * Creates the readers that look up rows on the executors with the planned Python lookup reader.
 * The session configurations are captured on the driver, because `SQLConf.get` on an executor is
 * not the session's.
 *
 * @param reader the planned lookup reader, pickled, with the Python environment of the source
 * @param parameterTypes the data type of each field of a parameter row
 * @param readSchema the schema of the rows that the lookups return
 * @param maxRecordsPerBatch the maximum number of rows of an Arrow batch
 * @param pythonRunnerConf the session configurations of the Python worker
 * @param jobArtifactUUID the artifacts of the session, for the Python worker
 * @param sessionUUID the session, when Python worker logging is enabled
 */
class PythonExternalLookupReaderFactory(
    val reader: PythonFunction,
    val parameterTypes: Seq[DataType],
    val readSchema: StructType,
    val maxRecordsPerBatch: Int,
    val pythonRunnerConf: Map[String, String],
    val jobArtifactUUID: Option[String],
    val sessionUUID: Option[String]) extends ExternalLookupReaderFactory {
  override def createReader(): ExternalLookupReader = {
    throw SparkException.internalError(
      "Python data source external lookups cannot run on executors yet.")
  }
}
