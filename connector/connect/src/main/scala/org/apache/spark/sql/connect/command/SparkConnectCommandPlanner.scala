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

package org.apache.spark.sql.connect.command

import scala.collection.JavaConverters._

import com.google.common.collect.{Lists, Maps}

import org.apache.spark.annotation.{Since, Unstable}
import org.apache.spark.api.python.{PythonEvalType, SimplePythonFunction}
import org.apache.spark.connect.proto
import org.apache.spark.connect.proto.WriteOperation
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.connect.planner.{DataTypeProtoConverter, SparkConnectPlanner}
import org.apache.spark.sql.execution.python.UserDefinedPythonFunction
import org.apache.spark.sql.types.StringType

final case class InvalidCommandInput(
    private val message: String = "",
    private val cause: Throwable = null)
    extends Exception(message, cause)

@Unstable
@Since("3.4.0")
class SparkConnectCommandPlanner(session: SparkSession, command: proto.Command) {

  lazy val pythonExec =
    sys.env.getOrElse("PYSPARK_PYTHON", sys.env.getOrElse("PYSPARK_DRIVER_PYTHON", "python3"))

  def process(): Unit = {
    command.getCommandTypeCase match {
      case proto.Command.CommandTypeCase.CREATE_FUNCTION =>
        handleCreateScalarFunction(command.getCreateFunction)
      case proto.Command.CommandTypeCase.WRITE_OPERATION =>
        handleWriteOperation(command.getWriteOperation)
      case _ => throw new UnsupportedOperationException(s"$command not supported.")
    }
  }

  /**
   * This is a helper function that registers a new Python function in the SparkSession.
   *
   * Right now this function is very rudimentary and bare-bones just to showcase how it
   * is possible to remotely serialize a Python function and execute it on the Spark cluster.
   * If the Python version on the client and server diverge, the execution of the function that
   * is serialized will most likely fail.
   *
   * @param cf
   */
  def handleCreateScalarFunction(cf: proto.CreateScalarFunction): Unit = {
    val function = SimplePythonFunction(
      cf.getSerializedFunction.toByteArray,
      Maps.newHashMap(),
      Lists.newArrayList(),
      pythonExec,
      "3.9", // TODO(SPARK-40532) This needs to be an actual Python version.
      Lists.newArrayList(),
      null)

    val udf = UserDefinedPythonFunction(
      cf.getPartsList.asScala.head,
      function,
      StringType,
      PythonEvalType.SQL_BATCHED_UDF,
      udfDeterministic = false)

    session.udf.registerPython(cf.getPartsList.asScala.head, udf)
  }

  /**
   * Transforms the write operation and executes it.
   *
   * The input write operation contains a reference to the input plan and transforms it to the
   * corresponding logical plan. Afterwards, creates the DataFrameWriter and translates the
   * parameters of the WriteOperation into the corresponding methods calls.
   *
   * @param writeOperation
   */
  def handleWriteOperation(writeOperation: WriteOperation): Unit = {
    // Transform the input plan into the logical plan.
    val planner = new SparkConnectPlanner(writeOperation.getInput, session)
    val plan = planner.transform()
    // And create a Dataset from the plan.
    val dataset = Dataset.ofRows(session, logicalPlan = plan)

    val w = dataset.write
    if (writeOperation.getMode != proto.WriteOperation.SaveMode.SAVE_MODE_UNSPECIFIED) {
      w.mode(DataTypeProtoConverter.toSaveMode(writeOperation.getMode))
    }

    if (writeOperation.getOptionsCount > 0) {
      writeOperation.getOptionsMap.asScala.foreach { case (key, value) => w.option(key, value) }
    }

    if (writeOperation.getSortColumnNamesCount > 0) {
      val names = writeOperation.getSortColumnNamesList.asScala
      w.sortBy(names.head, names.tail.toSeq: _*)
    }

    if (writeOperation.hasBucketBy) {
      val op = writeOperation.getBucketBy
      val cols = op.getBucketColumnNamesList.asScala
      if (op.getNumBuckets <= 0) {
        throw InvalidCommandInput(
          s"BucketBy must specify a bucket count > 0, received ${op.getNumBuckets} instead.")
      }
      w.bucketBy(op.getNumBuckets, cols.head, cols.tail.toSeq: _*)
    }

    if (writeOperation.getPartitioningColumnsCount > 0) {
      val names = writeOperation.getPartitioningColumnsList.asScala
      w.partitionBy(names.toSeq: _*)
    }

    if (writeOperation.getSource != null) {
      w.format(writeOperation.getSource)
    }

    writeOperation.getSaveTypeCase match {
      case proto.WriteOperation.SaveTypeCase.PATH => w.save(writeOperation.getPath)
      case proto.WriteOperation.SaveTypeCase.TABLE_NAME =>
        w.saveAsTable(writeOperation.getTableName)
      case _ =>
        throw new UnsupportedOperationException(
          "WriteOperation:SaveTypeCase not supported "
            + s"${writeOperation.getSaveTypeCase.getNumber}")
    }
  }

}
