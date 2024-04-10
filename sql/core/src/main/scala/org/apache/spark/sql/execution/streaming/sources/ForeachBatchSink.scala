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

package org.apache.spark.sql.execution.streaming.sources

import scala.util.control.NonFatal

import org.apache.spark.{SparkException, SparkThrowable}
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.execution.LogicalRDD
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.streaming.DataStreamWriter

class ForeachBatchSink[T](batchWriter: (Dataset[T], Long) => Unit, encoder: ExpressionEncoder[T])
  extends Sink {

  override def addBatch(batchId: Long, data: DataFrame): Unit = {
    val node = LogicalRDD.fromDataset(rdd = data.queryExecution.toRdd, originDataset = data,
      isStreaming = false)
    implicit val enc = encoder
    val ds = Dataset.ofRows(data.sparkSession, node).as[T]
    // SPARK-47329 - for stateful queries that perform multiple operations on the dataframe, it is
    // highly recommended to persist the dataframe to prevent state stores from reloading
    // state multiple times in each batch. We cannot however always call `persist` on the dataframe
    // here since we do not know in advance whether multiple operations(actions) will be performed
    // on the dataframe or not. There are side effects to `persist` that could be detrimental to the
    // overall performance of the system such as increased cache memory usage,
    // possible disk writes (with the default storage level) and unwanted cache block eviction.
    // It is therefore the responsibility of the user to call `persist` on the dataframe if they
    // know that multiple operations (actions) will be performed on the dataframe within
    // the foreachbatch UDF (user defined function).
    callBatchWriter(ds, batchId)
  }

  override def toString(): String = "ForeachBatchSink"

  private def callBatchWriter(ds: Dataset[T], batchId: Long): Unit = {
    try {
      batchWriter(ds, batchId)
    } catch {
      // The user code can throw any type of exception.
      case NonFatal(e) if !e.isInstanceOf[SparkThrowable] =>
        throw ForeachBatchUserFuncException(e)
    }
  }
}

/**
 * Exception that wraps the exception thrown in the user provided function in ForeachBatch sink.
 */
private[streaming] case class ForeachBatchUserFuncException(cause: Throwable)
  extends SparkException(
    errorClass = "FOREACH_BATCH_USER_FUNCTION_ERROR",
    messageParameters = Map("reason" -> Option(cause.getMessage).getOrElse("")),
    cause = cause)

/**
 * Interface that is meant to be extended by Python classes via Py4J.
 * Py4J allows Python classes to implement Java interfaces so that the JVM can call back
 * Python objects. In this case, this allows the user-defined Python `foreachBatch` function
 * to be called from JVM when the query is active.
 * */
trait PythonForeachBatchFunction {
  /** Call the Python implementation of this function */
  def call(batchDF: DataFrame, batchId: Long): Unit
}

object PythonForeachBatchHelper {
  def callForeachBatch(dsw: DataStreamWriter[Row], pythonFunc: PythonForeachBatchFunction): Unit = {
    dsw.foreachBatch(pythonFunc.call _)
  }
}

