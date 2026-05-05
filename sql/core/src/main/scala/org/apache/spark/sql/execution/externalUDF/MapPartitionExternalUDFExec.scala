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

import org.apache.spark.TaskContext
import org.apache.spark.annotation.Experimental
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute,
  ExternalUserDefinedFunction}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.udf.worker.UDFWorkerSpecification
import org.apache.spark.udf.worker.core.InitMessage

/**
 * :: Experimental ::
 * Physical plan node that executes a mapPartitions-style UDF in an
 * external worker process.
 *
 * @param workerSpec       Specification describing the UDF worker.
 * @param functionExpr     The UDF to invoke.
 * @param resultAttributes Output attributes produced by the UDF.
 * @param child            Child plan providing input partitions.
 */
@Experimental
case class MapPartitionExternalUDFExec(
    workerSpec: UDFWorkerSpecification,
    functionExpr: ExternalUserDefinedFunction,
    resultAttributes: Seq[Attribute],
    child: SparkPlan)
  extends ExternalUDFExec {

  override protected def doExecute(): RDD[InternalRow] = {
    child.execute().mapPartitionsInternal { rows =>
      val taskContext = TaskContext.get()
      withUDFWorkerSession(taskContext, securityScope = None) {
        session =>
          session.init(InitMessage(
            functionPayload = functionExpr.payload,
            inputSchema = Array.empty,
            outputSchema = Array.empty))
          session.close()
          // TODO [SPARK-55278]: Stream rows to/from the worker
          // via session.process().
          rows
      }
    }
  }

  override protected def withNewChildInternal(
      newChild: SparkPlan): MapPartitionExternalUDFExec =
    copy(child = newChild)
}
