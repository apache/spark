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

import org.apache.spark.annotation.Experimental
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{
  Attribute,
  AttributeSet,
  ExternalUserDefinedFunction
}
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.udf.worker.UDFWorkerSpecification

/**
 * :: Experimental ::
 * Physical plan node that evaluates one scalar UDF in an external worker process.
 *
 * @param udf UDF expression evaluated by the worker session.
 * @param resultAttr Output attribute for the UDF expression.
 * @param child Child plan providing input rows.
 */
@Experimental
case class ExecuteExternalUDFExec(
    udf: ExternalUserDefinedFunction,
    resultAttr: Attribute,
    child: SparkPlan)
  extends ExternalUDFExec {

  override def workerSpec: UDFWorkerSpecification = udf.workerSpec

  override def output: Seq[Attribute] = child.output :+ resultAttr

  override def producedAttributes: AttributeSet = AttributeSet(Seq(resultAttr))

  override protected def doExecute(): RDD[InternalRow] = {
    // TODO(SPARK-55278): Stream rows to and from the worker through session.process().
    throw QueryExecutionErrors.methodNotImplementedError(
      "ExecuteExternalUDFExec.doExecute")
  }

  override protected def withNewChildInternal(newChild: SparkPlan): ExecuteExternalUDFExec =
    copy(child = newChild)
}
