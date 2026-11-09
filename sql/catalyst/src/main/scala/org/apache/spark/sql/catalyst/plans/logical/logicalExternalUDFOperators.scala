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

package org.apache.spark.sql.catalyst.plans.logical

import org.apache.spark.annotation.Experimental
import org.apache.spark.resource.ResourceProfile
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet,
  ExternalUserDefinedFunction}
import org.apache.spark.sql.catalyst.types.DataTypeUtils.toAttributes
import org.apache.spark.sql.types.StructType
import org.apache.spark.udf.worker.UDFWorkerSpecification

/**
 * :: Experimental ::
 * Base trait for logical plan nodes representing UDFs that are executed
 * in an external worker process. This covers Python UDFs, and any future
 * UDF languages that use the language-agnostic UDF worker framework.
 */
@Experimental
trait ExternalUDF extends UnaryNode {

  /** Specification describing how to create and communicate with the UDF worker. */
  def workerSpec: UDFWorkerSpecification
}

/**
 * :: Experimental ::
 * Logical plan node for evaluating one scalar UDF expression in an external
 * worker session.
 *
 * @param udf UDF expression evaluated by the worker session.
 * @param resultAttr Output attribute for the UDF expression.
 * @param child Input relation for the UDF.
 */
@Experimental
case class ExecuteExternalUDF(
    udf: ExternalUserDefinedFunction,
    resultAttr: Attribute,
    child: LogicalPlan)
  extends ExternalUDF {

  override def workerSpec: UDFWorkerSpecification = udf.workerSpec

  assert(udf.dataType == resultAttr.dataType && udf.nullable == resultAttr.nullable,
    "The UDF and result attribute must have matching types and nullability")
  // TODO(SPARK-59049): Support UDF chaining before allowing nested UDFs in one node.
  assert(!udf.children.exists(_.exists(_.isInstanceOf[ExternalUserDefinedFunction])),
    "Nested external UDFs must use separate evaluation nodes")

  override def output: Seq[Attribute] = child.output :+ resultAttr

  override def producedAttributes: AttributeSet = AttributeSet(Seq(resultAttr))

  override def maxRows: Option[Long] = child.maxRows

  override def maxRowsPerPartition: Option[Long] = child.maxRowsPerPartition

  override protected def withNewChildInternal(newChild: LogicalPlan): ExecuteExternalUDF =
    copy(child = newChild)
}

/**
 * :: Experimental ::
 * Logical plan node for mapPartitions-style UDF execution in an
 * external worker process.
 *
 * @param function         The UDF to invoke. Output attributes are
 *                         derived from `function.dataType`.
 * @param isBarrier        Whether to use barrier execution.
 * @param profile          Optional resource profile for the UDF execution.
 * @param child            Input relation whose partitions are processed.
 */
@Experimental
case class MapPartitionsExternalUDF(
    function: ExternalUserDefinedFunction,
    isBarrier: Boolean,
    profile: Option[ResourceProfile],
    child: LogicalPlan)
  extends ExternalUDF {

  override def workerSpec: UDFWorkerSpecification = function.workerSpec

  val nodeOutputAttributes = toAttributes(
    function.dataType.asInstanceOf[StructType]
  )

  // Map partitions always operate on StructTypes
  override def output: Seq[Attribute] = nodeOutputAttributes

  override protected def withNewChildInternal(
      newChild: LogicalPlan): MapPartitionsExternalUDF =
    copy(child = newChild)
}
