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
import org.apache.spark.sql.catalyst.expressions.{Attribute,
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
 * Logical plan node for mapPartitions-style UDF execution in an
 * external worker process.
 *
 * @param workerSpec       Specification describing the UDF worker.
 * @param function         The UDF to invoke. Output attributes are
 *                         derived from `function.dataType`.
 * @param isBarrier        Whether to use barrier execution.
 * @param child            Input relation whose partitions are processed.
 */
@Experimental
case class MapPartitionsExternalUDF(
    workerSpec: UDFWorkerSpecification,
    function: ExternalUserDefinedFunction,
    isBarrier: Boolean,
    child: LogicalPlan)
  extends ExternalUDF {

  // Map partitions always operate on StructTypes
  override def output: Seq[Attribute] = toAttributes(
    function.dataType.asInstanceOf[StructType]
  )

  override protected def withNewChildInternal(
      newChild: LogicalPlan): MapPartitionsExternalUDF =
    copy(child = newChild)
}
