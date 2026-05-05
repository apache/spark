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

package org.apache.spark.sql.catalyst.plans.logical.externalUDF

import org.apache.spark.annotation.Experimental
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet}
import org.apache.spark.sql.catalyst.plans.logical.UnaryNode
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

  /** Output attributes produced by this UDF node. */
  def resultAttributes: Seq[Attribute]

  override def output: Seq[Attribute] = resultAttributes

  override val producedAttributes: AttributeSet = AttributeSet(resultAttributes)

  // The UDF may reference any column from the child output.
  // Explicitly include child.outputSet so the optimizer does not
  // prune input columns that the external worker needs.
  override lazy val references: AttributeSet = child.outputSet
}
