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

package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.annotation.Experimental
import org.apache.spark.sql.catalyst.trees.TreePattern.{EXTERNAL_UDF, TreePattern}
import org.apache.spark.sql.types.DataType

/**
 * :: Experimental ::
 * A serialized external UDF that is executed in an external worker process
 * via the language-agnostic UDF worker framework.
 *
 * This is a Catalyst expression analogous to [[PythonUDF]] but
 * language-agnostic. The [[payload]] carries an opaque serialized
 * function definition whose interpretation is left to the worker.
 * The optional [[inputTypes]] declare the expected argument types for
 * validation during analysis; when absent, any input types are accepted.
 *
 * This expression is [[Unevaluable]] and requires a dedicated physical
 * operator (e.g. [[org.apache.spark.sql.execution.externalUDF.MapPartitionsExternalUDFExec]])
 * to execute.
 *
 * @param name             Optional name of the UDF.
 * @param payload          Opaque serialized function definition.
 * @param dataType         Return type of the UDF.
 * @param children         Input argument expressions.
 * @param inputTypes       Optional declared input types for validation.
 * @param udfDeterministic Whether this UDF is deterministic.
 * @param udfNullable      Whether this UDF can return null.
 * @param resultId         Unique expression ID for this invocation.
 */
@Experimental
case class ExternalUserDefinedFunction(
    name: Option[String],
    payload: Array[Byte],
    dataType: DataType,
    children: Seq[Expression],
    inputTypes: Option[Seq[DataType]] = None,
    udfDeterministic: Boolean,
    udfNullable: Boolean,
    resultId: ExprId = NamedExpression.newExprId)
  extends Expression with NonSQLExpression with Unevaluable {

  override lazy val deterministic: Boolean = udfDeterministic && children.forall(_.deterministic)

  override def nullable: Boolean = udfNullable

  override lazy val canonicalized: Expression = {
    val canonicalizedChildren = children.map(_.canonicalized)
    // `resultId` can be seen as cosmetic variation in ExternalUserDefinedFunction,
    // as it doesn't affect the result.
    this.copy(resultId = ExprId(-1)).withNewChildren(canonicalizedChildren)
  }

  final override val nodePatterns: Seq[TreePattern] = Seq(EXTERNAL_UDF)

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): ExternalUserDefinedFunction =
    copy(children = newChildren)
}
