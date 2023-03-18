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

package org.apache.spark.sql.execution.datasources.v2

import org.apache.spark.sql.catalyst.{InternalRow, VariableIdentifier}
import org.apache.spark.sql.catalyst.analysis.NoSuchVariableException
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, Literal, VariableReference}
import org.apache.spark.sql.catalyst.trees.UnaryLike
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.types.StructField

/**
 * Physical plan node for setting a variable.
 */
case class SetVariableExec(variables: Seq[Expression],
             query: SparkPlan) extends V2CommandExec with UnaryLike[SparkPlan] {

  override def output: Seq[Attribute] = Seq.empty

  override protected def run(): Seq[InternalRow] = {

    val catalog = session.sessionState.catalog

    case class VarInfoObj(identifier: VariableIdentifier, fieldInfo: StructField)

    val varInfoList = variables.collect { case v: VariableReference =>
      val varIdentifier = VariableIdentifier(v.varName)
      val varInfo = catalog.getVariable(varIdentifier)
      if (varInfo.isEmpty) {
        throw new NoSuchVariableException(varIdentifier.nameParts)
      }
      VarInfoObj(varIdentifier, varInfo.get._2)
    }
    val array = query.executeCollect()
    if (array.length == 0) {
      varInfoList foreach(info => catalog.createTempVariable(info.identifier.variableName,
        Literal(null, info.fieldInfo.dataType),
        info.fieldInfo.getCurrentDefaultValue().get
        , overrideIfExists = true))
    } else if (array.length > 1) {
      throw QueryExecutionErrors.multipleRowSubqueryError()
    } else {
      val row = array(0)
      varInfoList.zipWithIndex.foreach { case (varInfo, index) =>
        val value = row.get(index, varInfo.fieldInfo.dataType)
        val valueType = varInfo.fieldInfo.dataType
        val name = varInfo.identifier.variableName
        val litValue = Literal.create(value, valueType)
        val defaultExpr = varInfo.fieldInfo.getCurrentDefaultValue().get
        catalog.createTempVariable(name, litValue, defaultExpr, overrideIfExists = true)
      }
    }
    Seq.empty
  }

  override def child: SparkPlan = query

  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan = {
    copy(query = newChild)
  }
}
