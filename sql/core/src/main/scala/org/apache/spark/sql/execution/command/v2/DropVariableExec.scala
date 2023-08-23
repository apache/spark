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

package org.apache.spark.sql.execution.command.v2

import java.util.Locale

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.connector.catalog.CatalogManager
import org.apache.spark.sql.errors.DataTypeErrorsBase
import org.apache.spark.sql.execution.datasources.v2.LeafV2CommandExec

/**
 * Physical plan node for dropping a variable.
 */
case class DropVariableExec(name: String, ifExists: Boolean) extends LeafV2CommandExec
  with DataTypeErrorsBase {

  override protected def run(): Seq[InternalRow] = {
    val variableManager = session.sessionState.catalogManager.tempVariableManager
    val normalizedName = if (session.sessionState.conf.caseSensitiveAnalysis) {
      name
    } else {
      name.toLowerCase(Locale.ROOT)
    }
    if (!variableManager.remove(normalizedName)) {
      // The variable does not exist
      if (!ifExists) {
        throw new AnalysisException(
          errorClass = "VARIABLE_NOT_FOUND",
          Map("variableName" -> toSQLId(
            Seq(CatalogManager.SYSTEM_CATALOG_NAME, CatalogManager.SESSION_NAMESPACE, name))))
      }
    }
    Nil
  }

  override def output: Seq[Attribute] = Nil
}
