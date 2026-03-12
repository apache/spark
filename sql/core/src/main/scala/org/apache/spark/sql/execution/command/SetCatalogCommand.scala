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

package org.apache.spark.sql.execution.command

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.analysis.IdentifierResolution
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.errors.QueryCompilationErrors

/**
 * The command for `SET CATALOG XXX`.
 *
 * Supports the following expression forms:
*   - Session temp variable: SET CATALOG var_name (where var_name is a declared variable)
*   - Simple identifier: SET CATALOG my_catalog
*     (tries to resolve as session temp variable first, then uses as literal catalog name)
 *   - String literal: SET CATALOG 'my_catalog'
 *   - identifier() function: SET CATALOG identifier('my_catalog')
 *   - CAST, CONCAT, or other foldable expressions:
 *     SET CATALOG CAST('my_catalog' AS STRING)
 */
case class SetCatalogCommand(catalogNameExpr: Expression)
  extends LeafRunnableCommand {
  override def output: Seq[Attribute] = Seq.empty

  /**
   * Extracts the catalog name from the catalogNameExpr.
   * This method evaluates the expression and returns the catalog name string.
   */
  def getCatalogName(): String = {
    // Use IdentifierResolution to evaluate and validate the expression.
    // This handles: foldability, StringType validation, null checks, and
    // parsing. By this point, all expressions (including identifier()) are
    // resolved by earlier analysis rules, so evalIdentifierExpr will work
    // correctly.
    val nameParts = IdentifierResolution.evalIdentifierExpr(catalogNameExpr)
    if (nameParts.length > 1) {
      throw QueryCompilationErrors.multipartCatalogNameNotAllowed(nameParts)
    }
    nameParts.head
  }

  override def run(sparkSession: SparkSession): Seq[Row] = {
    sparkSession.sessionState.catalogManager.setCurrentCatalog(getCatalogName())
    Seq.empty
  }
}
