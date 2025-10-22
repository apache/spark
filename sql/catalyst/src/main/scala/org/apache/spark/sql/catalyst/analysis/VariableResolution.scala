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

package org.apache.spark.sql.catalyst.analysis

import java.util.Locale

import org.apache.spark.sql.catalyst.{SQLConfHelper, SqlScriptingContextManager}
import org.apache.spark.sql.catalyst.catalog.TempVariableManager
import org.apache.spark.sql.catalyst.expressions.{
  Expression,
  ExtractValue,
  Literal,
  VariableReference
}
import org.apache.spark.sql.catalyst.parser.SqlScriptingLabelContext.isForbiddenLabelOrForVariableName
import org.apache.spark.sql.connector.catalog.{
  CatalogManager,
  Identifier
}

class VariableResolution(tempVariableManager: TempVariableManager) extends SQLConfHelper {

  /**
   * Resolves a `multipartName` to an [[Expression]] tree, supporting nested field access.
   *
   * This method implements a longest-match strategy similar to column resolution,
   * preferring fully qualified variable names to avoid naming conflicts. It supports
   * accessing nested fields within variables through dot notation.
   *
   * The resolution process works as follows:
   * 1. Attempts to resolve the full name as a variable
   * 2. If unsuccessful, treats the rightmost parts as nested field access
   * 3. Continues until a variable is found or all combinations are exhausted
   * 4. Wraps the result in ExtractValue expressions for nested field access
   *
   * @param nameParts The sequence of name parts representing the variable identifier
   *   (e.g., ["catalog", "schema", "variable", "field1", "field2"])
   * @param resolvingView Whether this resolution is happening within a view context.
   *   When true, only variables explicitly referred to in the view definition are accessible.
   * @param referredTempVariableNames When resolving within a view, this contains the list of
   *   variable names that the view explicitly references and should have access to.
   *
   * @return Some(Expression) if a variable is successfully resolved, potentially wrapped in
   *   [[ExtractValue]] expressions for nested field access. None if no variable can be resolved
   *   from the given name parts.
   */
  def resolveMultipartName(
      nameParts: Seq[String],
      resolvingView: Boolean,
      referredTempVariableNames: Seq[Seq[String]]): Option[Expression] = {
    var resolvedVariable: Option[Expression] = None
    // We only support temp variables for now, so the variable name can at most have 3 parts.
    var numInnerFields: Int = math.max(0, nameParts.length - 3)
    // Follow the column resolution and prefer the longest match. This makes sure that users
    // can always use fully qualified variable name to avoid name conflicts.
    while (resolvedVariable.isEmpty && numInnerFields < nameParts.length) {
      resolvedVariable = resolveVariable(
        nameParts = nameParts.dropRight(numInnerFields),
        resolvingView = resolvingView,
        referredTempVariableNames = referredTempVariableNames
      )

      if (resolvedVariable.isEmpty) {
        numInnerFields += 1
      }
    }

    resolvedVariable.map { variable =>
      if (numInnerFields != 0) {
        val nestedFields = nameParts.takeRight(numInnerFields)
        nestedFields.foldLeft(variable: Expression) { (e, name) =>
          ExtractValue(e, Literal(name), conf.resolver)
        }
      } else {
        variable
      }
    }
  }

  /**
   * Look up variable by nameParts.
   * If in SQL Script, first check local variables.
   * If not found fall back to session variables.
   * @param nameParts NameParts of the variable.
   * @return Reference to the variable.
   */
  def lookupVariable(nameParts: Seq[String]): Option[VariableReference] = {
    val namePartsCaseAdjusted = if (conf.caseSensitiveAnalysis) {
      nameParts
    } else {
      nameParts.map(_.toLowerCase(Locale.ROOT))
    }

    SqlScriptingContextManager
      .get()
      .map(_.getVariableManager)
      // If variable name is qualified with session.<varName> treat it as a session variable.
      .filterNot(
        _ =>
          nameParts.length > 2
          || (nameParts.length == 2 && isForbiddenLabelOrForVariableName(nameParts.head))
      )
      .flatMap(_.get(namePartsCaseAdjusted))
      .map { varDef =>
        VariableReference(
          nameParts,
          FakeLocalCatalog,
          Identifier.of(Array(varDef.identifier.namespace().last), namePartsCaseAdjusted.last),
          varDef
        )
      }
      .orElse(
        if (maybeTempVariableName(nameParts)) {
          tempVariableManager
            .get(namePartsCaseAdjusted)
            .map { varDef =>
              VariableReference(
                nameParts,
                FakeSystemCatalog,
                Identifier.of(Array(CatalogManager.SESSION_NAMESPACE), namePartsCaseAdjusted.last),
                varDef
              )
            }
        } else {
          None
        }
      )
  }

  private def resolveVariable(
      nameParts: Seq[String],
      resolvingView: Boolean,
      referredTempVariableNames: Seq[Seq[String]]): Option[Expression] = {
    if (resolvingView) {
      if (referredTempVariableNames.contains(nameParts)) {
        lookupVariable(nameParts = nameParts)
      } else {
        None
      }
    } else {
      lookupVariable(nameParts = nameParts)
    }
  }

  // The temp variables live in `SYSTEM.SESSION`, and the name can be qualified or not.
  private def maybeTempVariableName(nameParts: Seq[String]): Boolean = {
    nameParts.length == 1 || {
      if (nameParts.length == 2) {
        nameParts.head.equalsIgnoreCase(CatalogManager.SESSION_NAMESPACE)
      } else if (nameParts.length == 3) {
        nameParts(0).equalsIgnoreCase(CatalogManager.SYSTEM_CATALOG_NAME) &&
        nameParts(1).equalsIgnoreCase(CatalogManager.SESSION_NAMESPACE)
      } else {
        false
      }
    }
  }

}
