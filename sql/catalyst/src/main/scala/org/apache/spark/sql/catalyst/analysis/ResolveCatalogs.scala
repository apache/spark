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

import scala.jdk.CollectionConverters._

import org.apache.spark.SparkException
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.SqlScriptingLocalVariableManager
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.util.SparkCharVarcharUtils.replaceCharVarcharWithString
import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.errors.DataTypeErrors.toSQLId
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.util.ArrayImplicits._

/**
 * Resolves the catalog of the name parts for table/view/function/namespace.
 */
class ResolveCatalogs(val catalogManager: CatalogManager)
  extends Rule[LogicalPlan] with LookupCatalog {

  override def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperatorsDown {
    // We only support temp variables for now and the system catalog is not properly implemented
    // yet. We need to resolve `UnresolvedIdentifier` for variable commands specially.
    case c @ CreateVariable(UnresolvedIdentifier(nameParts, _), _, _) =>
      // From scripts we can only create local variables, which must be unqualified,
      // and must not be DECLARE OR REPLACE.
      val resolved = if (withinSqlScript) {
        if (c.replace) {
          throw new AnalysisException(
            "INVALID_VARIABLE_DECLARATION.REPLACE_LOCAL_VARIABLE",
            Map("varName" -> toSQLId(nameParts))
          )
        }

        if (nameParts.length != 1) {
          throw new AnalysisException(
            "INVALID_VARIABLE_DECLARATION.QUALIFIED_LOCAL_VARIABLE",
            Map("varName" -> toSQLId(nameParts)))
        }

        SqlScriptingLocalVariableManager.get()
          .getOrElse(throw SparkException.internalError(
              "Scripting local variable manager should be present in SQL script."))
          .qualify(nameParts.last)
      } else {
        val resolvedIdentifier = catalogManager.tempVariableManager.qualify(nameParts.last)

        assertValidSessionVariableNameParts(nameParts, resolvedIdentifier)
        resolvedIdentifier
      }

      c.copy(name = resolved)
    case d @ DropVariable(UnresolvedIdentifier(nameParts, _), _) =>
      if (withinSqlScript) {
        throw new AnalysisException(
          "UNSUPPORTED_FEATURE.SQL_SCRIPTING_DROP_TEMPORARY_VARIABLE", Map.empty)
      }
      val resolved = catalogManager.tempVariableManager.qualify(nameParts.last)
      assertValidSessionVariableNameParts(nameParts, resolved)
      d.copy(name = resolved)

    // For CREATE TABLE and REPLACE TABLE statements, resolve the table identifier and include
    // the table columns as output. This allows expressions (e.g., constraints) referencing these
    // columns to be resolved correctly.
    case c @ CreateTable(UnresolvedIdentifier(nameParts, allowTemp), columns, _, _, _) =>
      val resolvedIdentifier = resolveIdentifier(nameParts, allowTemp, columns)
      c.copy(name = resolvedIdentifier)

    case r @ ReplaceTable(UnresolvedIdentifier(nameParts, allowTemp), columns, _, _, _) =>
      val resolvedIdentifier = resolveIdentifier(nameParts, allowTemp, columns)
      r.copy(name = resolvedIdentifier)

    case UnresolvedIdentifier(nameParts, allowTemp) =>
      resolveIdentifier(nameParts, allowTemp, Nil)

    case CurrentNamespace =>
      ResolvedNamespace(currentCatalog, catalogManager.currentNamespace.toImmutableArraySeq)
    case UnresolvedNamespace(Seq(), fetchMetadata) =>
      resolveNamespace(currentCatalog, Seq.empty[String], fetchMetadata)
    case UnresolvedNamespace(CatalogAndNamespace(catalog, ns), fetchMetadata) =>
      resolveNamespace(catalog, ns, fetchMetadata)
  }

  private def resolveIdentifier(
      nameParts: Seq[String],
      allowTemp: Boolean,
      columns: Seq[ColumnDefinition]): ResolvedIdentifier = {
    val columnOutput = columns.map { col =>
      val dataType = if (conf.preserveCharVarcharTypeInfo) {
        col.dataType
      } else {
        replaceCharVarcharWithString(col.dataType)
      }
      AttributeReference(col.name, dataType, col.nullable, col.metadata)()
    }
    if (allowTemp && catalogManager.v1SessionCatalog.isTempView(nameParts)) {
      val ident = Identifier.of(nameParts.dropRight(1).toArray, nameParts.last)
      ResolvedIdentifier(FakeSystemCatalog, ident, columnOutput)
    } else {
      val CatalogAndIdentifier(catalog, identifier) = nameParts
      ResolvedIdentifier(catalog, identifier, columnOutput)
    }
  }

  private def resolveNamespace(
      catalog: CatalogPlugin,
      ns: Seq[String],
      fetchMetadata: Boolean): ResolvedNamespace = {
    catalog match {
      case supportsNS: SupportsNamespaces if fetchMetadata =>
        ResolvedNamespace(
          catalog,
          ns,
          supportsNS.loadNamespaceMetadata(ns.toArray).asScala.toMap)
      case _ =>
        ResolvedNamespace(catalog, ns)
    }
  }

  private def withinSqlScript: Boolean =
    SqlScriptingLocalVariableManager.get().isDefined && !AnalysisContext.get.isExecuteImmediate

  private def assertValidSessionVariableNameParts(
      nameParts: Seq[String],
      resolvedIdentifier: ResolvedIdentifier): Unit = {
    if (!validSessionVariableName(nameParts)) {
      throw QueryCompilationErrors.unresolvedVariableError(
        nameParts,
        Seq(
          resolvedIdentifier.catalog.name(),
          resolvedIdentifier.identifier.namespace().head)
      )
    }

    def validSessionVariableName(nameParts: Seq[String]): Boolean = nameParts.length match {
      case 1 => true

      // On declare variable, local variables support only unqualified names.
      // On drop variable, local variables are not supported at all.
      case 2 if nameParts.head.equalsIgnoreCase(CatalogManager.SESSION_NAMESPACE) => true

      // When there are 3 nameParts the variable must be a fully qualified session variable
      // i.e. "system.session.<varName>"
      case 3 if nameParts(0).equalsIgnoreCase(CatalogManager.SYSTEM_CATALOG_NAME) &&
        nameParts(1).equalsIgnoreCase(CatalogManager.SESSION_NAMESPACE) => true

      case _ => false
    }
  }
}
