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

package org.apache.spark.sql.connector.catalog

import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.catalyst.util.quoteIfNeeded
import org.apache.spark.sql.connector.expressions.{BucketTransform, IdentityTransform, LogicalExpressions, Transform}
import org.apache.spark.sql.errors.QueryCompilationErrors

/**
 * Conversion helpers for working with v2 [[CatalogPlugin]].
 */
private[sql] object CatalogV2Implicits {
  import LogicalExpressions._

  implicit class PartitionTypeHelper(colNames: Seq[String]) {
    def asTransforms: Array[Transform] = {
      colNames.map(col => identity(reference(Seq(col)))).toArray
    }
  }

  implicit class BucketSpecHelper(spec: BucketSpec) {
    def asTransform: BucketTransform = {
      val references = spec.bucketColumnNames.map(col => reference(Seq(col)))
      if (spec.sortColumnNames.nonEmpty) {
        val sortedCol = spec.sortColumnNames.map(col => reference(Seq(col)))
        bucket(spec.numBuckets, references.toArray, sortedCol.toArray)
      } else {
        bucket(spec.numBuckets, references.toArray)
      }
    }
  }

  implicit class TransformHelper(transforms: Seq[Transform]) {
    def asPartitionColumns: Seq[String] = {
      val (idTransforms, nonIdTransforms) = transforms.partition(_.isInstanceOf[IdentityTransform])

      if (nonIdTransforms.nonEmpty) {
        throw QueryCompilationErrors.cannotConvertTransformsToPartitionColumnsError(nonIdTransforms)
      }

      idTransforms.map(_.asInstanceOf[IdentityTransform]).map(_.reference).map { ref =>
        val parts = ref.fieldNames
        if (parts.size > 1) {
          throw QueryCompilationErrors.cannotPartitionByNestedColumnError(ref)
        } else {
          parts(0)
        }
      }
    }
  }

  implicit class CatalogHelper(plugin: CatalogPlugin) {
    def asTableCatalog: TableCatalog = plugin match {
      case tableCatalog: TableCatalog =>
        tableCatalog
      case _ =>
        throw QueryCompilationErrors.cannotUseCatalogError(plugin, "not a TableCatalog")
    }

    def asNamespaceCatalog: SupportsNamespaces = plugin match {
      case namespaceCatalog: SupportsNamespaces =>
        namespaceCatalog
      case _ =>
        throw QueryCompilationErrors.cannotUseCatalogError(plugin, "does not support namespaces")
    }

    def isFunctionCatalog: Boolean = plugin match {
      case _: FunctionCatalog => true
      case _ => false
    }

    def asFunctionCatalog: FunctionCatalog = plugin match {
      case functionCatalog: FunctionCatalog =>
        functionCatalog
      case _ =>
        throw QueryCompilationErrors.cannotUseCatalogError(plugin, "not a FunctionCatalog")
    }
  }

  implicit class NamespaceHelper(namespace: Array[String]) {
    def quoted: String = namespace.map(quoteIfNeeded).mkString(".")
  }

  implicit class FunctionIdentifierHelper(ident: FunctionIdentifier) {
    def asMultipart: Seq[String] = {
      ident.database match {
        case Some(db) =>
          Seq(db, ident.funcName)
        case _ =>
          Seq(ident.funcName)
      }
    }
  }

  implicit class IdentifierHelper(ident: Identifier) {
    def quoted: String = {
      if (ident.namespace.nonEmpty) {
        ident.namespace.map(quoteIfNeeded).mkString(".") + "." + quoteIfNeeded(ident.name)
      } else {
        quoteIfNeeded(ident.name)
      }
    }

    def asMultipartIdentifier: Seq[String] = ident.namespace :+ ident.name

    def asTableIdentifier: TableIdentifier = ident.namespace match {
      case ns if ns.isEmpty => TableIdentifier(ident.name)
      case Array(dbName) => TableIdentifier(ident.name, Some(dbName))
      case _ =>
        throw QueryCompilationErrors.identifierHavingMoreThanTwoNamePartsError(
          quoted, "TableIdentifier")
    }

    def asFunctionIdentifier: FunctionIdentifier = ident.namespace() match {
      case ns if ns.isEmpty => FunctionIdentifier(ident.name())
      case Array(dbName) => FunctionIdentifier(ident.name(), Some(dbName))
      case _ =>
        throw QueryCompilationErrors.identifierHavingMoreThanTwoNamePartsError(
          quoted, "FunctionIdentifier")
    }
  }

  implicit class MultipartIdentifierHelper(parts: Seq[String]) {
    if (parts.isEmpty) {
      throw QueryCompilationErrors.emptyMultipartIdentifierError()
    }

    def asIdentifier: Identifier = Identifier.of(parts.init.toArray, parts.last)

    def asTableIdentifier: TableIdentifier = parts match {
      case Seq(tblName) => TableIdentifier(tblName)
      case Seq(dbName, tblName) => TableIdentifier(tblName, Some(dbName))
      case _ =>
        throw QueryCompilationErrors.identifierHavingMoreThanTwoNamePartsError(
          quoted, "TableIdentifier")
    }

    def asFunctionIdentifier: FunctionIdentifier = parts match {
      case Seq(funcName) => FunctionIdentifier(funcName)
      case Seq(dbName, funcName) => FunctionIdentifier(funcName, Some(dbName))
      case _ =>
        throw QueryCompilationErrors.identifierHavingMoreThanTwoNamePartsError(
          quoted, "FunctionIdentifier")
    }

    def quoted: String = parts.map(quoteIfNeeded).mkString(".")
  }

  def parseColumnPath(name: String): Seq[String] = {
    CatalystSqlParser.parseMultipartIdentifier(name)
  }
}
