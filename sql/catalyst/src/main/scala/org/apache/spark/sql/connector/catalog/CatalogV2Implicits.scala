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

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.connector.expressions.{BucketTransform, IdentityTransform, LogicalExpressions, Transform}

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
      if (spec.sortColumnNames.nonEmpty) {
        throw new AnalysisException(
          s"Cannot convert bucketing with sort columns to a transform: $spec")
      }

      val references = spec.bucketColumnNames.map(col => reference(Seq(col)))
      bucket(spec.numBuckets, references.toArray)
    }
  }

  implicit class TransformHelper(transforms: Seq[Transform]) {
    def asPartitionColumns: Seq[String] = {
      val (idTransforms, nonIdTransforms) = transforms.partition(_.isInstanceOf[IdentityTransform])

      if (nonIdTransforms.nonEmpty) {
        throw new AnalysisException("Transforms cannot be converted to partition columns: " +
            nonIdTransforms.map(_.describe).mkString(", "))
      }

      idTransforms.map(_.asInstanceOf[IdentityTransform]).map(_.reference).map { ref =>
        val parts = ref.fieldNames
        if (parts.size > 1) {
          throw new AnalysisException(s"Cannot partition by nested column: $ref")
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
        throw new AnalysisException(s"Cannot use catalog ${plugin.name}: not a TableCatalog")
    }

    def asNamespaceCatalog: SupportsNamespaces = plugin match {
      case namespaceCatalog: SupportsNamespaces =>
        namespaceCatalog
      case _ =>
        throw new AnalysisException(
          s"Cannot use catalog ${plugin.name}: does not support namespaces")
    }
  }

  implicit class NamespaceHelper(namespace: Array[String]) {
    def quoted: String = namespace.map(quoteIfNeeded).mkString(".")
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
        throw new AnalysisException(
          s"$quoted is not a valid TableIdentifier as it has more than 2 name parts.")
    }

    def asFunctionIdentifier: FunctionIdentifier = ident.namespace() match {
      case ns if ns.isEmpty => FunctionIdentifier(ident.name())
      case Array(dbName) => FunctionIdentifier(ident.name(), Some(dbName))
      case _ =>
        throw new AnalysisException(
          s"$quoted is not a valid FunctionIdentifier as it has more than 2 name parts.")
    }
  }

  implicit class MultipartIdentifierHelper(parts: Seq[String]) {
    if (parts.isEmpty) {
      throw new AnalysisException("multi-part identifier cannot be empty.")
    }

    def asIdentifier: Identifier = Identifier.of(parts.init.toArray, parts.last)

    def asTableIdentifier: TableIdentifier = parts match {
      case Seq(tblName) => TableIdentifier(tblName)
      case Seq(dbName, tblName) => TableIdentifier(tblName, Some(dbName))
      case _ =>
        throw new AnalysisException(
          s"$quoted is not a valid TableIdentifier as it has more than 2 name parts.")
    }

    def quoted: String = parts.map(quoteIfNeeded).mkString(".")
  }

  def quoteIfNeeded(part: String): String = {
    if (part.contains(".") || part.contains("`")) {
      s"`${part.replace("`", "``")}`"
    } else {
      part
    }
  }

  def parseColumnPath(name: String): Seq[String] = {
    CatalystSqlParser.parseMultipartIdentifier(name)
  }
}
