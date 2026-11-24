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

import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.catalyst.plans.logical.ColumnDefinition
import org.apache.spark.sql.catalyst.trees.CurrentOrigin
import org.apache.spark.sql.catalyst.util.SparkCharVarcharUtils.replaceCharVarcharWithString
import org.apache.spark.sql.connector.catalog.{CatalogManager, CatalogPlugin, Identifier, LookupCatalog, SupportsNamespaces}
import org.apache.spark.util.ArrayImplicits._

/**
 * Resolves command identifiers and namespaces for [[ResolveCatalogs]].
 */
class CatalogIdentifierResolution(override val catalogManager: CatalogManager)
    extends LookupCatalog with SQLConfHelper {

  /** Resolves a command target without column output, preserving its origin and tags. */
  def resolveUnresolvedIdentifier(
      unresolvedIdentifier: UnresolvedIdentifier): ResolvedIdentifier = {
    CurrentOrigin.withOrigin(unresolvedIdentifier.origin) {
      val resolvedIdentifier = resolveIdentifier(
        nameParts = unresolvedIdentifier.nameParts,
        allowTemp = unresolvedIdentifier.allowTemp,
        columnOutput = Nil)
      resolvedIdentifier.copyTagsFrom(unresolvedIdentifier)
      resolvedIdentifier
    }
  }

  /** A temporary view resolves to [[FakeSystemCatalog]] when `allowTemp` is set. */
  def resolveIdentifier(
      nameParts: Seq[String],
      allowTemp: Boolean,
      columnOutput: Seq[Attribute]): ResolvedIdentifier = {
    if (allowTemp && catalogManager.v1SessionCatalog.isTempView(nameParts)) {
      val identifier = Identifier.of(nameParts.dropRight(1).toArray, nameParts.last)
      ResolvedIdentifier(FakeSystemCatalog, identifier, columnOutput)
    } else {
      val CatalogAndIdentifier(catalog, identifier) = nameParts
      ResolvedIdentifier(catalog, identifier, columnOutput)
    }
  }

  /** Resolves the current catalog and namespace, e.g. the target of `SHOW TABLES`. */
  def resolveCurrentNamespace(): ResolvedNamespace = {
    ResolvedNamespace(currentCatalog, catalogManager.currentNamespace.toImmutableArraySeq)
  }

  /**
   * Resolves a namespace name, preserving its origin and tags and loading metadata on request.
   */
  def resolveUnresolvedNamespace(unresolvedNamespace: UnresolvedNamespace): ResolvedNamespace = {
    CurrentOrigin.withOrigin(unresolvedNamespace.origin) {
      val resolvedNamespace = unresolvedNamespace match {
        case UnresolvedNamespace(Seq(), fetchMetadata) =>
          resolveNamespace(currentCatalog, Seq.empty[String], fetchMetadata)
        case UnresolvedNamespace(CatalogAndNamespace(catalog, namespace), fetchMetadata) =>
          resolveNamespace(catalog, namespace, fetchMetadata)
      }
      resolvedNamespace.copyTagsFrom(unresolvedNamespace)
      resolvedNamespace
    }
  }

  /** Converts column definitions to output attributes, honoring CHAR/VARCHAR configuration. */
  def columnsToOutput(columns: Seq[ColumnDefinition]): Seq[AttributeReference] = {
    columns.map { col =>
      val dataType = if (conf.charVarcharFirstClassTypes) {
        col.dataType
      } else {
        replaceCharVarcharWithString(col.dataType)
      }
      AttributeReference(col.name, dataType, col.nullable, col.metadata)()
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
}
