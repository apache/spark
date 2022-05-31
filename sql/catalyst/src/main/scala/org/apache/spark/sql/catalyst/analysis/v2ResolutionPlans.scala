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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.expressions.{Attribute, LeafExpression, Unevaluable}
import org.apache.spark.sql.catalyst.plans.logical.{LeafNode, Statistics}
import org.apache.spark.sql.catalyst.trees.TreePattern.{TreePattern, UNRESOLVED_FUNC}
import org.apache.spark.sql.catalyst.util.CharVarcharUtils
import org.apache.spark.sql.connector.catalog.{CatalogPlugin, FunctionCatalog, Identifier, Table, TableCatalog}
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._
import org.apache.spark.sql.connector.catalog.TableChange.ColumnPosition
import org.apache.spark.sql.connector.catalog.functions.UnboundFunction
import org.apache.spark.sql.types.{DataType, StructField}

/**
 * Holds the name of a namespace that has yet to be looked up in a catalog. It will be resolved to
 * [[ResolvedNamespace]] during analysis.
 */
case class UnresolvedNamespace(multipartIdentifier: Seq[String]) extends LeafNode {
  override lazy val resolved: Boolean = false

  override def output: Seq[Attribute] = Nil
}

/**
 * Holds the name of a table that has yet to be looked up in a catalog. It will be resolved to
 * [[ResolvedTable]] during analysis.
 */
case class UnresolvedTable(
    multipartIdentifier: Seq[String],
    commandName: String,
    relationTypeMismatchHint: Option[String]) extends LeafNode {
  override lazy val resolved: Boolean = false

  override def output: Seq[Attribute] = Nil
}

/**
 * Holds the name of a view that has yet to be looked up. It will be resolved to
 * [[ResolvedView]] during analysis.
 */
case class UnresolvedView(
    multipartIdentifier: Seq[String],
    commandName: String,
    allowTemp: Boolean,
    relationTypeMismatchHint: Option[String]) extends LeafNode {
  override lazy val resolved: Boolean = false

  override def output: Seq[Attribute] = Nil
}

/**
 * Holds the name of a table or view that has yet to be looked up in a catalog. It will
 * be resolved to [[ResolvedTable]] or [[ResolvedView]] during analysis.
 */
case class UnresolvedTableOrView(
    multipartIdentifier: Seq[String],
    commandName: String,
    allowTempView: Boolean) extends LeafNode {
  override lazy val resolved: Boolean = false
  override def output: Seq[Attribute] = Nil
}

sealed trait PartitionSpec extends LeafExpression with Unevaluable {
  override def dataType: DataType = throw new IllegalStateException(
    "PartitionSpec.dataType should not be called.")
  override def nullable: Boolean = throw new IllegalStateException(
    "PartitionSpec.nullable should not be called.")
}

case class UnresolvedPartitionSpec(
    spec: TablePartitionSpec,
    location: Option[String] = None) extends PartitionSpec {
  override lazy val resolved = false
}

sealed trait FieldName extends LeafExpression with Unevaluable {
  def name: Seq[String]
  override def dataType: DataType = throw new IllegalStateException(
    "FieldName.dataType should not be called.")
  override def nullable: Boolean = throw new IllegalStateException(
    "FieldName.nullable should not be called.")
}

case class UnresolvedFieldName(name: Seq[String]) extends FieldName {
  override lazy val resolved = false
}

sealed trait FieldPosition extends LeafExpression with Unevaluable {
  def position: ColumnPosition
  override def dataType: DataType = throw new IllegalStateException(
    "FieldPosition.dataType should not be called.")
  override def nullable: Boolean = throw new IllegalStateException(
    "FieldPosition.nullable should not be called.")
}

case class UnresolvedFieldPosition(position: ColumnPosition) extends FieldPosition {
  override lazy val resolved = false
}

/**
 * Holds the name of a function that has yet to be looked up. It will be resolved to
 * [[ResolvedPersistentFunc]] or [[ResolvedNonPersistentFunc]] during analysis.
 */
case class UnresolvedFunc(
    multipartIdentifier: Seq[String],
    commandName: String,
    requirePersistent: Boolean,
    funcTypeMismatchHint: Option[String],
    possibleQualifiedName: Option[Seq[String]] = None) extends LeafNode {
  override lazy val resolved: Boolean = false
  override def output: Seq[Attribute] = Nil
  final override val nodePatterns: Seq[TreePattern] = Seq(UNRESOLVED_FUNC)
}

/**
 * Holds the name of a database object (table, view, namespace, function, etc.) that is to be
 * created and we need to determine the catalog to store it. It will be resolved to
 * [[ResolvedDBObjectName]] during analysis.
 */
case class UnresolvedDBObjectName(nameParts: Seq[String], isNamespace: Boolean) extends LeafNode {
  override lazy val resolved: Boolean = false
  override def output: Seq[Attribute] = Nil
}

/**
 * A resolved leaf node whose statistics has no meaning.
 */
trait LeafNodeWithoutStats extends LeafNode {
  // Here we just return a dummy statistics to avoid compute statsCache
  override def stats: Statistics = Statistics.DUMMY
}

/**
 * A plan containing resolved namespace.
 */
case class ResolvedNamespace(catalog: CatalogPlugin, namespace: Seq[String])
  extends LeafNodeWithoutStats {
  override def output: Seq[Attribute] = Nil
}

/**
 * A plan containing resolved table.
 */
case class ResolvedTable(
    catalog: TableCatalog,
    identifier: Identifier,
    table: Table,
    outputAttributes: Seq[Attribute])
  extends LeafNodeWithoutStats {
  override def output: Seq[Attribute] = {
    val qualifier = catalog.name +: identifier.namespace :+ identifier.name
    outputAttributes.map(_.withQualifier(qualifier))
  }
  def name: String = (catalog.name +: identifier.namespace() :+ identifier.name()).quoted
}

object ResolvedTable {
  def create(
      catalog: TableCatalog,
      identifier: Identifier,
      table: Table): ResolvedTable = {
    val schema = CharVarcharUtils.replaceCharVarcharWithStringInSchema(table.schema)
    ResolvedTable(catalog, identifier, table, schema.toAttributes)
  }
}

case class ResolvedPartitionSpec(
    names: Seq[String],
    ident: InternalRow,
    location: Option[String] = None) extends PartitionSpec

case class ResolvedFieldName(path: Seq[String], field: StructField) extends FieldName {
  def name: Seq[String] = path :+ field.name
}

case class ResolvedFieldPosition(position: ColumnPosition) extends FieldPosition


/**
 * A plan containing resolved (temp) views.
 */
// TODO: create a generic representation for temp view, v1 view and v2 view, after we add view
//       support to v2 catalog. For now we only need the identifier to fallback to v1 command.
case class ResolvedView(identifier: Identifier, isTemp: Boolean) extends LeafNodeWithoutStats {
  override def output: Seq[Attribute] = Nil
}

/**
 * A plan containing resolved persistent function.
 */
case class ResolvedPersistentFunc(
    catalog: FunctionCatalog,
    identifier: Identifier,
    func: UnboundFunction)
  extends LeafNodeWithoutStats {
  override def output: Seq[Attribute] = Nil
}

/**
 * A plan containing resolved non-persistent (temp or built-in) function.
 */
case class ResolvedNonPersistentFunc(
    name: String,
    func: UnboundFunction)
  extends LeafNodeWithoutStats {
  override def output: Seq[Attribute] = Nil
}

/**
 * A plan containing resolved database object name with catalog determined.
 */
case class ResolvedDBObjectName(
    catalog: CatalogPlugin,
    nameParts: Seq[String])
  extends LeafNodeWithoutStats {
  override def output: Seq[Attribute] = Nil
}
