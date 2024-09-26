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

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.expressions.{Attribute, LeafExpression, Unevaluable}
import org.apache.spark.sql.catalyst.plans.logical.{LeafNode, Statistics}
import org.apache.spark.sql.catalyst.trees.TreePattern.{TreePattern, UNRESOLVED_FUNC, UNRESOLVED_PROCEDURE}
import org.apache.spark.sql.catalyst.types.DataTypeUtils.toAttributes
import org.apache.spark.sql.catalyst.util.CharVarcharUtils
import org.apache.spark.sql.connector.catalog.{CatalogPlugin, FunctionCatalog, Identifier, ProcedureCatalog, Table, TableCatalog}
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._
import org.apache.spark.sql.connector.catalog.TableChange.ColumnPosition
import org.apache.spark.sql.connector.catalog.functions.UnboundFunction
import org.apache.spark.sql.connector.catalog.procedures.Procedure
import org.apache.spark.sql.types.{DataType, StructField}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.util.ArrayImplicits._

/**
 * Holds the name of a namespace that has yet to be looked up in a catalog. It will be resolved to
 * [[ResolvedNamespace]] during analysis.
 */
case class UnresolvedNamespace(
    multipartIdentifier: Seq[String],
    fetchMetadata: Boolean = false) extends UnresolvedLeafNode

/**
 * A variant of [[UnresolvedNamespace]] that should be resolved to [[ResolvedNamespace]]
 * representing the current namespace of the current catalog.
 */
case object CurrentNamespace extends UnresolvedLeafNode

/**
 * Holds the name of a table that has yet to be looked up in a catalog. It will be resolved to
 * [[ResolvedTable]] during analysis.
 */
case class UnresolvedTable(
    multipartIdentifier: Seq[String],
    commandName: String,
    suggestAlternative: Boolean = false) extends UnresolvedLeafNode

/**
 * Holds the name of a view that has yet to be looked up. It will be resolved to
 * [[ResolvedPersistentView]] or [[ResolvedTempView]] during analysis.
 */
case class UnresolvedView(
    multipartIdentifier: Seq[String],
    commandName: String,
    allowTemp: Boolean,
    suggestAlternative: Boolean = false) extends UnresolvedLeafNode

/**
 * Holds the name of a table or view that has yet to be looked up in a catalog. It will
 * be resolved to [[ResolvedTable]], [[ResolvedPersistentView]] or [[ResolvedTempView]] during
 * analysis.
 */
case class UnresolvedTableOrView(
    multipartIdentifier: Seq[String],
    commandName: String,
    allowTempView: Boolean) extends UnresolvedLeafNode

sealed trait PartitionSpec extends LeafExpression with Unevaluable {
  override def dataType: DataType = throw SparkException.internalError(
    "PartitionSpec.dataType should not be called.")
  override def nullable: Boolean = throw SparkException.internalError(
    "PartitionSpec.nullable should not be called.")
}

case class UnresolvedPartitionSpec(
    spec: TablePartitionSpec,
    location: Option[String] = None) extends PartitionSpec {
  override lazy val resolved = false
}

sealed trait FieldName extends LeafExpression with Unevaluable {
  def name: Seq[String]
  override def dataType: DataType = throw SparkException.internalError(
    "FieldName.dataType should not be called.")
  override def nullable: Boolean = throw SparkException.internalError(
    "FieldName.nullable should not be called.")
}

case class UnresolvedFieldName(name: Seq[String]) extends FieldName {
  override lazy val resolved = false
}

sealed trait FieldPosition extends LeafExpression with Unevaluable {
  def position: ColumnPosition
  override def dataType: DataType = throw SparkException.internalError(
    "FieldPosition.dataType should not be called.")
  override def nullable: Boolean = throw SparkException.internalError(
    "FieldPosition.nullable should not be called.")
}

case class UnresolvedFieldPosition(position: ColumnPosition) extends FieldPosition {
  override lazy val resolved = false
}

/**
 * Holds the name of a function that has yet to be looked up. It will be resolved to
 * [[ResolvedPersistentFunc]] or [[ResolvedNonPersistentFunc]] during analysis of function-related
 * commands such as `DESCRIBE FUNCTION name`.
 */
case class UnresolvedFunctionName(
    multipartIdentifier: Seq[String],
    commandName: String,
    requirePersistent: Boolean,
    funcTypeMismatchHint: Option[String],
    possibleQualifiedName: Option[Seq[String]] = None) extends UnresolvedLeafNode {
  final override val nodePatterns: Seq[TreePattern] = Seq(UNRESOLVED_FUNC)
}

/**
 * Holds the name of a table/view/function identifier that we need to determine the catalog. It will
 * be resolved to [[ResolvedIdentifier]] during analysis.
 */
case class UnresolvedIdentifier(nameParts: Seq[String], allowTemp: Boolean = false)
  extends UnresolvedLeafNode

/**
 * A procedure identifier that should be resolved into [[ResolvedProcedure]].
 */
case class UnresolvedProcedure(nameParts: Seq[String]) extends UnresolvedLeafNode {
  final override val nodePatterns: Seq[TreePattern] = Seq(UNRESOLVED_PROCEDURE)
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
case class ResolvedNamespace(
    catalog: CatalogPlugin,
    namespace: Seq[String],
    metadata: Map[String, String] = Map.empty)
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
    outputAttributes.map(_.withQualifier(qualifier.toImmutableArraySeq))
  }
  def name: String = (catalog.name +: identifier.namespace() :+ identifier.name()).quoted
}

object ResolvedTable {
  def create(
      catalog: TableCatalog,
      identifier: Identifier,
      table: Table): ResolvedTable = {
    val schema = CharVarcharUtils.replaceCharVarcharWithStringInSchema(table.columns.asSchema)
    ResolvedTable(catalog, identifier, table, toAttributes(schema))
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

case class ResolvedProcedure(
    catalog: ProcedureCatalog,
    ident: Identifier,
    procedure: Procedure) extends LeafNodeWithoutStats {
  override def output: Seq[Attribute] = Nil
}

/**
 * A plan containing resolved persistent views.
 */
// TODO: create a generic representation for views, after we add view support to v2 catalog. For now
//       we only hold the view schema.
case class ResolvedPersistentView(
    catalog: CatalogPlugin,
    identifier: Identifier,
    metadata: CatalogTable) extends LeafNodeWithoutStats {
  override def output: Seq[Attribute] = Nil
}

/**
 * A plan containing resolved (global) temp views.
 */
case class ResolvedTempView(identifier: Identifier, metadata: CatalogTable)
  extends LeafNodeWithoutStats {
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
 * A plan containing resolved identifier with catalog determined.
 */
case class ResolvedIdentifier(
    catalog: CatalogPlugin,
    identifier: Identifier) extends LeafNodeWithoutStats {
  override def output: Seq[Attribute] = Nil
}

// A fake v2 catalog to hold temp views.
object FakeSystemCatalog extends CatalogPlugin {
  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = {}
  override def name(): String = "system"
}
