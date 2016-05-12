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

package org.apache.spark.sql.catalyst.catalog

import java.util.Date
import javax.annotation.Nullable

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.catalyst.plans.logical.{LeafNode, LogicalPlan}


/**
 * A function defined in the catalog.
 *
 * @param identifier name of the function
 * @param className fully qualified class name, e.g. "org.apache.spark.util.MyFunc"
 * @param resources resource types and Uris used by the function
 */
case class CatalogFunction(
    identifier: FunctionIdentifier,
    className: String,
    resources: Seq[FunctionResource])


/**
 * Storage format, used to describe how a partition or a table is stored.
 */
case class CatalogStorageFormat(
    locationUri: Option[String],
    inputFormat: Option[String],
    outputFormat: Option[String],
    serde: Option[String],
    compressed: Boolean,
    serdeProperties: Map[String, String]) {

  override def toString: String = {
    val compressedToString = if (compressed) "Yes" else "No"
    val serdePropertiesToString =
      serdeProperties.map(p => p._1 + "=" + p._2).mkString("[", ", ", "]")
    s"Storage Information(Location:${locationUri.orNull}, " +
      s"InputFormat:${inputFormat.orNull}, " +
      s"OutputFormat:${outputFormat.orNull}, " +
      s"Compressed:$compressedToString, " +
      s"Storage Desc Library:${serde.orNull}, " +
      s"Storage Desc Parameter:$serdePropertiesToString)"
  }
}


/**
 * A column in a table.
 */
case class CatalogColumn(
    name: String,
    // This may be null when used to create views. TODO: make this type-safe; this is left
    // as a string due to issues in converting Hive varchars to and from SparkSQL strings.
    @Nullable dataType: String,
    nullable: Boolean = true,
    comment: Option[String] = None) {

  override def toString: String = {
    val nullableToString = if (nullable) "Yes" else "No"
    s"Column(name:$name, dataType:${dataType.toLowerCase}, nullable:$nullableToString, " +
      s"comment:${comment.orNull})"
  }

}

/**
 * A partition (Hive style) defined in the catalog.
 *
 * @param spec partition spec values indexed by column name
 * @param storage storage format of the partition
 */
case class CatalogTablePartition(
    spec: CatalogTypes.TablePartitionSpec,
    storage: CatalogStorageFormat)


/**
 * A table defined in the catalog.
 *
 * Note that Hive's metastore also tracks skewed columns. We should consider adding that in the
 * future once we have a better understanding of how we want to handle skewed columns.
 */
case class CatalogTable(
    identifier: TableIdentifier,
    tableType: CatalogTableType,
    storage: CatalogStorageFormat,
    schema: Seq[CatalogColumn],
    partitionColumnNames: Seq[String] = Seq.empty,
    sortColumnNames: Seq[String] = Seq.empty,
    bucketColumnNames: Seq[String] = Seq.empty,
    numBuckets: Int = -1,
    owner: String = "",
    createTime: Long = System.currentTimeMillis,
    lastAccessTime: Long = -1,
    properties: Map[String, String] = Map.empty,
    viewOriginalText: Option[String] = None,
    viewText: Option[String] = None,
    comment: Option[String] = None) {

  // Verify that the provided columns are part of the schema
  private val colNames = schema.map(_.name).toSet
  private def requireSubsetOfSchema(cols: Seq[String], colType: String): Unit = {
    require(cols.toSet.subsetOf(colNames), s"$colType columns (${cols.mkString(", ")}) " +
      s"must be a subset of schema (${colNames.mkString(", ")}) in table '$identifier'")
  }
  requireSubsetOfSchema(partitionColumnNames, "partition")
  requireSubsetOfSchema(sortColumnNames, "sort")
  requireSubsetOfSchema(bucketColumnNames, "bucket")

  /** Columns this table is partitioned by. */
  def partitionColumns: Seq[CatalogColumn] =
    schema.filter { c => partitionColumnNames.contains(c.name) }

  /** Return the database this table was specified to belong to, assuming it exists. */
  def database: String = identifier.database.getOrElse {
    throw new AnalysisException(s"table $identifier did not specify database")
  }

  /** Return the fully qualified name of this table, assuming the database was specified. */
  def qualifiedName: String = identifier.unquotedString

  /** Syntactic sugar to update a field in `storage`. */
  def withNewStorage(
      locationUri: Option[String] = storage.locationUri,
      inputFormat: Option[String] = storage.inputFormat,
      outputFormat: Option[String] = storage.outputFormat,
      compressed: Boolean = false,
      serde: Option[String] = storage.serde,
      serdeProperties: Map[String, String] = storage.serdeProperties): CatalogTable = {
    copy(storage = CatalogStorageFormat(
      locationUri, inputFormat, outputFormat, serde, compressed, serdeProperties))
  }

  override def toString: String = {
    val tableProperties = properties.map(p => p._1 + "=" + p._2).mkString("[", ", ", "]")
    val tableSchema = schema.mkString("[", ", ", "]")
    val partitionColumns = partitionColumnNames.mkString("[", ", ", "]")
    val sortColumns = sortColumnNames.mkString("[", ", ", "]")
    val bucketColumns = bucketColumnNames.mkString("[", ", ", "]")

    s"CatalogTable(Table:${identifier.table}, " +
      s"Database:${identifier.database.orNull}, " +
      s"Owner:$owner, " +
      s"Create Time:${new Date(createTime).toString}, " +
      s"Last Access Time:${new Date(lastAccessTime).toString}, " +
      s"Location:${storage.locationUri.orNull}, " +
      s"Table Type:${tableType.name}, " +
      s"Schema:$tableSchema, " +
      s"Partition Columns:$partitionColumns, " +
      s"Num Buckets:$numBuckets, " +
      s"Bucket Columns:$bucketColumns, " +
      s"Sort Columns:$sortColumns, " +
      s"View Original Text:${viewOriginalText.orNull}, " +
      s"View Text:${viewText.orNull}, " +
      s"Comment:${comment.orNull}, " +
      s"Table Parameters:$tableProperties, " +
      s"$storage)"
  }

}


case class CatalogTableType private(name: String)
object CatalogTableType {
  val EXTERNAL = new CatalogTableType("EXTERNAL")
  val MANAGED = new CatalogTableType("MANAGED")
  val INDEX = new CatalogTableType("INDEX")
  val VIEW = new CatalogTableType("VIEW")
}


/**
 * A database defined in the catalog.
 */
case class CatalogDatabase(
    name: String,
    description: String,
    locationUri: String,
    properties: Map[String, String])


object CatalogTypes {
  /**
   * Specifications of a table partition. Mapping column name to column value.
   */
  type TablePartitionSpec = Map[String, String]
}


/**
 * An interface that is implemented by logical plans to return the underlying catalog table.
 * If we can in the future consolidate SimpleCatalogRelation and MetastoreRelation, we should
 * probably remove this interface.
 */
trait CatalogRelation {
  def catalogTable: CatalogTable
  def output: Seq[Attribute]
}


/**
 * A [[LogicalPlan]] that wraps [[CatalogTable]].
 *
 * Note that in the future we should consolidate this and HiveCatalogRelation.
 */
case class SimpleCatalogRelation(
    databaseName: String,
    metadata: CatalogTable,
    alias: Option[String] = None)
  extends LeafNode with CatalogRelation {

  override def catalogTable: CatalogTable = metadata

  override val output: Seq[Attribute] = {
    val cols = catalogTable.schema
      .filter { c => !catalogTable.partitionColumnNames.contains(c.name) }
    (cols ++ catalogTable.partitionColumns).map { f =>
      AttributeReference(
        f.name,
        CatalystSqlParser.parseDataType(f.dataType),
        // Since data can be dumped in randomly with no validation, everything is nullable.
        nullable = true
      )(qualifier = Some(alias.getOrElse(metadata.identifier.table)))
    }
  }

  require(metadata.identifier.database == Some(databaseName),
    "provided database does not match the one specified in the table definition")
}
