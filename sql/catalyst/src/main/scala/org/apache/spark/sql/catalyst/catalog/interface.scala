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

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.{LeafNode, LogicalPlan, Statistics}
import org.apache.spark.sql.catalyst.util.quoteIdentifier
import org.apache.spark.sql.types.StructType


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
    properties: Map[String, String]) {

  override def toString: String = {
    val maskedProperties = properties.map {
      case (password, _) if password.toLowerCase == "password" => (password, "###")
      case (url, value) if url.toLowerCase == "url" && value.toLowerCase.contains("password") =>
        (url, "###")
      case o => o
    }
    val serdePropsToString =
      if (maskedProperties.nonEmpty) {
        s"Properties: " + maskedProperties.map(p => p._1 + "=" + p._2).mkString("[", ", ", "]")
      } else {
        ""
      }
    val output =
      Seq(locationUri.map("Location: " + _).getOrElse(""),
        inputFormat.map("InputFormat: " + _).getOrElse(""),
        outputFormat.map("OutputFormat: " + _).getOrElse(""),
        if (compressed) "Compressed" else "",
        serde.map("Serde: " + _).getOrElse(""),
        serdePropsToString)
    output.filter(_.nonEmpty).mkString("Storage(", ", ", ")")
  }

}

object CatalogStorageFormat {
  /** Empty storage format for default values and copies. */
  val empty = CatalogStorageFormat(locationUri = None, inputFormat = None,
    outputFormat = None, serde = None, compressed = false, properties = Map.empty)
}

/**
 * A partition (Hive style) defined in the catalog.
 *
 * @param spec partition spec values indexed by column name
 * @param storage storage format of the partition
 * @param parameters some parameters for the partition, for example, stats.
 */
case class CatalogTablePartition(
    spec: CatalogTypes.TablePartitionSpec,
    storage: CatalogStorageFormat,
    parameters: Map[String, String] = Map.empty) {

  override def toString: String = {
    val output =
      Seq(
        s"Partition Values: [${spec.values.mkString(", ")}]",
        s"$storage",
        s"Partition Parameters:{${parameters.map(p => p._1 + "=" + p._2).mkString(", ")}}")

    output.filter(_.nonEmpty).mkString("CatalogPartition(\n\t", "\n\t", ")")
  }
}


/**
 * A container for bucketing information.
 * Bucketing is a technology for decomposing data sets into more manageable parts, and the number
 * of buckets is fixed so it does not fluctuate with data.
 *
 * @param numBuckets number of buckets.
 * @param bucketColumnNames the names of the columns that used to generate the bucket id.
 * @param sortColumnNames the names of the columns that used to sort data in each bucket.
 */
case class BucketSpec(
    numBuckets: Int,
    bucketColumnNames: Seq[String],
    sortColumnNames: Seq[String]) {
  if (numBuckets <= 0) {
    throw new AnalysisException(s"Expected positive number of buckets, but got `$numBuckets`.")
  }
}

/**
 * A table defined in the catalog.
 *
 * Note that Hive's metastore also tracks skewed columns. We should consider adding that in the
 * future once we have a better understanding of how we want to handle skewed columns.
 *
 * @param provider the name of the data source provider for this table, e.g. parquet, json, etc.
 *                 Can be None if this table is a View, should be "hive" for hive serde tables.
 * @param unsupportedFeatures is a list of string descriptions of features that are used by the
 *        underlying table but not supported by Spark SQL yet.
 */
case class CatalogTable(
    identifier: TableIdentifier,
    tableType: CatalogTableType,
    storage: CatalogStorageFormat,
    schema: StructType,
    provider: Option[String] = None,
    partitionColumnNames: Seq[String] = Seq.empty,
    bucketSpec: Option[BucketSpec] = None,
    owner: String = "",
    createTime: Long = System.currentTimeMillis,
    lastAccessTime: Long = -1,
    properties: Map[String, String] = Map.empty,
    stats: Option[Statistics] = None,
    viewOriginalText: Option[String] = None,
    viewText: Option[String] = None,
    comment: Option[String] = None,
    unsupportedFeatures: Seq[String] = Seq.empty) {

  /** schema of this table's partition columns */
  def partitionSchema: StructType = StructType(schema.filter {
    c => partitionColumnNames.contains(c.name)
  })

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
      properties: Map[String, String] = storage.properties): CatalogTable = {
    copy(storage = CatalogStorageFormat(
      locationUri, inputFormat, outputFormat, serde, compressed, properties))
  }

  override def toString: String = {
    val tableProperties = properties.map(p => p._1 + "=" + p._2).mkString("[", ", ", "]")
    val partitionColumns = partitionColumnNames.map(quoteIdentifier).mkString("[", ", ", "]")
    val bucketStrings = bucketSpec match {
      case Some(BucketSpec(numBuckets, bucketColumnNames, sortColumnNames)) =>
        val bucketColumnsString = bucketColumnNames.map(quoteIdentifier).mkString("[", ", ", "]")
        val sortColumnsString = sortColumnNames.map(quoteIdentifier).mkString("[", ", ", "]")
        Seq(
          s"Num Buckets: $numBuckets",
          if (bucketColumnNames.nonEmpty) s"Bucket Columns: $bucketColumnsString" else "",
          if (sortColumnNames.nonEmpty) s"Sort Columns: $sortColumnsString" else ""
        )

      case _ => Nil
    }

    val output =
      Seq(s"Table: ${identifier.quotedString}",
        if (owner.nonEmpty) s"Owner: $owner" else "",
        s"Created: ${new Date(createTime).toString}",
        s"Last Access: ${new Date(lastAccessTime).toString}",
        s"Type: ${tableType.name}",
        if (schema.nonEmpty) s"Schema: ${schema.mkString("[", ", ", "]")}" else "",
        if (provider.isDefined) s"Provider: ${provider.get}" else "",
        if (partitionColumnNames.nonEmpty) s"Partition Columns: $partitionColumns" else ""
      ) ++ bucketStrings ++ Seq(
        viewOriginalText.map("Original View: " + _).getOrElse(""),
        viewText.map("View: " + _).getOrElse(""),
        comment.map("Comment: " + _).getOrElse(""),
        if (properties.nonEmpty) s"Properties: $tableProperties" else "",
        if (stats.isDefined) s"Statistics: ${stats.get.simpleString}" else "",
        s"$storage")

    output.filter(_.nonEmpty).mkString("CatalogTable(\n\t", "\n\t", ")")
  }

}


case class CatalogTableType private(name: String)
object CatalogTableType {
  val EXTERNAL = new CatalogTableType("EXTERNAL")
  val MANAGED = new CatalogTableType("MANAGED")
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
    metadata: CatalogTable)
  extends LeafNode with CatalogRelation {

  override def catalogTable: CatalogTable = metadata

  override lazy val resolved: Boolean = false

  override val output: Seq[Attribute] = {
    val (partCols, dataCols) = metadata.schema.toAttributes
      // Since data can be dumped in randomly with no validation, everything is nullable.
      .map(_.withNullability(true).withQualifier(Some(metadata.identifier.table)))
      .partition { a =>
        metadata.partitionColumnNames.contains(a.name)
      }
    dataCols ++ partCols
  }

  require(
    metadata.identifier.database == Some(databaseName),
    "provided database does not match the one specified in the table definition")
}
