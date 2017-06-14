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

import java.net.URI
import java.util.Date

import scala.collection.mutable

import com.google.common.base.Objects

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.{FunctionIdentifier, InternalRow, TableIdentifier}
import org.apache.spark.sql.catalyst.analysis.MultiInstanceRelation
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeMap, AttributeReference, Cast, Literal}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.util.{CaseInsensitiveMap, DateTimeUtils}
import org.apache.spark.sql.catalyst.util.quoteIdentifier
import org.apache.spark.sql.internal.SQLConf
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
    locationUri: Option[URI],
    inputFormat: Option[String],
    outputFormat: Option[String],
    serde: Option[String],
    compressed: Boolean,
    properties: Map[String, String]) {

  override def toString: String = {
    toLinkedHashMap.map { case ((key, value)) =>
      if (value.isEmpty) key else s"$key: $value"
    }.mkString("Storage(", ", ", ")")
  }

  def toLinkedHashMap: mutable.LinkedHashMap[String, String] = {
    val map = new mutable.LinkedHashMap[String, String]()
    locationUri.foreach(l => map.put("Location", l.toString))
    serde.foreach(map.put("Serde Library", _))
    inputFormat.foreach(map.put("InputFormat", _))
    outputFormat.foreach(map.put("OutputFormat", _))
    if (compressed) map.put("Compressed", "")
    CatalogUtils.maskCredentials(properties) match {
      case props if props.isEmpty => // No-op
      case props =>
        map.put("Storage Properties", props.map(p => p._1 + "=" + p._2).mkString("[", ", ", "]"))
    }
    map
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

  def toLinkedHashMap: mutable.LinkedHashMap[String, String] = {
    val map = new mutable.LinkedHashMap[String, String]()
    val specString = spec.map { case (k, v) => s"$k=$v" }.mkString(", ")
    map.put("Partition Values", s"[$specString]")
    map ++= storage.toLinkedHashMap
    if (parameters.nonEmpty) {
      map.put("Partition Parameters", s"{${parameters.map(p => p._1 + "=" + p._2).mkString(", ")}}")
    }
    map
  }

  override def toString: String = {
    toLinkedHashMap.map { case ((key, value)) =>
      if (value.isEmpty) key else s"$key: $value"
    }.mkString("CatalogPartition(\n\t", "\n\t", ")")
  }

  /** Readable string representation for the CatalogTablePartition. */
  def simpleString: String = {
    toLinkedHashMap.map { case ((key, value)) =>
      if (value.isEmpty) key else s"$key: $value"
    }.mkString("", "\n", "")
  }

  /** Return the partition location, assuming it is specified. */
  def location: URI = storage.locationUri.getOrElse {
    val specString = spec.map { case (k, v) => s"$k=$v" }.mkString(", ")
    throw new AnalysisException(s"Partition [$specString] did not specify locationUri")
  }

  /**
   * Given the partition schema, returns a row with that schema holding the partition values.
   */
  def toRow(partitionSchema: StructType, defaultTimeZondId: String): InternalRow = {
    val caseInsensitiveProperties = CaseInsensitiveMap(storage.properties)
    val timeZoneId = caseInsensitiveProperties.getOrElse(
      DateTimeUtils.TIMEZONE_OPTION, defaultTimeZondId)
    InternalRow.fromSeq(partitionSchema.map { field =>
      val partValue = if (spec(field.name) == ExternalCatalogUtils.DEFAULT_PARTITION_NAME) {
        null
      } else {
        spec(field.name)
      }
      Cast(Literal(partValue), field.dataType, Option(timeZoneId)).eval()
    })
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
  if (numBuckets <= 0 || numBuckets >= 100000) {
    throw new AnalysisException(
      s"Number of buckets should be greater than 0 but less than 100000. Got `$numBuckets`")
  }

  override def toString: String = {
    val bucketString = s"bucket columns: [${bucketColumnNames.mkString(", ")}]"
    val sortString = if (sortColumnNames.nonEmpty) {
      s", sort columns: [${sortColumnNames.mkString(", ")}]"
    } else {
      ""
    }
    s"$numBuckets buckets, $bucketString$sortString"
  }

  def toLinkedHashMap: mutable.LinkedHashMap[String, String] = {
    mutable.LinkedHashMap[String, String](
      "Num Buckets" -> numBuckets.toString,
      "Bucket Columns" -> bucketColumnNames.map(quoteIdentifier).mkString("[", ", ", "]"),
      "Sort Columns" -> sortColumnNames.map(quoteIdentifier).mkString("[", ", ", "]")
    )
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
 * @param tracksPartitionsInCatalog whether this table's partition metadata is stored in the
 *                                  catalog. If false, it is inferred automatically based on file
 *                                  structure.
 * @param schemaPreservesCase Whether or not the schema resolved for this table is case-sensitive.
 *                           When using a Hive Metastore, this flag is set to false if a case-
 *                           sensitive schema was unable to be read from the table properties.
 *                           Used to trigger case-sensitive schema inference at query time, when
 *                           configured.
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
    stats: Option[CatalogStatistics] = None,
    viewText: Option[String] = None,
    comment: Option[String] = None,
    unsupportedFeatures: Seq[String] = Seq.empty,
    tracksPartitionsInCatalog: Boolean = false,
    schemaPreservesCase: Boolean = true) {

  import CatalogTable._

  /**
   * schema of this table's partition columns
   */
  def partitionSchema: StructType = {
    val partitionFields = schema.takeRight(partitionColumnNames.length)
    assert(partitionFields.map(_.name) == partitionColumnNames)

    StructType(partitionFields)
  }

  /**
   * schema of this table's data columns
   */
  def dataSchema: StructType = {
    val dataFields = schema.dropRight(partitionColumnNames.length)
    StructType(dataFields)
  }

  /** Return the database this table was specified to belong to, assuming it exists. */
  def database: String = identifier.database.getOrElse {
    throw new AnalysisException(s"table $identifier did not specify database")
  }

  /** Return the table location, assuming it is specified. */
  def location: URI = storage.locationUri.getOrElse {
    throw new AnalysisException(s"table $identifier did not specify locationUri")
  }

  /** Return the fully qualified name of this table, assuming the database was specified. */
  def qualifiedName: String = identifier.unquotedString

  /**
   * Return the default database name we use to resolve a view, should be None if the CatalogTable
   * is not a View or created by older versions of Spark(before 2.2.0).
   */
  def viewDefaultDatabase: Option[String] = properties.get(VIEW_DEFAULT_DATABASE)

  /**
   * Return the output column names of the query that creates a view, the column names are used to
   * resolve a view, should be empty if the CatalogTable is not a View or created by older versions
   * of Spark(before 2.2.0).
   */
  def viewQueryColumnNames: Seq[String] = {
    for {
      numCols <- properties.get(VIEW_QUERY_OUTPUT_NUM_COLUMNS).toSeq
      index <- 0 until numCols.toInt
    } yield properties.getOrElse(
      s"$VIEW_QUERY_OUTPUT_COLUMN_NAME_PREFIX$index",
      throw new AnalysisException("Corrupted view query output column names in catalog: " +
        s"$numCols parts expected, but part $index is missing.")
    )
  }

  /** Syntactic sugar to update a field in `storage`. */
  def withNewStorage(
      locationUri: Option[URI] = storage.locationUri,
      inputFormat: Option[String] = storage.inputFormat,
      outputFormat: Option[String] = storage.outputFormat,
      compressed: Boolean = false,
      serde: Option[String] = storage.serde,
      properties: Map[String, String] = storage.properties): CatalogTable = {
    copy(storage = CatalogStorageFormat(
      locationUri, inputFormat, outputFormat, serde, compressed, properties))
  }


  def toLinkedHashMap: mutable.LinkedHashMap[String, String] = {
    val map = new mutable.LinkedHashMap[String, String]()
    val tableProperties = properties.map(p => p._1 + "=" + p._2).mkString("[", ", ", "]")
    val partitionColumns = partitionColumnNames.map(quoteIdentifier).mkString("[", ", ", "]")

    identifier.database.foreach(map.put("Database", _))
    map.put("Table", identifier.table)
    if (owner.nonEmpty) map.put("Owner", owner)
    map.put("Created", new Date(createTime).toString)
    map.put("Last Access", new Date(lastAccessTime).toString)
    map.put("Type", tableType.name)
    provider.foreach(map.put("Provider", _))
    bucketSpec.foreach(map ++= _.toLinkedHashMap)
    comment.foreach(map.put("Comment", _))
    if (tableType == CatalogTableType.VIEW) {
      viewText.foreach(map.put("View Text", _))
      viewDefaultDatabase.foreach(map.put("View Default Database", _))
      if (viewQueryColumnNames.nonEmpty) {
        map.put("View Query Output Columns", viewQueryColumnNames.mkString("[", ", ", "]"))
      }
    }

    if (properties.nonEmpty) map.put("Table Properties", tableProperties)
    stats.foreach(s => map.put("Statistics", s.simpleString))
    map ++= storage.toLinkedHashMap
    if (tracksPartitionsInCatalog) map.put("Partition Provider", "Catalog")
    if (partitionColumnNames.nonEmpty) map.put("Partition Columns", partitionColumns)
    if (schema.nonEmpty) map.put("Schema", schema.treeString)

    map
  }

  override def toString: String = {
    toLinkedHashMap.map { case ((key, value)) =>
      if (value.isEmpty) key else s"$key: $value"
    }.mkString("CatalogTable(\n", "\n", ")")
  }

  /** Readable string representation for the CatalogTable. */
  def simpleString: String = {
    toLinkedHashMap.map { case ((key, value)) =>
      if (value.isEmpty) key else s"$key: $value"
    }.mkString("", "\n", "")
  }
}

object CatalogTable {
  val VIEW_DEFAULT_DATABASE = "view.default.database"
  val VIEW_QUERY_OUTPUT_PREFIX = "view.query.out."
  val VIEW_QUERY_OUTPUT_NUM_COLUMNS = VIEW_QUERY_OUTPUT_PREFIX + "numCols"
  val VIEW_QUERY_OUTPUT_COLUMN_NAME_PREFIX = VIEW_QUERY_OUTPUT_PREFIX + "col."
}

/**
 * This class of statistics is used in [[CatalogTable]] to interact with metastore.
 * We define this new class instead of directly using [[Statistics]] here because there are no
 * concepts of attributes or broadcast hint in catalog.
 */
case class CatalogStatistics(
    sizeInBytes: BigInt,
    rowCount: Option[BigInt] = None,
    colStats: Map[String, ColumnStat] = Map.empty) {

  /**
   * Convert [[CatalogStatistics]] to [[Statistics]], and match column stats to attributes based
   * on column names.
   */
  def toPlanStats(planOutput: Seq[Attribute]): Statistics = {
    val matched = planOutput.flatMap(a => colStats.get(a.name).map(a -> _))
    Statistics(sizeInBytes = sizeInBytes, rowCount = rowCount,
      attributeStats = AttributeMap(matched))
  }

  /** Readable string representation for the CatalogStatistics. */
  def simpleString: String = {
    val rowCountString = if (rowCount.isDefined) s", ${rowCount.get} rows" else ""
    s"$sizeInBytes bytes$rowCountString"
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
    locationUri: URI,
    properties: Map[String, String])


object CatalogTypes {
  /**
   * Specifications of a table partition. Mapping column name to column value.
   */
  type TablePartitionSpec = Map[String, String]
}


/**
 * A [[LogicalPlan]] that represents a table.
 */
case class CatalogRelation(
    tableMeta: CatalogTable,
    dataCols: Seq[AttributeReference],
    partitionCols: Seq[AttributeReference]) extends LeafNode with MultiInstanceRelation {
  assert(tableMeta.identifier.database.isDefined)
  assert(tableMeta.partitionSchema.sameType(partitionCols.toStructType))
  assert(tableMeta.dataSchema.sameType(dataCols.toStructType))

  // The partition column should always appear after data columns.
  override def output: Seq[AttributeReference] = dataCols ++ partitionCols

  def isPartitioned: Boolean = partitionCols.nonEmpty

  override def equals(relation: Any): Boolean = relation match {
    case other: CatalogRelation => tableMeta == other.tableMeta && output == other.output
    case _ => false
  }

  override def hashCode(): Int = {
    Objects.hashCode(tableMeta.identifier, output)
  }

  override def preCanonicalized: LogicalPlan = copy(tableMeta = CatalogTable(
    identifier = tableMeta.identifier,
    tableType = tableMeta.tableType,
    storage = CatalogStorageFormat.empty,
    schema = tableMeta.schema,
    partitionColumnNames = tableMeta.partitionColumnNames,
    bucketSpec = tableMeta.bucketSpec,
    createTime = -1
  ))

  override def computeStats(conf: SQLConf): Statistics = {
    // For data source tables, we will create a `LogicalRelation` and won't call this method, for
    // hive serde tables, we will always generate a statistics.
    // TODO: unify the table stats generation.
    tableMeta.stats.map(_.toPlanStats(output)).getOrElse {
      throw new IllegalStateException("table stats must be specified.")
    }
  }

  override def newInstance(): LogicalPlan = copy(
    dataCols = dataCols.map(_.newInstance()),
    partitionCols = partitionCols.map(_.newInstance()))
}
