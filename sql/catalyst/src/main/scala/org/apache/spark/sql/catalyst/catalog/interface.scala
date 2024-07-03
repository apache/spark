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
import java.time.{ZoneId, ZoneOffset}
import java.util.Date

import scala.collection.mutable
import scala.util.control.NonFatal

import com.fasterxml.jackson.annotation.JsonInclude.Include
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.{ClassTagExtensions, DefaultScalaModule}
import org.apache.commons.lang3.StringUtils
import org.json4s.JsonAST.{JArray, JString}
import org.json4s.jackson.JsonMethods._

import org.apache.spark.SparkException
import org.apache.spark.internal.{Logging, MDC}
import org.apache.spark.internal.LogKeys._
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.{CurrentUserContext, FunctionIdentifier, InternalRow, SQLConfHelper, TableIdentifier}
import org.apache.spark.sql.catalyst.analysis.{MultiInstanceRelation, Resolver, SchemaBinding, SchemaCompensation, SchemaEvolution, SchemaTypeEvolution, SchemaUnsupported, UnresolvedLeafNode, ViewSchemaMode}
import org.apache.spark.sql.catalyst.catalog.CatalogTable.VIEW_STORING_ANALYZED_PLAN
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeMap, AttributeReference, Cast, ExprId, Literal}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.plans.logical.statsEstimation.EstimationUtils
import org.apache.spark.sql.catalyst.types.DataTypeUtils
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.connector.catalog.CatalogManager
import org.apache.spark.sql.connector.expressions.{ClusterByTransform, FieldReference, NamedReference, Transform}
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.{CaseInsensitiveStringMap, SchemaUtils}
import org.apache.spark.util.ArrayImplicits._


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
    SQLConf.get.redactOptions(properties) match {
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
 * @param parameters some parameters for the partition
 * @param createTime creation time of the partition, in milliseconds
 * @param lastAccessTime last access time, in milliseconds
 * @param stats optional statistics (number of rows, total size, etc.)
 */
case class CatalogTablePartition(
    spec: CatalogTypes.TablePartitionSpec,
    storage: CatalogStorageFormat,
    parameters: Map[String, String] = Map.empty,
    createTime: Long = System.currentTimeMillis,
    lastAccessTime: Long = -1,
    stats: Option[CatalogStatistics] = None) {

  def toLinkedHashMap: mutable.LinkedHashMap[String, String] = {
    val map = new mutable.LinkedHashMap[String, String]()
    val specString = spec.map { case (k, v) => s"$k=$v" }.mkString(", ")
    map.put("Partition Values", s"[$specString]")
    map ++= storage.toLinkedHashMap
    if (parameters.nonEmpty) {
      map.put("Partition Parameters", s"{" +
        s"${SQLConf.get.redactOptions(parameters).map(p => p._1 + "=" + p._2).mkString(", ")}}")
    }
    map.put("Created Time", new Date(createTime).toString)
    val lastAccess = {
      if (lastAccessTime <= 0) "UNKNOWN" else new Date(lastAccessTime).toString
    }
    map.put("Last Access", lastAccess)
    stats.foreach(s => map.put("Partition Statistics", s.simpleString))
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
    throw QueryCompilationErrors.partitionNotSpecifyLocationUriError(specString)
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
 * A container for clustering information.
 *
 * @param columnNames the names of the columns used for clustering.
 */
case class ClusterBySpec(columnNames: Seq[NamedReference]) {
  override def toString: String = toJson

  def toJson: String = ClusterBySpec.mapper.writeValueAsString(columnNames.map(_.fieldNames))
}

object ClusterBySpec {
  private val mapper = {
    val ret = new ObjectMapper() with ClassTagExtensions
    ret.setSerializationInclusion(Include.NON_ABSENT)
    ret.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    ret.registerModule(DefaultScalaModule)
    ret
  }

  def fromProperty(columns: String): ClusterBySpec = {
    ClusterBySpec(mapper.readValue[Seq[Seq[String]]](columns).map(FieldReference(_)))
  }

  def toProperty(
      schema: StructType,
      clusterBySpec: ClusterBySpec,
      resolver: Resolver): (String, String) = {
    CatalogTable.PROP_CLUSTERING_COLUMNS ->
      normalizeClusterBySpec(schema, clusterBySpec, resolver).toJson
  }

  private def normalizeClusterBySpec(
      schema: StructType,
      clusterBySpec: ClusterBySpec,
      resolver: Resolver): ClusterBySpec = {
    val normalizedColumns = clusterBySpec.columnNames.map { columnName =>
      val position = SchemaUtils.findColumnPosition(
        columnName.fieldNames().toImmutableArraySeq, schema, resolver)
      FieldReference(SchemaUtils.getColumnName(position, schema))
    }

    SchemaUtils.checkColumnNameDuplication(
      normalizedColumns.map(_.toString),
      resolver)

    ClusterBySpec(normalizedColumns)
  }

  def extractClusterBySpec(transforms: Seq[Transform]): Option[ClusterBySpec] = {
    transforms.collectFirst {
      case ClusterByTransform(columnNames) => ClusterBySpec(columnNames)
    }
  }

  def extractClusterByTransform(
      schema: StructType,
      clusterBySpec: ClusterBySpec,
      resolver: Resolver): ClusterByTransform = {
    val normalizedClusterBySpec = normalizeClusterBySpec(schema, clusterBySpec, resolver)
    ClusterByTransform(normalizedClusterBySpec.columnNames)
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
    sortColumnNames: Seq[String]) extends SQLConfHelper {

  if (numBuckets <= 0 || numBuckets > conf.bucketingMaxBuckets) {
    throw QueryCompilationErrors.invalidBucketNumberError(
      conf.bucketingMaxBuckets, numBuckets)
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
 * @param ignoredProperties is a list of table properties that are used by the underlying table
 *                          but ignored by Spark SQL yet.
 * @param createVersion records the version of Spark that created this table metadata. The default
 *                      is an empty string. We expect it will be read from the catalog or filled by
 *                      ExternalCatalog.createTable. For temporary views, the value will be empty.
 */
case class CatalogTable(
    identifier: TableIdentifier,
    tableType: CatalogTableType,
    storage: CatalogStorageFormat,
    schema: StructType,
    provider: Option[String] = None,
    partitionColumnNames: Seq[String] = Seq.empty,
    bucketSpec: Option[BucketSpec] = None,
    owner: String = CurrentUserContext.getCurrentUserOrEmpty,
    createTime: Long = System.currentTimeMillis,
    lastAccessTime: Long = -1,
    createVersion: String = "",
    properties: Map[String, String] = Map.empty,
    stats: Option[CatalogStatistics] = None,
    viewText: Option[String] = None,
    comment: Option[String] = None,
    unsupportedFeatures: Seq[String] = Seq.empty,
    tracksPartitionsInCatalog: Boolean = false,
    schemaPreservesCase: Boolean = true,
    ignoredProperties: Map[String, String] = Map.empty,
    viewOriginalText: Option[String] = None) {

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
    throw QueryCompilationErrors.tableNotSpecifyDatabaseError(identifier)
  }

  /** Return the table location, assuming it is specified. */
  def location: URI = storage.locationUri.getOrElse {
    throw QueryCompilationErrors.tableNotSpecifyLocationUriError(identifier)
  }

  /** Return the fully qualified name of this table, assuming the database was specified. */
  def qualifiedName: String = identifier.unquotedString

  /**
   * Return the current catalog and namespace (concatenated as a Seq[String]) of when the view was
   * created.
   */
  def viewCatalogAndNamespace: Seq[String] = {
    if (properties.contains(VIEW_CATALOG_AND_NAMESPACE)) {
      val numParts = properties(VIEW_CATALOG_AND_NAMESPACE).toInt
      (0 until numParts).map { index =>
        properties.getOrElse(
          s"$VIEW_CATALOG_AND_NAMESPACE_PART_PREFIX$index",
          throw QueryCompilationErrors.corruptedTableNameContextInCatalogError(numParts, index)
        )
      }
    } else if (properties.contains(VIEW_DEFAULT_DATABASE)) {
      // Views created before Spark 3.0 can only access tables in the session catalog.
      Seq(CatalogManager.SESSION_CATALOG_NAME, properties(VIEW_DEFAULT_DATABASE))
    } else {
      Nil
    }
  }

  /**
   * Return the SQL configs of when the view was created, the configs are applied when parsing and
   * analyzing the view, should be empty if the CatalogTable is not a View or created by older
   * versions of Spark(before 3.1.0).
   */
  def viewSQLConfigs: Map[String, String] = {
    try {
      for ((key, value) <- properties if key.startsWith(CatalogTable.VIEW_SQL_CONFIG_PREFIX))
        yield (key.substring(CatalogTable.VIEW_SQL_CONFIG_PREFIX.length), value)
    } catch {
      case e: Exception =>
        throw QueryCompilationErrors.corruptedViewSQLConfigsInCatalogError(e)
    }
  }

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
      throw QueryCompilationErrors.corruptedViewQueryOutputColumnsInCatalogError(numCols, index)
    )
  }

  /**
   * Return the schema binding mode. Defaults to SchemaBinding if not a view or an older
   * version, unless the viewSchemaBindingMode config is set to false
   */
  def viewSchemaMode: ViewSchemaMode = {
    if (!SQLConf.get.viewSchemaBindingEnabled) {
      SchemaUnsupported
    } else {
      val schemaMode = properties.getOrElse(VIEW_SCHEMA_MODE, SchemaBinding.toString)
      schemaMode match {
        case SchemaBinding.toString => SchemaBinding
        case SchemaEvolution.toString => SchemaEvolution
        case SchemaTypeEvolution.toString => SchemaTypeEvolution
        case SchemaCompensation.toString => SchemaCompensation
        case other => throw SparkException.internalError("Unexpected ViewSchemaMode")
      }
    }
  }

  /**
   * Return temporary view names the current view was referred. should be empty if the
   * CatalogTable is not a Temporary View or created by older versions of Spark(before 3.1.0).
   */
  def viewReferredTempViewNames: Seq[Seq[String]] = {
    try {
      properties.get(VIEW_REFERRED_TEMP_VIEW_NAMES).map { json =>
        parse(json).asInstanceOf[JArray].arr.map { namePartsJson =>
          namePartsJson.asInstanceOf[JArray].arr.map(_.asInstanceOf[JString].s)
        }
      }.getOrElse(Seq.empty)
    } catch {
      case e: Exception =>
        throw QueryCompilationErrors.corruptedViewReferredTempViewInCatalogError(e)
    }
  }

  /**
   * Return temporary function names the current view was referred. should be empty if the
   * CatalogTable is not a Temporary View or created by older versions of Spark(before 3.1.0).
   */
  def viewReferredTempFunctionNames: Seq[String] = {
    try {
      properties.get(VIEW_REFERRED_TEMP_FUNCTION_NAMES).map { json =>
        parse(json).asInstanceOf[JArray].arr.map(_.asInstanceOf[JString].s)
      }.getOrElse(Seq.empty)
    } catch {
      case e: Exception =>
        throw QueryCompilationErrors.corruptedViewReferredTempFunctionsInCatalogError(e)
    }
  }

  /**
   * Return temporary variable names the current view was referred. should be empty if the
   * CatalogTable is not a Temporary View or created by older versions of Spark(before 3.4.0).
   */
  def viewReferredTempVariableNames: Seq[Seq[String]] = {
    try {
      properties.get(VIEW_REFERRED_TEMP_VARIABLE_NAMES).map { json =>
        parse(json).asInstanceOf[JArray].arr.map { namePartsJson =>
          namePartsJson.asInstanceOf[JArray].arr.map(_.asInstanceOf[JString].s)
        }
      }.getOrElse(Seq.empty)
    } catch {
      case e: Exception =>
        throw new AnalysisException(
          errorClass = "INTERNAL_ERROR_METADATA_CATALOG.TEMP_VARIABLE_REFERENCE",
          messageParameters = Map.empty,
          cause = Some(e))
    }
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
    val tableProperties =
      SQLConf.get.redactOptions(properties.filter { case (k, _) => !k.startsWith(VIEW_PREFIX) })
        .toSeq.sortBy(_._1)
        .map(p => p._1 + "=" + p._2)
    val partitionColumns = partitionColumnNames.map(quoteIdentifier).mkString("[", ", ", "]")
    val lastAccess = {
      if (lastAccessTime <= 0) "UNKNOWN" else new Date(lastAccessTime).toString
    }

    identifier.catalog.foreach(map.put("Catalog", _))
    identifier.database.foreach(map.put("Database", _))
    map.put("Table", identifier.table)
    if (owner != null && owner.nonEmpty) map.put("Owner", owner)
    map.put("Created Time", new Date(createTime).toString)
    map.put("Last Access", lastAccess)
    map.put("Created By", "Spark " + createVersion)
    map.put("Type", tableType.name)
    provider.foreach(map.put("Provider", _))
    bucketSpec.foreach(map ++= _.toLinkedHashMap)
    comment.foreach(map.put("Comment", _))
    if (tableType == CatalogTableType.VIEW) {
      viewText.foreach(map.put("View Text", _))
      viewOriginalText.foreach(map.put("View Original Text", _))
      if (SQLConf.get.viewSchemaBindingEnabled) {
        map.put("View Schema Mode", viewSchemaMode.toString)
      }
      if (viewCatalogAndNamespace.nonEmpty) {
        import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._
        map.put("View Catalog and Namespace", viewCatalogAndNamespace.quoted)
      }
      if (viewQueryColumnNames.nonEmpty) {
        map.put("View Query Output Columns", viewQueryColumnNames.mkString("[", ", ", "]"))
      }
    }

    if (tableProperties.nonEmpty) {
      map.put("Table Properties", tableProperties.mkString("[", ", ", "]"))
    }
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

  lazy val clusterBySpec: Option[ClusterBySpec] = {
    properties.get(PROP_CLUSTERING_COLUMNS).map(ClusterBySpec.fromProperty)
  }
}

object CatalogTable {
  val VIEW_PREFIX = "view."
  // Starting from Spark 3.0, we don't use this property any more. `VIEW_CATALOG_AND_NAMESPACE` is
  // used instead.
  val VIEW_DEFAULT_DATABASE = VIEW_PREFIX + "default.database"

  val VIEW_CATALOG_AND_NAMESPACE = VIEW_PREFIX + "catalogAndNamespace.numParts"
  val VIEW_CATALOG_AND_NAMESPACE_PART_PREFIX = VIEW_PREFIX + "catalogAndNamespace.part."
  // Convert the current catalog and namespace to properties.
  def catalogAndNamespaceToProps(
      currentCatalog: String,
      currentNamespace: Seq[String]): Map[String, String] = {
    val props = new mutable.HashMap[String, String]
    val parts = currentCatalog +: currentNamespace
    if (parts.nonEmpty) {
      props.put(VIEW_CATALOG_AND_NAMESPACE, parts.length.toString)
      parts.zipWithIndex.foreach { case (name, index) =>
        props.put(s"$VIEW_CATALOG_AND_NAMESPACE_PART_PREFIX$index", name)
      }
    }
    props.toMap
  }

  val VIEW_SQL_CONFIG_PREFIX = VIEW_PREFIX + "sqlConfig."

  val VIEW_QUERY_OUTPUT_PREFIX = VIEW_PREFIX + "query.out."
  val VIEW_QUERY_OUTPUT_NUM_COLUMNS = VIEW_QUERY_OUTPUT_PREFIX + "numCols"
  val VIEW_QUERY_OUTPUT_COLUMN_NAME_PREFIX = VIEW_QUERY_OUTPUT_PREFIX + "col."

  val VIEW_REFERRED_TEMP_VIEW_NAMES = VIEW_PREFIX + "referredTempViewNames"
  val VIEW_REFERRED_TEMP_FUNCTION_NAMES = VIEW_PREFIX + "referredTempFunctionsNames"
  val VIEW_REFERRED_TEMP_VARIABLE_NAMES = VIEW_PREFIX + "referredTempVariablesNames"

  val VIEW_SCHEMA_MODE = VIEW_PREFIX + "schemaMode"

  val VIEW_STORING_ANALYZED_PLAN = VIEW_PREFIX + "storingAnalyzedPlan"

  val PROP_CLUSTERING_COLUMNS: String = "clusteringColumns"

  def splitLargeTableProp(
      key: String,
      value: String,
      addProp: (String, String) => Unit,
      defaultThreshold: Int): Unit = {
    val threshold = SQLConf.get.getConf(SQLConf.HIVE_TABLE_PROPERTY_LENGTH_THRESHOLD)
      .getOrElse(defaultThreshold)
    if (value.length <= threshold) {
      addProp(key, value)
    } else {
      val parts = value.grouped(threshold).toSeq
      addProp(s"$key.numParts", parts.length.toString)
      parts.zipWithIndex.foreach { case (part, index) =>
        addProp(s"$key.part.$index", part)
      }
    }
  }

  def readLargeTableProp(props: Map[String, String], key: String): Option[String] = {
    props.get(key).orElse {
      if (props.exists { case (mapKey, _) => mapKey.startsWith(key) }) {
        props.get(s"$key.numParts") match {
          case None => throw QueryCompilationErrors.insufficientTablePropertyError(key)
          case Some(numParts) =>
            val parts = (0 until numParts.toInt).map { index =>
              val keyPart = s"$key.part.$index"
              props.getOrElse(keyPart, {
                throw QueryCompilationErrors.insufficientTablePropertyPartError(keyPart, numParts)
              })
            }
            Some(parts.mkString)
        }
      } else {
        None
      }
    }
  }

  def isLargeTableProp(originalKey: String, propKey: String): Boolean = {
    propKey == originalKey || propKey == s"$originalKey.numParts" ||
      propKey.startsWith(s"$originalKey.part.")
  }

  def normalize(table: CatalogTable): CatalogTable = {
    val nondeterministicProps = Set(
      "CreateTime",
      "transient_lastDdlTime",
      "grantTime",
      "lastUpdateTime",
      "last_modified_by",
      "last_modified_time",
      "Owner:",
      // The following are hive specific schema parameters which we do not need to match exactly.
      "totalNumberFiles",
      "maxFileSize",
      "minFileSize"
    )

    table.copy(
      createTime = 0L,
      lastAccessTime = 0L,
      properties = table.properties
        .filter { case (k, _) => !nondeterministicProps.contains(k) }
        .map(identity),
      stats = None,
      ignoredProperties = Map.empty
    )
  }
}

/**
 * This class of statistics is used in [[CatalogTable]] to interact with metastore.
 * We define this new class instead of directly using [[Statistics]] here because there are no
 * concepts of attributes in catalog.
 */
case class CatalogStatistics(
    sizeInBytes: BigInt,
    rowCount: Option[BigInt] = None,
    colStats: Map[String, CatalogColumnStat] = Map.empty) {

  /**
   * Convert [[CatalogStatistics]] to [[Statistics]], and match column stats to attributes based
   * on column names.
   */
  def toPlanStats(planOutput: Seq[Attribute], planStatsEnabled: Boolean): Statistics = {
    if (planStatsEnabled && rowCount.isDefined) {
      val attrStats = AttributeMap(planOutput
        .flatMap(a => colStats.get(a.name).map(a -> _.toPlanStat(a.name, a.dataType))))
      // Estimate size as number of rows * row size.
      val size = EstimationUtils.getOutputSize(planOutput, rowCount.get, attrStats)
      Statistics(sizeInBytes = size, rowCount = rowCount, attributeStats = attrStats)
    } else {
      // When plan statistics are disabled or the table doesn't have other statistics,
      // we apply the size-only estimation strategy and only propagate sizeInBytes in statistics.
      Statistics(sizeInBytes = sizeInBytes)
    }
  }

  /** Readable string representation for the CatalogStatistics. */
  def simpleString: String = {
    val rowCountString = if (rowCount.isDefined) s", ${rowCount.get} rows" else ""
    s"$sizeInBytes bytes$rowCountString"
  }
}

/**
 * This class of statistics for a column is used in [[CatalogTable]] to interact with metastore.
 */
case class CatalogColumnStat(
    distinctCount: Option[BigInt] = None,
    min: Option[String] = None,
    max: Option[String] = None,
    nullCount: Option[BigInt] = None,
    avgLen: Option[Long] = None,
    maxLen: Option[Long] = None,
    histogram: Option[Histogram] = None,
    version: Int = CatalogColumnStat.VERSION) {

  /**
   * Returns a map from string to string that can be used to serialize the column stats.
   * The key is the name of the column and name of the field (e.g. "colName.distinctCount"),
   * and the value is the string representation for the value.
   * min/max values are stored as Strings. They can be deserialized using
   * [[CatalogColumnStat.fromExternalString]].
   *
   * As part of the protocol, the returned map always contains a key called "version".
   * Any of the fields that are null (None) won't appear in the map.
   */
  def toMap(colName: String): Map[String, String] = {
    val map = new scala.collection.mutable.HashMap[String, String]
    map.put(s"${colName}.${CatalogColumnStat.KEY_VERSION}", CatalogColumnStat.VERSION.toString)
    distinctCount.foreach { v =>
      map.put(s"${colName}.${CatalogColumnStat.KEY_DISTINCT_COUNT}", v.toString)
    }
    nullCount.foreach { v =>
      map.put(s"${colName}.${CatalogColumnStat.KEY_NULL_COUNT}", v.toString)
    }
    avgLen.foreach { v => map.put(s"${colName}.${CatalogColumnStat.KEY_AVG_LEN}", v.toString) }
    maxLen.foreach { v => map.put(s"${colName}.${CatalogColumnStat.KEY_MAX_LEN}", v.toString) }
    min.foreach { v => map.put(s"${colName}.${CatalogColumnStat.KEY_MIN_VALUE}", v) }
    max.foreach { v => map.put(s"${colName}.${CatalogColumnStat.KEY_MAX_VALUE}", v) }
    histogram.foreach { h =>
      CatalogTable.splitLargeTableProp(
        s"$colName.${CatalogColumnStat.KEY_HISTOGRAM}",
        HistogramSerializer.serialize(h),
        map.put,
        4000)
    }
    map.toMap
  }

  /** Convert [[CatalogColumnStat]] to [[ColumnStat]]. */
  def toPlanStat(
      colName: String,
      dataType: DataType): ColumnStat =
    ColumnStat(
      distinctCount = distinctCount,
      min = min.map(CatalogColumnStat.fromExternalString(_, colName, dataType, version)),
      max = max.map(CatalogColumnStat.fromExternalString(_, colName, dataType, version)),
      nullCount = nullCount,
      avgLen = avgLen,
      maxLen = maxLen,
      histogram = histogram,
      version = version)
}

object CatalogColumnStat extends Logging {

  // List of string keys used to serialize CatalogColumnStat
  val KEY_VERSION = "version"
  private val KEY_DISTINCT_COUNT = "distinctCount"
  private val KEY_MIN_VALUE = "min"
  private val KEY_MAX_VALUE = "max"
  private val KEY_NULL_COUNT = "nullCount"
  private val KEY_AVG_LEN = "avgLen"
  private val KEY_MAX_LEN = "maxLen"
  private val KEY_HISTOGRAM = "histogram"

  val VERSION = 2

  def getTimestampFormatter(
      isParsing: Boolean,
      format: String = "yyyy-MM-dd HH:mm:ss.SSSSSS",
      zoneId: ZoneId = ZoneOffset.UTC,
      forTimestampNTZ: Boolean = false): TimestampFormatter = {
    TimestampFormatter(
      format = format,
      zoneId = zoneId,
      isParsing = isParsing,
      forTimestampNTZ = forTimestampNTZ)
  }

  /**
   * Converts from string representation of data type to the corresponding Catalyst data type.
   */
  def fromExternalString(s: String, name: String, dataType: DataType, version: Int): Any = {
    dataType match {
      case BooleanType => s.toBoolean
      case DateType if version == 1 => DateTimeUtils.fromJavaDate(java.sql.Date.valueOf(s))
      case DateType => DateFormatter().parse(s)
      case TimestampType if version == 1 =>
        DateTimeUtils.fromJavaTimestamp(java.sql.Timestamp.valueOf(s))
      case TimestampType => getTimestampFormatter(isParsing = true).parse(s)
      case TimestampNTZType =>
        getTimestampFormatter(isParsing = true, forTimestampNTZ = true).parse(s)
      case ByteType => s.toByte
      case ShortType => s.toShort
      case IntegerType => s.toInt
      case LongType => s.toLong
      case FloatType => s.toFloat
      case DoubleType => s.toDouble
      case _: DecimalType => Decimal(s)
      // This version of Spark does not use min/max for binary/string types so we ignore it.
      case BinaryType | StringType => null
      case _ =>
        throw QueryCompilationErrors.columnStatisticsDeserializationNotSupportedError(
          name, dataType)
    }
  }

  /**
   * Converts the given value from Catalyst data type to string representation of external
   * data type.
   */
  def toExternalString(v: Any, colName: String, dataType: DataType): String = {
    val externalValue = dataType match {
      case DateType => DateFormatter().format(v.asInstanceOf[Int])
      case TimestampType => getTimestampFormatter(isParsing = false).format(v.asInstanceOf[Long])
      case TimestampNTZType =>
        getTimestampFormatter(isParsing = false, forTimestampNTZ = true)
          .format(v.asInstanceOf[Long])
      case BooleanType | _: IntegralType | FloatType | DoubleType => v
      case _: DecimalType => v.asInstanceOf[Decimal].toJavaBigDecimal
      // This version of Spark does not use min/max for binary/string types so we ignore it.
      case _ =>
        throw QueryCompilationErrors.columnStatisticsSerializationNotSupportedError(
          colName, dataType)
    }
    externalValue.toString
  }


  /**
   * Creates a [[CatalogColumnStat]] object from the given map.
   * This is used to deserialize column stats from some external storage.
   * The serialization side is defined in [[CatalogColumnStat.toMap]].
   */
  def fromMap(
    table: String,
    colName: String,
    map: Map[String, String]): Option[CatalogColumnStat] = {

    try {
      Some(CatalogColumnStat(
        distinctCount = map.get(s"${colName}.${KEY_DISTINCT_COUNT}").map(v => BigInt(v.toLong)),
        min = map.get(s"${colName}.${KEY_MIN_VALUE}"),
        max = map.get(s"${colName}.${KEY_MAX_VALUE}"),
        nullCount = map.get(s"${colName}.${KEY_NULL_COUNT}").map(v => BigInt(v.toLong)),
        avgLen = map.get(s"${colName}.${KEY_AVG_LEN}").map(_.toLong),
        maxLen = map.get(s"${colName}.${KEY_MAX_LEN}").map(_.toLong),
        histogram = CatalogTable.readLargeTableProp(map, s"$colName.$KEY_HISTOGRAM")
          .map(HistogramSerializer.deserialize),
        version = map(s"${colName}.${KEY_VERSION}").toInt
      ))
    } catch {
      case NonFatal(e) =>
        logWarning(log"Failed to parse column statistics for column " +
          log"${MDC(COLUMN_NAME, colName)} in table ${MDC(RELATION_NAME, table)}", e)
        None
    }
  }
}


case class CatalogTableType private(name: String)
object CatalogTableType {
  val EXTERNAL = new CatalogTableType("EXTERNAL")
  val MANAGED = new CatalogTableType("MANAGED")
  val VIEW = new CatalogTableType("VIEW")

  val tableTypes = Seq(EXTERNAL, MANAGED, VIEW)
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

  /**
   * Initialize an empty spec.
   */
  lazy val emptyTablePartitionSpec: TablePartitionSpec = Map.empty[String, String]
}

/**
 * A placeholder for a table relation, which will be replaced by concrete relation like
 * `LogicalRelation` or `HiveTableRelation`, during analysis.
 */
case class UnresolvedCatalogRelation(
    tableMeta: CatalogTable,
    options: CaseInsensitiveStringMap = CaseInsensitiveStringMap.empty(),
    override val isStreaming: Boolean = false) extends UnresolvedLeafNode {
  assert(tableMeta.identifier.database.isDefined)
}

/**
 * A wrapper to store the temporary view info, will be kept in `SessionCatalog`
 * and will be transformed to `View` during analysis. If the temporary view is
 * storing an analyzed plan, `plan` is set to the analyzed plan for the view.
 */
case class TemporaryViewRelation(
    tableMeta: CatalogTable,
    plan: Option[LogicalPlan] = None) extends UnresolvedLeafNode {
  require(plan.isEmpty ||
    (plan.get.resolved && tableMeta.properties.contains(VIEW_STORING_ANALYZED_PLAN)))
}

/**
 * A `LogicalPlan` that represents a hive table.
 *
 * TODO: remove this after we completely make hive as a data source.
 */
case class HiveTableRelation(
    tableMeta: CatalogTable,
    dataCols: Seq[AttributeReference],
    partitionCols: Seq[AttributeReference],
    tableStats: Option[Statistics] = None,
    @transient prunedPartitions: Option[Seq[CatalogTablePartition]] = None)
  extends LeafNode with MultiInstanceRelation {
  assert(tableMeta.identifier.database.isDefined)
  assert(DataTypeUtils.sameType(tableMeta.partitionSchema, partitionCols.toStructType))
  assert(DataTypeUtils.sameType(tableMeta.dataSchema, dataCols.toStructType))

  // The partition column should always appear after data columns.
  override def output: Seq[AttributeReference] = dataCols ++ partitionCols

  def isPartitioned: Boolean = partitionCols.nonEmpty

  override def doCanonicalize(): HiveTableRelation = copy(
    tableMeta = CatalogTable.normalize(tableMeta),
    dataCols = dataCols.zipWithIndex.map {
      case (attr, index) => attr.withExprId(ExprId(index))
    },
    partitionCols = partitionCols.zipWithIndex.map {
      case (attr, index) => attr.withExprId(ExprId(index + dataCols.length))
    },
    tableStats = None
  )

  override def computeStats(): Statistics = {
    tableMeta.stats.map(_.toPlanStats(output, conf.cboEnabled || conf.planStatsEnabled))
      .orElse(tableStats)
      .getOrElse {
      throw SparkException.internalError("Table stats must be specified.")
    }
  }

  override def newInstance(): HiveTableRelation = copy(
    dataCols = dataCols.map(_.newInstance()),
    partitionCols = partitionCols.map(_.newInstance()))

  override def simpleString(maxFields: Int): String = {
    val catalogTable = tableMeta.storage.serde match {
      case Some(serde) => tableMeta.identifier :: serde :: Nil
      case _ => tableMeta.identifier :: Nil
    }

    var metadata = Map(
      "CatalogTable" -> catalogTable.mkString(", "),
      "Data Cols" -> truncatedString(dataCols, "[", ", ", "]", maxFields),
      "Partition Cols" -> truncatedString(partitionCols, "[", ", ", "]", maxFields)
    )

    if (prunedPartitions.nonEmpty) {
      metadata += ("Pruned Partitions" -> {
        val parts = prunedPartitions.get.map { part =>
          val spec = part.spec.map { case (k, v) => s"$k=$v" }.mkString(", ")
          if (part.storage.serde.nonEmpty && part.storage.serde != tableMeta.storage.serde) {
            s"($spec, ${part.storage.serde.get})"
          } else {
            s"($spec)"
          }
        }
        truncatedString(parts, "[", ", ", "]", maxFields)
      })
    }

    val metadataEntries = metadata.toSeq.map {
      case (key, value) if key == "CatalogTable" => value
      case (key, value) =>
        key + ": " + StringUtils.abbreviate(value, SQLConf.get.maxMetadataStringLength)
    }

    val metadataStr = truncatedString(metadataEntries, "[", ", ", "]", maxFields)
    s"$nodeName $metadataStr"
  }
}
