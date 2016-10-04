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

package org.apache.spark.sql.hive

import java.io.IOException

import scala.collection.JavaConverters._

import com.google.common.base.Objects
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hive.common.StatsSetupConst
import org.apache.hadoop.hive.metastore.{TableType => HiveTableType}
import org.apache.hadoop.hive.metastore.api.FieldSchema
import org.apache.hadoop.hive.ql.metadata.{Partition, Table => HiveTable}
import org.apache.hadoop.hive.ql.plan.TableDesc

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.MultiInstanceRelation
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LeafNode, LogicalPlan, Statistics}
import org.apache.spark.sql.execution.FileRelation
import org.apache.spark.sql.hive.client.HiveClient


private[hive] case class MetastoreRelation(
    databaseName: String,
    tableName: String,
    alias: Option[String])
    (val catalogTable: CatalogTable,
     @transient private val client: HiveClient,
     @transient private val sparkSession: SparkSession)
  extends LeafNode with MultiInstanceRelation with FileRelation with CatalogRelation {

  override def equals(other: Any): Boolean = other match {
    case relation: MetastoreRelation =>
      databaseName == relation.databaseName &&
        tableName == relation.tableName &&
        alias == relation.alias &&
        output == relation.output
    case _ => false
  }

  override def hashCode(): Int = {
    Objects.hashCode(databaseName, tableName, alias, output)
  }

  override protected def otherCopyArgs: Seq[AnyRef] = catalogTable :: client :: sparkSession :: Nil

  private def toHiveColumn(c: CatalogColumn): FieldSchema = {
    new FieldSchema(c.name, c.dataType, c.comment.orNull)
  }

  // TODO: merge this with HiveClientImpl#toHiveTable
  @transient val hiveQlTable: HiveTable = {
    // We start by constructing an API table as Hive performs several important transformations
    // internally when converting an API table to a QL table.
    val tTable = new org.apache.hadoop.hive.metastore.api.Table()
    tTable.setTableName(catalogTable.identifier.table)
    tTable.setDbName(catalogTable.database)

    val tableParameters = new java.util.HashMap[String, String]()
    tTable.setParameters(tableParameters)
    catalogTable.properties.foreach { case (k, v) => tableParameters.put(k, v) }

    tTable.setTableType(catalogTable.tableType match {
      case CatalogTableType.EXTERNAL => HiveTableType.EXTERNAL_TABLE.toString
      case CatalogTableType.MANAGED => HiveTableType.MANAGED_TABLE.toString
      case CatalogTableType.VIEW => HiveTableType.VIRTUAL_VIEW.toString
    })

    val sd = new org.apache.hadoop.hive.metastore.api.StorageDescriptor()
    tTable.setSd(sd)

    // Note: In Hive the schema and partition columns must be disjoint sets
    val (partCols, schema) = catalogTable.schema.map(toHiveColumn).partition { c =>
      catalogTable.partitionColumnNames.contains(c.getName)
    }
    sd.setCols(schema.asJava)
    tTable.setPartitionKeys(partCols.asJava)

    catalogTable.storage.locationUri.foreach(sd.setLocation)
    catalogTable.storage.inputFormat.foreach(sd.setInputFormat)
    catalogTable.storage.outputFormat.foreach(sd.setOutputFormat)

    val serdeInfo = new org.apache.hadoop.hive.metastore.api.SerDeInfo
    catalogTable.storage.serde.foreach(serdeInfo.setSerializationLib)
    sd.setSerdeInfo(serdeInfo)

    val serdeParameters = new java.util.HashMap[String, String]()
    catalogTable.storage.serdeProperties.foreach { case (k, v) => serdeParameters.put(k, v) }
    serdeInfo.setParameters(serdeParameters)

    new HiveTable(tTable)
  }

  @transient def getSize(): Long = {
    val totalSize = hiveQlTable.getParameters.get(StatsSetupConst.TOTAL_SIZE)
    val rawDataSize = hiveQlTable.getParameters.get(StatsSetupConst.RAW_DATA_SIZE)

    // NOTE: getting `totalSize` directly from params is kind of hacky, but this should be
    // relatively cheap if parameters for the table are populated into the metastore.
    // Besides `totalSize`, there are also `numFiles`, `numRows`, `rawDataSize` keys
    // (see StatsSetupConst in Hive) that we can look at in the future.

    // When table is external,`totalSize` is always zero, which will influence join strategy
    // so when `totalSize` is zero, use `rawDataSize` instead
    // if the size is still less than zero, we try to get the file size from HDFS.
    // given this is only needed for optimization, if the HDFS call fails we return the default.
    if (totalSize != null && totalSize.toLong > 0L) {
      totalSize.toLong
    } else if (rawDataSize != null && rawDataSize.toLong > 0) {
      rawDataSize.toLong
    } else if (sparkSession.sessionState.conf.fallBackToHdfsForStatsEnabled) {
      try {
        val hadoopConf = sparkSession.sessionState.newHadoopConf()
        val fs: FileSystem = hiveQlTable.getPath.getFileSystem(hadoopConf)
        fs.getContentSummary(hiveQlTable.getPath).getLength
      } catch {
        case e: IOException =>
          logWarning("Failed to get table size from hdfs.", e)
          sparkSession.sessionState.conf.defaultSizeInBytes
      }
    } else {
      sparkSession.sessionState.conf.defaultSizeInBytes
    }
  }

  @transient override def statistics(implicit parents: Option[Seq[LogicalPlan]] = None):
  Statistics = Statistics(
    sizeInBytes = {
      BigInt(
        if (sparkSession.sessionState.conf.prunedPartitionStatsEnabled) {
          try {
            // Get the immediate parent, if its not a filter we won't do the optimization.
            // This is rigid for now but easy to traverse up the tree and find all filter blocks.
            // but need to verify that will still lead to correct behavior.
            val parentNodes = parents.getOrElse(Seq.empty[LogicalPlan])
            val filterNode: Option[Filter] =
              if (parentNodes.isEmpty || !parentNodes.head.isInstanceOf[Filter]) {
              None
            } else {
              Some(parentNodes.head.asInstanceOf[Filter])
            }
            filterNode match {
              case Some(filter) =>
                val partitionPaths = getPartitionPathFromFilter(filter)
                if (partitionPaths.nonEmpty) {
                  val hadoopConf = sparkSession.sessionState.newHadoopConf()
                  val fs: FileSystem = hiveQlTable.getPath.getFileSystem(hadoopConf)
                  partitionPaths.foldLeft(0L)(
                    (sum, path) => sum + fs.getContentSummary(path).getLength
                  )
                } else {
                  getSize
                }
              case None => getSize
            }
          } catch {
            case e: IOException =>
              logWarning("Failed to get table size from hdfs.", e)
              getSize
          }
        } else {
          getSize
        })
    }
  )

  // Return list of paths for partitions that are valid for this operation, an empty list
  // if no partition prunning is possible.
  private def getPartitionPathFromFilter(filter: Filter): Seq[Path] = {

    // TODO: For now just considering equalTO and IN, We could add more operators later.

    // Following is the expected output for different cases:
    // (p1 = b and p2 = c) or (p1 = d) should yield [p1=b/p2=c, p1=d]
    // (p1 = b or p2 = c) should yield [p1=b, p2=c]
    // (p1 = b or p2 = c) and (p3 = d) should yield [p1=b/p3=d, p2=c/p3=d]
    // (p1 = b or p2 = c) or (p1 = d) should yield [p1=b, p2=c, p1=d]
    // p1 IN (a,b,c) should yield [p1=a,p1=b,p1=c]
    // If a column appears in any unsupported expression, we discard it from
    // size estimate calculation
    // TODO: We may still return incorrect size estimates due to operators that can add
    // partition space and are higher up in parse tree so the columns never get blacklisted.
    // i.e. if we have 'not (partition=1 or partition=2)' current code will provide estimate
    // based on sizes of partition 1 and 2.
    var blackListedPartitionColumns: Set[AttributeReference] = Set.empty[AttributeReference]

    def getPartitionInfo(expression: Expression): Seq[Map[AttributeReference, String]] = {

      def getPartitionTuple(attribute: AttributeReference, literal: Literal) = {
        if (partitionKeys.contains(attribute)) {
          Seq(Map(attribute -> literal.value.toString))
        } else {
          Seq.empty[Map[AttributeReference, String]]
        }
      }

      expression match {
        // Join adds notnull checks for all filter columns,
        // to avoid blacklisting we handle it by returning empty map.
        case e: IsNotNull => Seq.empty[Map[AttributeReference, String]]

        // If a partition column appears under Not expression, we black list it.
        case e: Not =>
          getPartitionInfo(e.child).foldLeft(blackListedPartitionColumns)(
            (s, m) => s ++ m.keySet
          )
          Seq.empty[Map[AttributeReference, String]]

        case e: EqualTo => (e.left, e.right) match {
          case (attr : AttributeReference, l: Literal) => getPartitionTuple(attr, l)
          case (l: Literal, attr : AttributeReference) => getPartitionTuple(attr, l)
          case _ => getPartitionInfo(e.left) ++ getPartitionInfo(e.right)
        }

        // In: Only processes a partition in list of literal values.
        case e: In => (e.value, e.list)
          if (e.value.isInstanceOf[AttributeReference] &&
            e.list.filter(!_.isInstanceOf[Literal]).nonEmpty) {
              e.list.map(literal => getPartitionTuple(e.value.asInstanceOf[AttributeReference],
                literal.asInstanceOf[Literal])).flatten
            } else {
              getPartitionInfo(e.value) ++ e.list.flatMap(getPartitionInfo(_))
            }

        case e: And =>
          val right = getPartitionInfo(e.right)
          val left = getPartitionInfo(e.left)

          if (left.isEmpty) {
            right
          } else if (right.isEmpty) {
            left
          } else {
            left.map(lMap => right.map(rMap => if (lMap.keySet.intersect(rMap.keySet).isEmpty) {
              lMap ++ rMap
            } else {
              Map.empty[AttributeReference, String]
            })).flatten.filter(!_.isEmpty)
          }

        case e: Or => getPartitionInfo(e.left) ++ getPartitionInfo(e.right)

        case e: AttributeReference =>
          // If any of our partition keys are part of a filter condition that
          // we do not handle, we should ignore those columns from size estimation
          // for correctness.
          if (partitionKeys.contains(e)) {
            blackListedPartitionColumns = blackListedPartitionColumns + e
          }
          Seq.empty[Map[AttributeReference, String]]

        case ex: Expression =>
          if (expression.children.nonEmpty) {
          expression.children.foldLeft(Seq.empty[Map[AttributeReference, String]]) (
            (r, e) => r ++ getPartitionInfo(e)
          )
        } else {
            Seq.empty[Map[AttributeReference, String]]
          }
      }
    }

    val partitionsWithEqualityCheck = getPartitionInfo(filter.condition).filter(!_.isEmpty)

    // Build partition Paths in the same order as the storage layer, as soon as first
    // missing partition is found we have to stop.
    partitionsWithEqualityCheck.map(
      m => {
      var missingPartitionCol = false
      var blacklistedCol = false
      var partitionPath = hiveQlTable.getPath

      for(partitionKey <- partitionKeys if !missingPartitionCol && !blacklistedCol) {
         if (m.keySet.contains(partitionKey)) {
            if (blackListedPartitionColumns.contains(partitionKey)) {
              blacklistedCol = true
              partitionPath = hiveQlTable.getPath
            } else {
              val path = partitionKey.name + "=" + m.get(partitionKey).get
              partitionPath = new Path(partitionPath, path)
            }
          } else {
            missingPartitionCol = true
          }
        }
        partitionPath
    }).filter(!_.equals(hiveQlTable.getPath))
  }

  // When metastore partition pruning is turned off, we cache the list of all partitions to
  // mimic the behavior of Spark < 1.5
  private lazy val allPartitions: Seq[CatalogTablePartition] = client.getPartitions(catalogTable)

  def getHiveQlPartitions(predicates: Seq[Expression] = Nil): Seq[Partition] = {
    val rawPartitions = if (sparkSession.sessionState.conf.metastorePartitionPruning) {
      client.getPartitionsByFilter(catalogTable, predicates)
    } else {
      allPartitions
    }

    rawPartitions.map { p =>
      val tPartition = new org.apache.hadoop.hive.metastore.api.Partition
      tPartition.setDbName(databaseName)
      tPartition.setTableName(tableName)
      tPartition.setValues(partitionKeys.map(a => p.spec(a.name)).asJava)

      val sd = new org.apache.hadoop.hive.metastore.api.StorageDescriptor()
      tPartition.setSd(sd)

      // Note: In Hive the schema and partition columns must be disjoint sets
      val schema = catalogTable.schema.map(toHiveColumn).filter { c =>
        !catalogTable.partitionColumnNames.contains(c.getName)
      }
      sd.setCols(schema.asJava)

      p.storage.locationUri.foreach(sd.setLocation)
      p.storage.inputFormat.foreach(sd.setInputFormat)
      p.storage.outputFormat.foreach(sd.setOutputFormat)

      val serdeInfo = new org.apache.hadoop.hive.metastore.api.SerDeInfo
      sd.setSerdeInfo(serdeInfo)
      // maps and lists should be set only after all elements are ready (see HIVE-7975)
      p.storage.serde.foreach(serdeInfo.setSerializationLib)

      val serdeParameters = new java.util.HashMap[String, String]()
      catalogTable.storage.serdeProperties.foreach { case (k, v) => serdeParameters.put(k, v) }
      p.storage.serdeProperties.foreach { case (k, v) => serdeParameters.put(k, v) }
      serdeInfo.setParameters(serdeParameters)

      new Partition(hiveQlTable, tPartition)
    }
  }

  /** Only compare database and tablename, not alias. */
  override def sameResult(plan: LogicalPlan): Boolean = {
    plan.canonicalized match {
      case mr: MetastoreRelation =>
        mr.databaseName == databaseName && mr.tableName == tableName
      case _ => false
    }
  }

  val tableDesc = new TableDesc(
    hiveQlTable.getInputFormatClass,
    // The class of table should be org.apache.hadoop.hive.ql.metadata.Table because
    // getOutputFormatClass will use HiveFileFormatUtils.getOutputFormatSubstitute to
    // substitute some output formats, e.g. substituting SequenceFileOutputFormat to
    // HiveSequenceFileOutputFormat.
    hiveQlTable.getOutputFormatClass,
    hiveQlTable.getMetadata
  )

  implicit class SchemaAttribute(f: CatalogColumn) {
    def toAttribute: AttributeReference = AttributeReference(
      f.name,
      CatalystSqlParser.parseDataType(f.dataType),
      // Since data can be dumped in randomly with no validation, everything is nullable.
      nullable = true
    )(qualifier = Some(alias.getOrElse(tableName)))
  }

  /** PartitionKey attributes */
  val partitionKeys = catalogTable.partitionColumns.map(_.toAttribute)

  /** Non-partitionKey attributes */
  // TODO: just make this hold the schema itself, not just non-partition columns
  val attributes = catalogTable.schema
    .filter { c => !catalogTable.partitionColumnNames.contains(c.name) }
    .map(_.toAttribute)

  val output = attributes ++ partitionKeys

  /** An attribute map that can be used to lookup original attributes based on expression id. */
  val attributeMap = AttributeMap(output.map(o => (o, o)))

  /** An attribute map for determining the ordinal for non-partition columns. */
  val columnOrdinals = AttributeMap(attributes.zipWithIndex)

  override def inputFiles: Array[String] = {
    val partLocations = client
      .getPartitionsByFilter(catalogTable, Nil)
      .flatMap(_.storage.locationUri)
      .toArray
    if (partLocations.nonEmpty) {
      partLocations
    } else {
      Array(
        catalogTable.storage.locationUri.getOrElse(
          sys.error(s"Could not get the location of ${catalogTable.qualifiedName}.")))
    }
  }

  override def newInstance(): MetastoreRelation = {
    MetastoreRelation(databaseName, tableName, alias)(catalogTable, client, sparkSession)
  }
}
