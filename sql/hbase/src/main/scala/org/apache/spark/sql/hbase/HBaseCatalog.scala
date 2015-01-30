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
package org.apache.spark.sql.hbase

import java.io._
import java.util.zip._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.log4j.Logger
import org.apache.spark.Logging
import org.apache.spark.sql.catalyst.analysis.{Catalog, OverrideCatalog}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Subquery}
import org.apache.spark.sql.types._
import org.apache.spark.sql.hbase.HBaseCatalog._

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
 * Column represent the sql column
 * sqlName the name of the column
 * dataType the data type of the column
 */
sealed abstract class AbstractColumn extends Serializable {
  val sqlName: String
  val dataType: DataType
  var ordinal: Int = -1

  def isKeyColumn: Boolean

  override def toString: String = {
    s"$sqlName , $dataType.typeName"
  }
}

case class KeyColumn(sqlName: String, dataType: DataType, order: Int)
  extends AbstractColumn {
  override def isKeyColumn: Boolean = true
}

case class NonKeyColumn(sqlName: String, dataType: DataType, family: String, qualifier: String)
  extends AbstractColumn {
  @transient lazy val familyRaw = Bytes.toBytes(family)
  @transient lazy val qualifierRaw = Bytes.toBytes(qualifier)

  override def isKeyColumn: Boolean = false

  override def toString = {
    s"$sqlName , $dataType.typeName , $family:$qualifier"
  }
}

private[hbase] class HBaseCatalog(@transient hbaseContext: HBaseSQLContext,
                                  @transient configuration: Configuration)
  extends Catalog with Logging with Serializable {

  lazy val logger = Logger.getLogger(getClass.getName)

  lazy val relationMapCache = new mutable.HashMap[String, HBaseRelation]
    with mutable.SynchronizedMap[String, HBaseRelation]

  private var admin = new HBaseAdmin(configuration)

  private def processTableName(tableName: String): String = {
    if (!caseSensitive) {
      tableName.toLowerCase
    } else {
      tableName
    }
  }

  val caseSensitive = true

  private def createHBaseUserTable(tableName: String,
                                   families: Set[String],
                                   splitKeys: Array[Array[Byte]]): Unit = {
    val tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName))
    families.foreach(family => tableDescriptor.addFamily(new HColumnDescriptor(family)))

    try {
      if (splitKeys == null) {
        admin.createTable(tableDescriptor)
      } else {
        admin.createTable(tableDescriptor, splitKeys)
      }
    } catch {
      case e: Throwable =>
        admin = new HBaseAdmin(configuration)
        admin.createTable(tableDescriptor, splitKeys)
    }
  }

  def createTable(tableName: String, hbaseNamespace: String, hbaseTableName: String,
                  allColumns: Seq[AbstractColumn], splitKeys: Array[Array[Byte]]): HBaseRelation = {
    val metadataTable = getMetadataTable

    if (checkLogicalTableExist(tableName, metadataTable)) {
      throw new Exception(s"The logical table: $tableName already exists")
    }

    // create a new hbase table for the user if not exist
    val nonKeyColumns = allColumns.filter(_.isInstanceOf[NonKeyColumn])
      .asInstanceOf[Seq[NonKeyColumn]]
    val families = nonKeyColumns.map(_.family).toSet
    if (!checkHBaseTableExists(hbaseTableName)) {
      createHBaseUserTable(hbaseTableName, families, splitKeys)
    } else {
      families.foreach {
        case family =>
          if (!checkFamilyExists(hbaseTableName, family)) {
            throw new Exception(s"The HBase table doesn't contain the Column Family: $family")
          }
      }
    }

    metadataTable.setAutoFlushTo(false)

    val get = new Get(Bytes.toBytes(tableName))
    val result = if (metadataTable.exists(get)) {
      throw new Exception(s"row key $tableName exists")
    } else {
      val hbaseRelation = HBaseRelation(tableName, hbaseNamespace, hbaseTableName,
        allColumns)(hbaseContext)
      hbaseRelation.setConfig(configuration)

      writeObjectToTable(hbaseRelation, metadataTable)

      relationMapCache.put(processTableName(tableName), hbaseRelation)
      hbaseRelation
    }

    metadataTable.close()
    result
  }

  def alterTableDropNonKey(tableName: String, columnName: String) = {
    val metadataTable = getMetadataTable
    val result = getTable(tableName, metadataTable)
    if (result.isDefined) {
      val relation = result.get
      val allColumns = relation.allColumns.filter(_.sqlName != columnName)
      val hbaseRelation = HBaseRelation(relation.tableName,
        relation.hbaseNamespace, relation.hbaseTableName, allColumns)(hbaseContext)
      hbaseRelation.setConfig(configuration)

      writeObjectToTable(hbaseRelation, metadataTable)

      relationMapCache.put(processTableName(tableName), hbaseRelation)
    }
    metadataTable.close()
  }

  def alterTableAddNonKey(tableName: String, column: NonKeyColumn) = {
    val metadataTable = getMetadataTable
    val result = getTable(tableName, metadataTable)
    if (result.isDefined) {
      val relation = result.get
      val allColumns = relation.allColumns :+ column
      val hbaseRelation = HBaseRelation(relation.tableName,
        relation.hbaseNamespace, relation.hbaseTableName, allColumns)(hbaseContext)
      hbaseRelation.setConfig(configuration)

      writeObjectToTable(hbaseRelation, metadataTable)

      relationMapCache.put(processTableName(tableName), hbaseRelation)
    }
    metadataTable.close()
  }

  private def writeObjectToTable(hbaseRelation: HBaseRelation,
                                 metadataTable: HTable) = {
    val tableName = hbaseRelation.tableName

    val put = new Put(Bytes.toBytes(tableName))
    val byteArrayOutputStream = new ByteArrayOutputStream()
    val deflaterOutputStream = new DeflaterOutputStream(byteArrayOutputStream)
    val objectOutputStream = new ObjectOutputStream(deflaterOutputStream)

    objectOutputStream.writeObject(hbaseRelation)
    objectOutputStream.close()

    put.add(ColumnFamily, QualData, byteArrayOutputStream.toByteArray)

    // write to the metadata table
    metadataTable.put(put)
    metadataTable.flushCommits()
    metadataTable.close()
  }

  def getTable(tableName: String,
               metadataTable_ : HTable = null): Option[HBaseRelation] = {
    val (metadataTable, needToCloseAtTheEnd) = {
      if (metadataTable_ == null) (getMetadataTable, true)
      else (metadataTable_, false)
    }

    var result = relationMapCache.get(processTableName(tableName))
    if (result.isEmpty) {
      val get = new Get(Bytes.toBytes(tableName))
      val values = metadataTable.get(get)
      if (values == null || values.isEmpty) {
        result = None
      } else {
        result = Some(getRelationFromResult(values))
      }
    }
    if (result.isDefined) {
      result.get.fetchPartitions()
    }
    if (needToCloseAtTheEnd) metadataTable.close()

    result
  }

  private def getRelationFromResult(result: Result): HBaseRelation = {
    val value = result.getValue(ColumnFamily, QualData)
    val byteArrayInputStream = new ByteArrayInputStream(value)
    val inflaterInputStream = new InflaterInputStream(byteArrayInputStream)
    val objectInputStream = new ObjectInputStream(inflaterInputStream)
    val hbaseRelation: HBaseRelation
    = objectInputStream.readObject().asInstanceOf[HBaseRelation]
    hbaseRelation.context = hbaseContext
    hbaseRelation.setConfig(configuration)
    hbaseRelation
  }

  def getAllTableName: Seq[String] = {
    val metadataTable = getMetadataTable
    val tables = new ArrayBuffer[String]()
    val scanner = metadataTable.getScanner(ColumnFamily)
    var result = scanner.next()
    while (result != null) {
      val relation = getRelationFromResult(result)
      tables.append(relation.tableName)
      result = scanner.next()
    }
    metadataTable.close()
    tables.toSeq
  }

  override def lookupRelation(tableIdentifier: Seq[String],
                     alias: Option[String] = None): LogicalPlan = {
    val tableName = tableIdentifier(0)
    val hbaseRelation = getTable(tableName)
    if (hbaseRelation.isEmpty) {
      sys.error(s"Table Not Found: $tableName")
    } else {
      val tableWithQualifiers = Subquery(tableName, hbaseRelation.get.logicalRelation)
      alias.map(a => Subquery(a.toLowerCase, tableWithQualifiers)).getOrElse(tableWithQualifiers)
    }
  }

  def deleteTable(tableName: String): Unit = {
    val metadataTable = getMetadataTable
    if (!checkLogicalTableExist(tableName, metadataTable)) {
      throw new IllegalStateException(s"The logical table $tableName does not exist")
    }

    val delete = new Delete(Bytes.toBytes(tableName))
    metadataTable.delete(delete)
    metadataTable.close()
    relationMapCache.remove(processTableName(tableName))
  }

  def getMetadataTable: HTable = {
    // create the metadata table if it does not exist
    def checkAndCreateMetadataTable() = {
      if (!admin.tableExists(MetaData)) {
        val descriptor = new HTableDescriptor(TableName.valueOf(MetaData))
        val columnDescriptor = new HColumnDescriptor(ColumnFamily)
        descriptor.addFamily(columnDescriptor)
        admin.createTable(descriptor)
      }
    }

    try {
      checkAndCreateMetadataTable()
    } catch {
      case e: Throwable =>
        admin = new HBaseAdmin(configuration)
        checkAndCreateMetadataTable()
    }

    // return the metadata table
    new HTable(configuration, MetaData)
  }

  private[hbase] def checkHBaseTableExists(hbaseTableName: String): Boolean = {
    try {
      admin.tableExists(hbaseTableName)
    } catch {
      case e: Throwable =>
        admin = new HBaseAdmin(configuration)
        admin.tableExists(hbaseTableName)
    }
  }

  override def tableExists(tableIdentifier: Seq[String]): Boolean = {
    val tableName = tableIdentifier(0)
    checkLogicalTableExist(tableName)
  }

  private[hbase] def checkLogicalTableExist(tableName: String,
                                            metadataTable_ : HTable = null): Boolean = {
    val (metadataTable, needToCloseAtTheEnd) = {
      if (metadataTable_ == null) (getMetadataTable, true)
      else (metadataTable_, false)
    }
    val get = new Get(Bytes.toBytes(tableName))
    val result = metadataTable.get(get)

    if (needToCloseAtTheEnd) metadataTable.close()
    result.size() > 0
  }

  private[hbase] def checkFamilyExists(hbaseTableName: String, family: String): Boolean = {
    try {
      val tableDescriptor = admin.getTableDescriptor(TableName.valueOf(hbaseTableName))
      tableDescriptor.hasFamily(Bytes.toBytes(family))
    } catch {
      case e: Throwable =>
        admin = new HBaseAdmin(configuration)
        val tableDescriptor = admin.getTableDescriptor(TableName.valueOf(hbaseTableName))
        tableDescriptor.hasFamily(Bytes.toBytes(family))
    }
  }

  def getDataType(dataType: String): DataType = {
    if (dataType.equalsIgnoreCase(StringType.typeName)) {
      StringType
    } else if (dataType.equalsIgnoreCase(ByteType.typeName)) {
      ByteType
    } else if (dataType.equalsIgnoreCase(ShortType.typeName)) {
      ShortType
    } else if (dataType.equalsIgnoreCase(IntegerType.typeName) ||
      dataType.equalsIgnoreCase("int")) {
      IntegerType
    } else if (dataType.equalsIgnoreCase(LongType.typeName)) {
      LongType
    } else if (dataType.equalsIgnoreCase(FloatType.typeName)) {
      FloatType
    } else if (dataType.equalsIgnoreCase(DoubleType.typeName)) {
      DoubleType
    } else if (dataType.equalsIgnoreCase(BooleanType.typeName)) {
      BooleanType
    } else {
      throw new IllegalArgumentException(s"Unrecognized data type: $dataType")
    }
  }

  /**
   * UNIMPLEMENTED: It needs to be decided how we will persist in-memory tables to the metastore.
   * For now, if this functionality is desired mix in the in-memory [[OverrideCatalog]].
   */
  override def registerTable(tableIdentifier: Seq[String], plan: LogicalPlan): Unit = {}

  override def unregisterTable(tableIdentifier: Seq[String]): Unit = {}

  override def unregisterAllTables(): Unit = {}
}

object HBaseCatalog {
  private final val MetaData = "metadata"
  private final val ColumnFamily = Bytes.toBytes("colfam")
  private final val QualData = Bytes.toBytes("data")
}
