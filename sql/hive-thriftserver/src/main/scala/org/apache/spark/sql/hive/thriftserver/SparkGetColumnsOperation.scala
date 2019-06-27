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

package org.apache.spark.sql.hive.thriftserver

import java.util.UUID
import java.util.regex.Pattern

import scala.collection.JavaConverters.seqAsJavaListConverter

import org.apache.hadoop.hive.ql.security.authorization.plugin.{HiveOperationType, HivePrivilegeObject}
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject.HivePrivilegeObjectType
import org.apache.hive.service.cli._
import org.apache.hive.service.cli.operation.GetColumnsOperation
import org.apache.hive.service.cli.session.HiveSession

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.SessionCatalog
import org.apache.spark.sql.hive.thriftserver.ThriftserverShimUtils.toJavaSQLType

/**
 * Spark's own SparkGetColumnsOperation
 *
 * @param sqlContext SQLContext to use
 * @param parentSession a HiveSession from SessionManager
 * @param catalogName catalog name. NULL if not applicable.
 * @param schemaName database name, NULL or a concrete database name
 * @param tableName table name
 * @param columnName column name
 */
private[hive] class SparkGetColumnsOperation(
    sqlContext: SQLContext,
    parentSession: HiveSession,
    catalogName: String,
    schemaName: String,
    tableName: String,
    columnName: String)
  extends GetColumnsOperation(parentSession, catalogName, schemaName, tableName, columnName)
    with Logging {

  val catalog: SessionCatalog = sqlContext.sessionState.catalog

  private var statementId: String = _

  override def runInternal(): Unit = {
    val cmdStr = s"catalog : $catalogName, schemaPattern : $schemaName, tablePattern : $tableName" +
      s", columnName : $columnName"
    logInfo(s"GetColumnsOperation: $cmdStr")

    setState(OperationState.RUNNING)
    // Always use the latest class loader provided by executionHive's state.
    val executionHiveClassLoader = sqlContext.sharedState.jarClassLoader
    Thread.currentThread().setContextClassLoader(executionHiveClassLoader)

    val schemaPattern = convertSchemaPattern(schemaName)
    val tablePattern = convertIdentifierPattern(tableName, true)

    var columnPattern: Pattern = null
    if (columnName != null) {
      columnPattern = Pattern.compile(convertIdentifierPattern(columnName, false))
    }

    val db2Tabs = catalog.listDatabases(schemaPattern).map { dbName =>
      (dbName, catalog.listTables(dbName, tablePattern))
    }.toMap

    if (isAuthV2Enabled) {
      val privObjs = seqAsJavaListConverter(getPrivObjs(db2Tabs)).asJava
      val cmdStr =
        s"catalog : $catalogName, schemaPattern : $schemaName, tablePattern : $tableName"
      authorizeMetaGets(HiveOperationType.GET_COLUMNS, privObjs, cmdStr)
    }

    try {
      db2Tabs.foreach {
        case (dbName, tables) =>
          catalog.getTablesByName(tables).foreach { catalogTable =>
            catalogTable.schema.foreach { column =>
              if (columnPattern != null && !columnPattern.matcher(column.name).matches()) {
              } else {
                val rowData = Array[AnyRef](
                  null,  // TABLE_CAT
                  dbName, // TABLE_SCHEM
                  catalogTable.identifier.table, // TABLE_NAME
                  column.name, // COLUMN_NAME
                  toJavaSQLType(column.dataType.sql).asInstanceOf[AnyRef], // DATA_TYPE
                  column.dataType.sql, // TYPE_NAME
                  null, // COLUMN_SIZE
                  null, // BUFFER_LENGTH, unused
                  null, // DECIMAL_DIGITS
                  null, // NUM_PREC_RADIX
                  (if (column.nullable) 1 else 0).asInstanceOf[AnyRef], // NULLABLE
                  column.getComment().getOrElse(""), // REMARKS
                  null, // COLUMN_DEF
                  null, // SQL_DATA_TYPE
                  null, // SQL_DATETIME_SUB
                  null, // CHAR_OCTET_LENGTH
                  null, // ORDINAL_POSITION
                  "YES", // IS_NULLABLE
                  null, // SCOPE_CATALOG
                  null, // SCOPE_SCHEMA
                  null, // SCOPE_TABLE
                  null, // SOURCE_DATA_TYPE
                  "NO" // IS_AUTO_INCREMENT
                )
                rowSet.addRow(rowData)
              }
            }
          }
      }
      setState(OperationState.FINISHED)
    } catch {
      case e: HiveSQLException =>
        setState(OperationState.ERROR)
        throw e
    }
  }

  private def getPrivObjs(db2Tabs: Map[String, Seq[TableIdentifier]]): Seq[HivePrivilegeObject] = {
    db2Tabs.foldLeft(Seq.empty[HivePrivilegeObject])({
      case (i, (dbName, tables)) => i ++ tables.map { tableId =>
        new HivePrivilegeObject(HivePrivilegeObjectType.TABLE_OR_VIEW, dbName, tableId.table)
      }
    })
  }
}
