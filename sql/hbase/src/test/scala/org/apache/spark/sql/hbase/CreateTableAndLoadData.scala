package org.apache.spark.sql.hbase

import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{TableExistsException, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.spark.Logging

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

/**
 * CreateTableAndLoadData
 *
 */
trait CreateTableAndLoadData extends Logging {
  val DefaultStagingTableName = "StageTable"
  val DefaultTableName = "TestTable"
  val DefaultHbaseStagingTableName = s"Hb$DefaultStagingTableName"
  val DefaultHbaseTabName = s"Hb$DefaultTableName"
  val DefaultHbaseColFamilies = Seq("cf1", "cf2")

  val CsvPaths = Array("src/test/resources", "sql/hbase/src/test/resources")
  val DefaultLoadFile = "testTable.txt"

  var AvoidRowkeyBug = false

  var AvoidIfNotExistsBug = true

  val ifNotExists = if (!AvoidIfNotExistsBug) "IF NOT EXISTS" else ""

  private val tpath = for (csvPath <- CsvPaths
                           if new java.io.File(csvPath).exists()
  ) yield {
    logInfo(s"Following path exists $csvPath")
    csvPath
  }
  private[hbase] val CsvPath = tpath(0)

  def createTableAndLoadData(): Unit = {
    createTables(DefaultStagingTableName, DefaultTableName,
      DefaultHbaseStagingTableName, DefaultHbaseTabName)
    loadData(DefaultStagingTableName, DefaultTableName, s"$CsvPath/$DefaultLoadFile")
  }

  def createTables(): Unit = {
    createTables(DefaultStagingTableName, DefaultTableName,
      DefaultHbaseStagingTableName, DefaultHbaseTabName)
  }

  def createNativeHbaseTable(tableName: String, families: Seq[String]) = {
    val hbaseAdmin = TestHbase.hbaseAdmin
    val hdesc = new HTableDescriptor(TableName.valueOf(tableName))
    families.foreach { f => hdesc.addFamily(new HColumnDescriptor(f))}
    try {
      hbaseAdmin.createTable(hdesc)
    } catch {
      case e: TableExistsException =>
        logError(s"Table already exists $tableName", e)
    }
  }

  def createTables(stagingTableName: String, tableName: String,
                   hbaseStagingTable: String, hbaseTable: String) = {
    val hbaseAdmin = TestHbase.hbaseAdmin
    if (!hbaseAdmin.tableExists(TableName.valueOf(hbaseStagingTable))) {
      createNativeHbaseTable(hbaseStagingTable, DefaultHbaseColFamilies)
    }
    if (!hbaseAdmin.tableExists(TableName.valueOf(hbaseTable))) {
      createNativeHbaseTable(hbaseTable, DefaultHbaseColFamilies)
    }

    if (TestHbase.catalog.checkLogicalTableExist(stagingTableName)) {
      val dropSql = s"drop table $stagingTableName"
      runSql(dropSql)
    }

    if (TestHbase.catalog.checkLogicalTableExist(tableName)) {
      val dropSql = s"drop table $tableName"
      runSql(dropSql)
    }

    val (stagingSql, tabSql) =
      ( s"""CREATE TABLE $ifNotExists $stagingTableName(strcol STRING, bytecol String, shortcol String, intcol String,
            longcol string, floatcol string, doublecol string, PRIMARY KEY(doublecol, strcol, intcol))
            MAPPED BY ($hbaseStagingTable, COLS=[bytecol=cf1.hbytecol,
            shortcol=cf1.hshortcol, longcol=cf2.hlongcol, floatcol=cf2.hfloatcol])"""
        .stripMargin
        ,
        s"""CREATE TABLE $tableName(strcol STRING, bytecol BYTE, shortcol SHORT, intcol INTEGER,
            longcol LONG, floatcol FLOAT, doublecol DOUBLE, PRIMARY KEY(doublecol, strcol, intcol))
            MAPPED BY ($hbaseTable, COLS=[bytecol=cf1.hbytecol,
            shortcol=cf1.hshortcol, longcol=cf2.hlongcol, floatcol=cf2.hfloatcol])"""
          .stripMargin
        )
    try {
      logInfo(s"invoking $stagingSql ..")
      runSql(stagingSql)
    } catch {
      case e: TableExistsException =>
        logInfo("IF NOT EXISTS still not implemented so we get the following exception", e)
    }

    logDebug(s"Created table $tableName: " +
      s"isTableAvailable= ${hbaseAdmin.isTableAvailable(s2b(hbaseStagingTable))}" +
      s" tableDescriptor= ${hbaseAdmin.getTableDescriptor(s2b(hbaseStagingTable))}")

    try {
      logInfo(s"invoking $tabSql ..")
      runSql(tabSql)
    } catch {
      case e: TableExistsException =>
        logInfo("IF NOT EXISTS still not implemented so we get the following exception", e)
    }
  }

  def runSql(sql: String) = {
    logInfo(sql)
    TestHbase.sql(sql).collect()
  }

  def loadData(stagingTableName: String, tableName: String, loadFile: String) = {

    // then load data into table
    val loadSql = s"LOAD DATA LOCAL INPATH '$loadFile' INTO TABLE $tableName"
    runSql(loadSql)

    val query1 = s"select * from $tableName"

    val result1 = runSql(query1)
//    assert(result1.size == 3)
  }

  def cleanUp() = {
    // delete the temp files
    val sparkHome = TestHbase.sparkContext.getSparkHome().getOrElse(".")

    val fileSystem = FileSystem.get(TestHbase.sparkContext.hadoopConfiguration)
    val files = fileSystem.listStatus(new Path(sparkHome))
    for (file <- files) {
      println(file.getPath.getName)
      if (file.getPath.getName.indexOf(DefaultTableName) != -1) {
        fileSystem.delete(file.getPath, true)
      }
    }
  }

  def s2b(s: String) = Bytes.toBytes(s)

}
