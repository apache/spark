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

import java.io.{ByteArrayOutputStream, DataOutputStream}

import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.log4j.Logger
import org.apache.spark.Logging
import org.apache.spark.sql.SchemaRDD
import org.apache.spark.sql.catalyst.expressions.{GenericRow, Row}
import org.apache.spark.sql.catalyst.types._
import org.apache.spark.sql.hbase.util.{DataTypeUtils, HBaseKVHelper, BytesUtils}

/**
 * HBaseMainTest
 * create HbTestTable and metadata table, and insert some data
 */
object HBaseMainTest extends HBaseIntegrationTestBase(true) with CreateTableAndLoadData
with Logging {
  @transient val logger = Logger.getLogger(getClass.getName)

  val TableName_a: String = "ta"
  val TableName_b: String = "tb"
  val HbaseTableName: String = "ht"
  val Metadata_Table = "metadata"

  def createTable(useMultiplePartitions: Boolean) = {
    try {
      // delete the existing hbase table
      if (hbaseAdmin.tableExists(HbaseTableName)) {
        hbaseAdmin.disableTable(HbaseTableName)
        hbaseAdmin.deleteTable(HbaseTableName)
      }

      if (hbaseAdmin.tableExists(Metadata_Table)) {
        hbaseAdmin.disableTable(Metadata_Table)
        hbaseAdmin.deleteTable(Metadata_Table)
      }

      var allColumns = List[AbstractColumn]()
      allColumns = allColumns :+ KeyColumn("col1", StringType, 1)
      allColumns = allColumns :+ NonKeyColumn("col2", ByteType, "cf1", "cq11")
      allColumns = allColumns :+ KeyColumn("col3", ShortType, 2)
      allColumns = allColumns :+ NonKeyColumn("col4", IntegerType, "cf1", "cq12")
      allColumns = allColumns :+ NonKeyColumn("col5", LongType, "cf2", "cq21")
      allColumns = allColumns :+ NonKeyColumn("col6", FloatType, "cf2", "cq22")
      allColumns = allColumns :+ KeyColumn("col7", IntegerType, 0)

      val splitKeys: Array[Array[Byte]] = if (useMultiplePartitions) {
        Array(
          new GenericRow(Array(256, " p256 ", 128: Short)),
          new GenericRow(Array(32, " p32 ", 256: Short)),
          new GenericRow(Array(-32, " n32 ", 128: Short)),
          new GenericRow(Array(-256, " n256 ", 256: Short)),
          new GenericRow(Array(-128, " n128 ", 128: Short)),
          new GenericRow(Array(0, " zero ", 256: Short)),
          new GenericRow(Array(128, " p128 ", 512: Short))
        ).map(HBaseKVHelper.makeRowKey(_, Seq(IntegerType, StringType, ShortType)))
      } else {
        null
      }

      catalog = new HBaseCatalog(hbc)
      catalog.createTable(TableName_a, null, HbaseTableName, allColumns, splitKeys)

      hbc.sql( s"""CREATE TABLE $TableName_b(col1 STRING, col2 BYTE, col3 SHORT, col4 INTEGER,
          col5 LONG, col6 FLOAT, col7 INTEGER, PRIMARY KEY(col7, col1, col3))
          MAPPED BY ($HbaseTableName, COLS=[col2=cf1.cq11, col4=cf1.cq12, col5=cf2.cq21,
          col6=cf2.cq22])""".stripMargin)

      if (!hbaseAdmin.tableExists(HbaseTableName)) {
        throw new IllegalArgumentException("where is our table?")
      }
    }
  }

  def checkHBaseTableExists(hbaseTable: String): Boolean = {
    val tableName = TableName.valueOf(hbaseTable)
    hbaseAdmin.tableExists(tableName)
  }

  def insertTestData() = {
    if (!checkHBaseTableExists(HbaseTableName)) {
      throw new IllegalStateException(s"Unable to find table $HbaseTableName")
    }

    val htable = new HTable(config, HbaseTableName)

    def putNewTableIntoHBase(keys: Seq[Any], keysType: Seq[DataType],
                             vals: Seq[Any], valsType: Seq[DataType]): Unit = {
      val row = new GenericRow(keys.toArray)
      val key = makeRowKey(row, keysType)
      val put = new Put(key)
      Seq((vals(0), valsType(0), "cf1", "cq11"),
        (vals(1), valsType(1), "cf1", "cq12"),
        (vals(2), valsType(2), "cf2", "cq21"),
        (vals(3), valsType(3), "cf2", "cq22")).foreach {
        case (rowValue, rowType, colFamily, colQualifier) =>
          addRowVals(put, rowValue, rowType, colFamily, colQualifier)
      }
      htable.put(put)
    }

    putNewTableIntoHBase(Seq(-257, " n257 ", 128: Short),
      Seq(IntegerType, StringType, ShortType),
      Seq[Any](1.toByte, -2048, 12345678901234L, 1234.5678F),
      Seq(ByteType, IntegerType, LongType, FloatType))

    putNewTableIntoHBase(Seq(-255, " n255 ", 128: Short),
      Seq(IntegerType, StringType, ShortType),
      Seq[Any](2.toByte, -1024, 12345678901234L, 1234.5678F),
      Seq(ByteType, IntegerType, LongType, FloatType))

    putNewTableIntoHBase(Seq(-129, " n129 ", 128: Short),
      Seq(IntegerType, StringType, ShortType),
      Seq[Any](3.toByte, -512, 12345678901234L, 1234.5678F),
      Seq(ByteType, IntegerType, LongType, FloatType))

    putNewTableIntoHBase(Seq(-127, " n127 ", 128: Short),
      Seq(IntegerType, StringType, ShortType),
      Seq[Any](4.toByte, -256, 12345678901234L, 1234.5678F),
      Seq(ByteType, IntegerType, LongType, FloatType))

    putNewTableIntoHBase(Seq(-33, " n33 ", 128: Short),
      Seq(IntegerType, StringType, ShortType),
      Seq[Any](5.toByte, -128, 12345678901234L, 1234.5678F),
      Seq(ByteType, IntegerType, LongType, FloatType))

    putNewTableIntoHBase(Seq(-31, " n31 ", 128: Short),
      Seq(IntegerType, StringType, ShortType),
      Seq[Any](6.toByte, -64, 12345678901234L, 1234.5678F),
      Seq(ByteType, IntegerType, LongType, FloatType))

    putNewTableIntoHBase(Seq(-1, " n1 ", 128: Short),
      Seq(IntegerType, StringType, ShortType),
      Seq[Any](7.toByte, -1, 12345678901234L, 1234.5678F),
      Seq(ByteType, IntegerType, LongType, FloatType))

    putNewTableIntoHBase(Seq(1, " p1 ", 128: Short),
      Seq(IntegerType, StringType, ShortType),
      Seq[Any](8.toByte, 1, 12345678901234L, 1234.5678F),
      Seq(ByteType, IntegerType, LongType, FloatType))

    putNewTableIntoHBase(Seq(31, " p31 ", 128: Short),
      Seq(IntegerType, StringType, ShortType),
      Seq[Any](9.toByte, 4, 12345678901234L, 1234.5678F),
      Seq(ByteType, IntegerType, LongType, FloatType))

    putNewTableIntoHBase(Seq(33, " p33 ", 128: Short),
      Seq(IntegerType, StringType, ShortType),
      Seq[Any](10.toByte, 64, 12345678901234L, 1234.5678F),
      Seq(ByteType, IntegerType, LongType, FloatType))

    putNewTableIntoHBase(Seq(127, " p127 ", 128: Short),
      Seq(IntegerType, StringType, ShortType),
      Seq[Any](11.toByte, 128, 12345678901234L, 1234.5678F),
      Seq(ByteType, IntegerType, LongType, FloatType))

    putNewTableIntoHBase(Seq(129, " p129 ", 128: Short),
      Seq(IntegerType, StringType, ShortType),
      Seq[Any](12.toByte, 256, 12345678901234L, 1234.5678F),
      Seq(ByteType, IntegerType, LongType, FloatType))

    putNewTableIntoHBase(Seq(255, " p255 ", 128: Short),
      Seq(IntegerType, StringType, ShortType),
      Seq[Any](13.toByte, 512, 12345678901234L, 1234.5678F),
      Seq(ByteType, IntegerType, LongType, FloatType))

    putNewTableIntoHBase(Seq(257, " p257 ", 128: Short),
      Seq(IntegerType, StringType, ShortType),
      Seq[Any](14.toByte, 1024, 12345678901234L, 1234.5678F),
      Seq(ByteType, IntegerType, LongType, FloatType))

    htable.close()
  }

  def printResults(msg: String, results: SchemaRDD) =
    results match {
      case rdd: TestingSchemaRDD =>
        val data = rdd.collectPartitions()
        println(s"For test [$msg]: Received data length=${data(0).length}: ${
          data(0).mkString("RDD results: {", "],[", "}")
        }")
      case _ =>
        val data = results.collect()
        println(s"For test [$msg]: Received data length=${data.length}: ${
          data.mkString("RDD results: {", "],[", "}")
        }")
    }

  def makeRowKey(row: Row, dataTypeOfKeys: Seq[DataType]) = {
    val rawKeyCol = dataTypeOfKeys.zipWithIndex.map {
      case (dataType, index) =>
        (DataTypeUtils.getRowColumnInHBaseRawType(row, index, dataType),
          dataType)
    }

    HBaseKVHelper.encodingRawKeyColumns(rawKeyCol)
  }

  def addRowVals(put: Put, rowValue: Any, rowType: DataType,
                 colFamily: String, colQulifier: String) = {
    val bos = new ByteArrayOutputStream()
    val dos = new DataOutputStream(bos)
    val bu = BytesUtils.create(rowType)
    rowType match {
      case StringType => dos.write(bu.toBytes(rowValue.asInstanceOf[String]))
      case IntegerType => dos.write(bu.toBytes(rowValue.asInstanceOf[Int]))
      case BooleanType => dos.write(bu.toBytes(rowValue.asInstanceOf[Boolean]))
      case ByteType => dos.write(bu.toBytes(rowValue.asInstanceOf[Byte]))
      case DoubleType => dos.write(bu.toBytes(rowValue.asInstanceOf[Double]))
      case FloatType => dos.write(bu.toBytes(rowValue.asInstanceOf[Float]))
      case LongType => dos.write(bu.toBytes(rowValue.asInstanceOf[Long]))
      case ShortType => dos.write(bu.toBytes(rowValue.asInstanceOf[Short]))
      case _ => throw new Exception("Unsupported HBase SQL Data Type")
    }
    put.add(Bytes.toBytes(colFamily), Bytes.toBytes(colQulifier), bos.toByteArray)
  }

  def testHBaseScanner() = {
    val scan = new Scan
    val htable = new HTable(config, HbaseTableName)
    val scanner = htable.getScanner(scan)
    var res: Result = null
    do {
      res = scanner.next
      if (res != null) println(s"Row ${res.getRow} has map=${res.getNoVersionMap.toString}")
    } while (res != null)
  }

  def setupData(useMultiplePartitions: Boolean, needInsertData: Boolean = false) {
    if (needInsertData) {
      createTable(useMultiplePartitions)
      insertTestData()
    }
  }

  def main(args: Array[String]) = {
    setupData(useMultiplePartitions = true, true)
  }
}
