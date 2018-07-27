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

package org.apache.spark.sql.hive.execution

import java.io.File
import java.net.URI

import org.scalatest.BeforeAndAfterEach
import org.scalatest.Matchers

import org.apache.spark.sql.{DataFrame, QueryTest, Row}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogTableType
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.command.DDLUtils
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SQLTestUtils

class MultiFormatTableSuite
  extends QueryTest with SQLTestUtils with TestHiveSingleton with BeforeAndAfterEach with Matchers {
  import testImplicits._

  val parser = new SparkSqlParser(new SQLConf())

  override def afterEach(): Unit = {
    try {
      // drop all databases, tables and functions after each test
      spark.sessionState.catalog.reset()
    } finally {
      super.afterEach()
    }
  }

  val partitionCol = "dt"
  val partitionVal1 = "2018-01-26"
  val partitionVal2 = "2018-01-27"
  private case class PartitionDefinition(
                                          column: String,
                                          value: String,
                                          location: URI,
                                          format: Option[String] = None
                                        ) {
    def toSpec: String = {
      s"($column='$value')"
    }
    def toSpecAsMap: Map[String, String] = {
      Map(column -> value)
    }
  }

  test("create hive table with multi format partitions") {
    val catalog = spark.sessionState.catalog
    withTempDir { baseDir =>

      val partitionedTable = "ext_multiformat_partition_table"
      withTable(partitionedTable) {
        assert(baseDir.listFiles.isEmpty)

        val partitions = createMultiformatPartitionDefinitions(baseDir)

        createTableWithPartitions(partitionedTable, baseDir, partitions)

        // Check table storage type is PARQUET
        val hiveResultTable =
          catalog.getTableMetadata(TableIdentifier(partitionedTable, Some("default")))
        assert(DDLUtils.isHiveTable(hiveResultTable))
        assert(hiveResultTable.tableType == CatalogTableType.EXTERNAL)
        assert(hiveResultTable.storage.inputFormat
          .contains("org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat")
        )
        assert(hiveResultTable.storage.outputFormat
          .contains("org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat")
        )
        assert(hiveResultTable.storage.serde
          .contains("org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe")
        )

        // Check table has correct partititons
        assert(
          catalog.listPartitions(TableIdentifier(partitionedTable,
            Some("default"))).map(_.spec).toSet == partitions.map(_.toSpecAsMap).toSet
        )

        // Check first table partition storage type is PARQUET
        val parquetPartition = catalog.getPartition(
          TableIdentifier(partitionedTable, Some("default")),
          partitions.head.toSpecAsMap
        )
        assert(
          parquetPartition.storage.serde
            .contains("org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe")
        )

        // Check second table partition storage type is AVRO
        val avroPartition = catalog.getPartition(
          TableIdentifier(partitionedTable, Some("default")),
          partitions.last.toSpecAsMap
        )
        assert(
          avroPartition.storage.serde.contains("org.apache.hadoop.hive.serde2.avro.AvroSerDe")
        )

        assert(
          avroPartition.storage.inputFormat
            .contains("org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat")
        )
      }
    }
  }


  private def createMultiformatPartitionDefinitions(baseDir: File): List[PartitionDefinition] = {
    val basePath = baseDir.getCanonicalPath
    val partitionPath_part1 = new File(basePath + s"/$partitionCol=$partitionVal1")
    val partitionPath_part2 = new File(basePath + s"/$partitionCol=$partitionVal2")

    List(
      PartitionDefinition(
        partitionCol, partitionVal1, partitionPath_part1.toURI, format = Some("PARQUET")
      ),
      PartitionDefinition(
        partitionCol, partitionVal2, partitionPath_part2.toURI, format = Some("AVRO")
      )
    )
  }

  private def createParquetPartitionDefinitions(baseDir: File): List[PartitionDefinition] = {
    val basePath = baseDir.getCanonicalPath
    val partitionPath_part1 = new File(basePath + s"/$partitionCol=$partitionVal1")
    val partitionPath_part2 = new File(basePath + s"/$partitionCol=$partitionVal2")

    List(
      PartitionDefinition(
        partitionCol, partitionVal1, partitionPath_part1.toURI, format = Some("PARQUET")
      ),
      PartitionDefinition(
        partitionCol, partitionVal2, partitionPath_part2.toURI, format = Some("PARQUET")
      )
    )
  }

  private def createTableWithPartitions(table: String,
                                        baseDir: File,
                                        partitions: List[PartitionDefinition],
                                        avro: Boolean = false
                                       ): Unit = {
    if (avro) {
      createAvroExternalTable(table, baseDir.toURI)
    } else {
      createParquetExternalTable(table, baseDir.toURI)
    }
    addPartitions(table, partitions)
    partitions.foreach(p => setPartitionFormat(table, p))
  }

  private def createAvroCheckTable(avroTable: String, partition: PartitionDefinition): Unit = {
    // The only valid way to insert avro data into the avro partition
    // is to create a new avro table directly on the location of the avro partition
    val avroSchema =
    """{
      |  "name": "baseRecord",
      |  "type": "record",
      |  "fields": [{
      |    "name": "key",
      |    "type": ["null", "int"],
      |    "default": null
      |  },
      |  {
      |    "name": "value",
      |    "type": ["null", "string"],
      |    "default": null
      |  }]
      |}
    """.stripMargin

    // Creates the Avro table
    sql(
      s"""
         |CREATE TABLE $avroTable
         |ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
         |STORED AS
         |  INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
         |  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
         |LOCATION '${partition.location}'
         |TBLPROPERTIES ('avro.schema.literal' = '$avroSchema')
            """.stripMargin
    )
  }

  private def createPqCheckTable(pqTable: String, partition: PartitionDefinition): Unit = {

    // Creates the Parquet table
    sql(
      s"""
         |CREATE TABLE $pqTable (key INT, value STRING)
         |STORED AS PARQUET
         |LOCATION '${partition.location}'
            """.stripMargin
    )
  }

  private def createParquetExternalTable(table: String, location: URI): DataFrame = {
    sql(
      s"""
         |CREATE EXTERNAL TABLE $table (key INT, value STRING)
         |PARTITIONED BY (dt STRING)
         |STORED AS PARQUET
         |LOCATION '$location'
      """.stripMargin
    )
  }

  private def createAvroExternalTable(table: String, location: URI): DataFrame = {
    val avroSchema =
      """{
        |  "name": "baseRecord",
        |  "type": "record",
        |  "fields": [{
        |    "name": "key",
        |    "type": ["null", "int"],
        |    "default": null
        |  },
        |  {
        |    "name": "value",
        |    "type": ["null", "string"],
        |    "default": null
        |  }]
        |}
      """.stripMargin
    sql(
      s"""
         |CREATE EXTERNAL TABLE $table (key INT, value STRING)
         |PARTITIONED BY (dt STRING)
         |STORED AS
         |  INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
         |  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
         |LOCATION '$location'
         |TBLPROPERTIES ('avro.schema.literal' = '$avroSchema')
      """.stripMargin
    )
  }

  private def addPartitions(table: String, partitionDefs: List[PartitionDefinition]): DataFrame = {
    val partitions = partitionDefs
      .map(definition => s"PARTITION ${definition.toSpec} LOCATION '${definition.location}'")
      .mkString("\n")

    sql(
      s"""
         |ALTER TABLE $table ADD
         |$partitions
      """.stripMargin
    )

  }

  private def setPartitionFormat(
                                  table: String,
                                  partitionDef: PartitionDefinition
                                ): DataFrame = {
    sql(
      s"""
         |ALTER TABLE $table
         |PARTITION ${partitionDef.toSpec} SET FILEFORMAT ${partitionDef.format.get}
      """.stripMargin
    )
  }
}
