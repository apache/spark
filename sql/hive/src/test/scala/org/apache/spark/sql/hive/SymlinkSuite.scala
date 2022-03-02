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

import java.io.{File, FileWriter, PrintWriter}

import org.apache.spark.internal.config
import org.apache.spark.sql.{DataFrame, QueryTest}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.execution.command.{AlterTableAddPartitionCommand, AnalyzeTableCommand}
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.test.SQLTestUtils
import org.apache.spark.sql.types.DataTypes


class SymlinkSuite extends QueryTest with TestHiveSingleton with SQLTestUtils {

  import spark.implicits._

  // SymlinkTextInputFormat table will get file splits size is empty
  // see SymlinkTextInputSplit.SymlinkTextInputSplit at hive 2.3
  spark.sparkContext.conf.set(config.HADOOP_RDD_IGNORE_EMPTY_SPLITS.key, "false")

  private val tuples: Seq[(String, Double, Int)] = Seq(
    ("Spark", 3.2, 1)
    , ("Hive", 2.3, 3)
    , ("Trino", 371, 2)
  )
  lazy val answerNoPartition: DataFrame = tuples.toDF("name", "version", "sort")

  private val tuplesWithPartition: Seq[(String, Double, Int, String)] = Seq(
    ("Spark", 3.2, 1, "2022-02-20")
    , ("Hive", 2.3, 3, "2022-02-19")
    , ("Trino", 371, 2, "2022-02-19")
  )
  lazy val answerWithPartition: DataFrame =
    tuplesWithPartition.toDF("name", "version", "sort", "pt")

  val schemaDDL = "name STRING, version DOUBLE, sort INT"
  val partitionSchemaDDL = "pt STRING"

  lazy val userDir: String = System.getProperty("user.dir")
  lazy val csvFiles = Seq(
    s"$userDir/src/test/resources/data/files/symlink_table/csv-part-1.csv",
    s"$userDir/src/test/resources/data/files/symlink_table/csv-part-2.csv")

  lazy val orcFiles = Seq(
    s"$userDir/src/test/resources/data/files/symlink_table/orc-part-1.snappy.orc",
    s"$userDir/src/test/resources/data/files/symlink_table/orc-part-2.snappy.orc")

  lazy val parquetFiles = Seq(
    s"$userDir/src/test/resources/data/files/symlink_table/parquet-part-1.snappy.parquet",
    s"$userDir/src/test/resources/data/files/symlink_table/parquet-part-2.snappy.parquet")

  def writeContentToPath(content: String, manifestPath: String): Unit = {
    new File(manifestPath).getParentFile.mkdir()
    val writer = new PrintWriter(new FileWriter(manifestPath, false))
    writer.write(content)
    writer.close()
  }

  def transformFilePath(files: Seq[String]): String = {
    files.map(file => s"file://$file").mkString("\n") + "\n"
  }

  case class SymlinkTable(tableName: String, rowFormat: String, files: Seq[String], ddl: String
                          , size: Long)

  private val orcSymlinkTable: SymlinkTable =
    SymlinkTable("symlink_orc",
      "SERDE 'org.apache.hadoop.hive.ql.io.orc.OrcSerde'", orcFiles, schemaDDL, 1052)
  private val parquetSymlinkTable: SymlinkTable = SymlinkTable("symlink_parquet"
    , "SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'"
    , parquetFiles, schemaDDL, 1900)
  private val csvSymlinkTable: SymlinkTable = SymlinkTable("symlink_csv"
    , "SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'"
    , csvFiles, schemaDDL, 35)

  // query symlink table with csv format and check answer
  test("[csv][symlink table][data]query check") {
    createSymlinkTable(csvSymlinkTable)(checkTableAnswer)
  }

  // analyze symlink table with csv format and check size in bytes
  test("[csv][symlink table][size in bytes]analyze check") {
    createSymlinkTable(csvSymlinkTable)(checkTableSize)
  }

  // query symlink partition table with orc format and check answer
  test("[csv][symlink table][partition][data]query check") {
    createSymlinkTable(csvSymlinkTable, isPartition = true)(checkTableAnswer)
  }

  // analyze symlink partition table with orc format and check size in bytes
  test("[csv][symlink table][partition][size in bytes]analyze check") {
    createSymlinkTable(csvSymlinkTable, isPartition = true)(checkTableSize)
  }

  // query symlink table with orc format and check answer
  test("[orc][symlink table][data]query check") {
    createSymlinkTable(orcSymlinkTable)(checkTableAnswer)
  }

  // analyze symlink table with orc format and check size in bytes
  test("[orc][symlink table][size in bytes]analyze check") {
    createSymlinkTable(orcSymlinkTable)(checkTableSize)
  }


  // query symlink partition table with orc format and check answer
  test("[orc][symlink table][partition][data]query check") {
    createSymlinkTable(orcSymlinkTable, isPartition = true)(checkTableAnswer)
  }

  // analyze symlink partition table with orc format and check size in bytes
  test("[orc][symlink table][partition][size in bytes]analyze check") {
    createSymlinkTable(orcSymlinkTable, isPartition = true)(checkTableSize)
  }


  // query symlink table with parquet format and check answer
  test("[parquet][symlink table][data]query check") {
    createSymlinkTable(parquetSymlinkTable)(checkTableAnswer)
  }

  // analyze symlink table with parquet format and check size in bytes
  test("[parquet][symlink table][size in bytes]analyze check") {
    createSymlinkTable(parquetSymlinkTable)(checkTableSize)
  }


  // query parquet partition table with orc format and check answer
  test("[parquet][symlink partition table][data]query check") {
    createSymlinkTable(parquetSymlinkTable, isPartition = true)(checkTableAnswer)
  }

  // analyze parquet partition table with orc format and check size in bytes
  test("[parquet][symlink partition table][size in bytes]analyze check") {
    createSymlinkTable(parquetSymlinkTable, isPartition = true)(checkTableSize)
  }

  def createSymlinkTable(symlinkTable: SymlinkTable, isPartition: Boolean = false)
                        (fuc: (SymlinkTable, Boolean) => Unit): Unit = {
    val tableName = symlinkTable.tableName
    val schemaDDL = symlinkTable.ddl
    withTempDir { temp =>
      withTable(tableName) {
        // generate manifest for symlink table
        val partitionDDL = if (isPartition) {
          val manifest1 = s"${temp.getCanonicalPath}/pt=2022-02-20/manifest"
          writeContentToPath(transformFilePath(symlinkTable.files.headOption.toSeq)
            , manifest1)
          val manifest2 = s"${temp.getCanonicalPath}/pt=2022-02-19/manifest"
          writeContentToPath(transformFilePath(symlinkTable.files.lastOption.toSeq)
            , manifest2)
          s"PARTITIONED BY ($partitionSchemaDDL)"
        } else {
          val manifest = s"${temp.getCanonicalPath}/manifest"
          writeContentToPath(transformFilePath(symlinkTable.files),
            manifest)
          ""
        }

        // drop table if exists
        spark.sql(s"DROP TABLE IF EXISTS $tableName")

        // create external table
        spark.sql(
          s"""CREATE EXTERNAL TABLE $tableName ( $schemaDDL )
             |$partitionDDL
             |ROW FORMAT ${symlinkTable.rowFormat}
             |STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat'
             |OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
             |LOCATION 'file://${temp.getCanonicalPath}'
             |""".stripMargin
        )

        // if this is partition, we should add partition for test
        if (isPartition) {
          AlterTableAddPartitionCommand(TableIdentifier(tableName),
            Seq(
              (Map("pt" -> "2022-02-20"), None),
              (Map("pt" -> "2022-02-19"), None)
            ),
            ifNotExists = true).run(spark)
        }

        fuc(symlinkTable, isPartition)
      }
    }
  }

  def castDF(df: DataFrame): DataFrame = {
    df.withColumn("version", 'version.cast(DataTypes.DoubleType))
      .withColumn("sort", 'sort.cast(DataTypes.IntegerType))
  }

  def checkTableAnswer(symlinkTable: SymlinkTable, isPartition: Boolean = false): Unit = {
    // read table and compared result
    val tableName = symlinkTable.tableName
    var df = spark.sql(s"select * from $tableName")
    df.printSchema()
    df.show()
    // spark will transform all columns to string when using OpenCSVSerde for row format
    if (symlinkTable.rowFormat.contains("OpenCSVSerde")) {
      df = castDF(df)
    }
    if (isPartition) {
      checkAnswer(df, answerWithPartition)
    } else {
      checkAnswer(df, answerNoPartition)
    }
  }

  def checkTableSize(symlinkTable: SymlinkTable, isPartition: Boolean = false): Unit = {
    val tableName = symlinkTable.tableName
    analyzeTable(tableName)
    // scalastyle:off println
    val stats = getCatalogTable(tableName).stats
    println(tableName, stats)
    assert(stats.get.sizeInBytes == symlinkTable.size)
  }


  def analyzeTable(tbl: String, db: Option[String] = Option.empty, noscan: Boolean = true): Unit = {
    AnalyzeTableCommand(TableIdentifier(tbl, db), noscan)
      .run(spark)
  }

  def getCatalogTable(tableName: String): CatalogTable = {
    spark.sessionState.catalog.getTableMetadata(TableIdentifier(tableName))
  }
}
