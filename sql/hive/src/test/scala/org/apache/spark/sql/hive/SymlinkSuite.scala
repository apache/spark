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

import java.io.{FileWriter, PrintWriter}

import org.apache.spark.sql.{DataFrame, QueryTest}
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.test.SQLTestUtils


class SymlinkSuite extends QueryTest with TestHiveSingleton with SQLTestUtils {

  import spark.implicits._

  private val tuples: Seq[(String, Double, Int)] = Seq(
    ("Spark", 3.2, 1)
    , ("Hive", 2.3, 3)
    , ("Trino", 371, 2)
  )
  lazy val answerNoPartition: DataFrame = tuples.toDF("name", "version", "sort")

  private val tuplesWithPartition: Seq[(String, Double, Int, String)] = Seq(
    ("Spark", 3.2, 1, "2022-02-21")
    , ("Hive", 2.3, 3, "2022-02-19")
    , ("Trino", 371, 2, "2022-02-20")
  )
  lazy val answerWithPartition: DataFrame =
    tuplesWithPartition.toDF("name", "version", "sort", "pt")

  val schemaDDL = "name STRING, version DOUBLE, sort INT"
  val partitionSchemaDDL = "pt STRING"

  lazy val userDir: String = System.getProperty("user.dir")
  lazy val csvFiles = Seq(
    s"$userDir/src/test/resources/data/files/symlink_table/csv-part-1.csv",
    s"$userDir/src/test/resources/data/files/symlink_table/csv-part-2.csv")

  lazy val jsonFiles = Seq(
    s"$userDir/src/test/resources/data/files/symlink_table/json-part-1.json",
    s"$userDir/src/test/resources/data/files/symlink_table/json-part-2.json")

  lazy val orcFiles = Seq(
    s"$userDir/src/test/resources/data/files/symlink_table/orc-part-1.snappy.orc",
    s"$userDir/src/test/resources/data/files/symlink_table/orc-part-2.snappy.orc")

  lazy val parquetFiles = Seq(
    s"$userDir/src/test/resources/data/files/symlink_table/parquet-part-1.snappy.parquet",
    s"$userDir/src/test/resources/data/files/symlink_table/parquet-part-2.snappy.parquet")

  def writeContentToPath(content: String, manifestPath: String): Unit = {
    val writer = new PrintWriter(new FileWriter(manifestPath, false))
    writer.write(content)
    writer.close()
  }

  def transformFilePath(files: Seq[String]): String = {
    files.map(file => s"file://$file").mkString("\n") + "\n"
  }

  case class SymlinkTable(tableName: String, rowFormat: String, files: Seq[String], ddl: String)

  private val orcSymlinkTable: SymlinkTable =
    SymlinkTable("symlink_orc",
      "SERDE 'org.apache.hadoop.hive.ql.io.orc.OrcSerde'", orcFiles, schemaDDL)
  private val parquetSymlinkTable: SymlinkTable = SymlinkTable("symlink_parquet"
    , "SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'"
    , parquetFiles, schemaDDL)
  private val csvSymlinkTable: SymlinkTable = SymlinkTable("symlink_csv"
    , "DELIMITED FIELDS TERMINATED BY ','", csvFiles, schemaDDL)

  test("query symlink table with orc format") {
    checkSymlinkTable(orcSymlinkTable)
  }

  test("query symlink table with paruqet format") {
    checkSymlinkTable(parquetSymlinkTable)
  }

  // csv/json format not work at Spark 3.2, don't known why
  //  test("query symlink table with csv format") {
  //    checkSymlinkTable(csvSymlinkTable)
  //  }
  //
  //  test("query symlink table with json format") {
  //    checkSymlinkTable(jsonSymlinkTable)
  //  }

  def checkSymlinkTable(symlinkTable: SymlinkTable, isPartition: Boolean = false): Unit = {
    val tableName = symlinkTable.tableName
    val schemaDDL = symlinkTable.ddl
    withTempDir { temp =>
      withTable(tableName) {
        // generate manifest for symlink table
        if (isPartition) {

        } else {
          val manifest = s"${temp.getCanonicalPath}/manifest"
          writeContentToPath(transformFilePath(symlinkTable.files),
            manifest)
        }

        // drop table if exists
        spark.sql(s"DROP TABLE IF EXISTS $tableName")

        // create external table
        spark.sql(
          s"""CREATE EXTERNAL TABLE $tableName ( $schemaDDL )
             |ROW FORMAT ${symlinkTable.rowFormat}
             |STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat'
             |OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
             |LOCATION 'file://${temp.getCanonicalPath}'
             |""".stripMargin
        )

        // read table and compared result
        val df = spark.sql(s"select * from $tableName")
        df.printSchema()
        df.show()
        checkAnswer(df, answerNoPartition)
      }
    }
  }
}
