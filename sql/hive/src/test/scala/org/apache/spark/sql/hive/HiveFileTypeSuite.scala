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

import org.apache.logging.log4j.Level

import org.apache.spark.SparkException
import org.apache.spark.sql.{AnalysisException, QueryTest, Row}
import org.apache.spark.sql.connector.catalog.CatalogManager.SESSION_CATALOG_NAME
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.types.{ArrayType, DataType, FileRef, FileType, IntegerType, StructType}

/**
 * Tests for FILE in the Hive catalog: a data source Parquet table stores FILE, and a Hive serde
 * table rejects it, as Hive has no FILE type.
 */
class HiveFileTypeSuite extends QueryTest with TestHiveSingleton {

  private val file = new FileRef("s3://bucket/a.pdf", null, 10L, "application/pdf", null)

  private def withSource(f: => Unit): Unit = {
    val schema = new StructType()
      .add("id", IntegerType)
      .add("c", FileType.external)
      .add("s", new StructType().add("g", FileType.external))
    spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(Row(1, file, Row(file)), Row(2, null, null)), 1), schema)
      .createOrReplaceTempView("src")
    try f finally spark.catalog.dropTempView("src")
  }

  private def checkRejected(columnName: String, dataType: DataType)(f: => Unit): Unit = {
    checkError(
      exception = intercept[AnalysisException](f),
      condition = "UNSUPPORTED_DATA_TYPE_FOR_DATASOURCE",
      parameters = Map(
        "columnName" -> s"`$columnName`",
        "columnType" -> s""""${dataType.sql}"""",
        "format" -> "Hive"))
  }

  test("a data source Parquet table in the Hive catalog stores FILE") {
    withSource {
      withTable("t") {
        val logAppender = new LogAppender("FILE is reported as Hive incompatible")
        logAppender.setThreshold(Level.WARN)
        withLogAppender(logAppender) {
          sql("CREATE TABLE t (id INT, c FILE EXTERNAL, s STRUCT<g: FILE EXTERNAL>) USING parquet")
        }
        // The table is not offered to the Hive metastore in a Hive compatible format first.
        val incompatible = Seq(FileType.external, new StructType().add("g", FileType.external))
          .map(_.simpleString).mkString(", ")
        val expectedMsg = s"Hive incompatible types found: $incompatible. " +
          s"Persisting data source table `$SESSION_CATALOG_NAME`.`default`.`t` into Hive " +
          "metastore in Spark SQL specific format, which is NOT compatible with Hive."
        assert(logAppender.loggingEvents.map(_.getMessage.getFormattedMessage)
          .contains(expectedMsg))
        sql("INSERT INTO t SELECT * FROM src")
        val expected = spark.table("src").schema
        assert(spark.table("t").schema === expected)
        assert(spark.sessionState.catalog.externalCatalog.getTable("default", "t").schema ===
          expected)
        checkAnswer(spark.table("t"), Seq(Row(1, file, Row(file)), Row(2, null, null)))
        // Hive has no FILE type, so the table is stored in the Spark SQL specific format.
        val rawTable = spark.sharedState.externalCatalog.unwrapped
          .asInstanceOf[HiveExternalCatalog].client.getTable("default", "t")
        assert(!rawTable.schema.exists(_.dataType.existsRecursively(_.isInstanceOf[FileType])))
      }
    }
  }

  test("a Hive serde table rejects a FILE column") {
    withSource {
      Seq("TEXTFILE", "PARQUET", "ORC").foreach { format =>
        withTable("t") {
          withClue(s"$format: ") {
            checkRejected("c", FileType.external) {
              sql(s"CREATE TABLE t (id INT, c FILE EXTERNAL) STORED AS $format")
            }
            checkRejected("c", ArrayType(FileType.external)) {
              sql(s"CREATE TABLE t (id INT, c ARRAY<FILE EXTERNAL>) STORED AS $format")
            }
            checkRejected("c", FileType.external) {
              sql(s"CREATE TABLE t STORED AS $format AS SELECT id, c FROM src")
            }
          }
        }
      }
    }
  }

  test("a write to a Hive serde directory rejects a FILE column") {
    // Without the conversion to the Parquet and ORC data sources, the write goes through the Hive
    // serde (HiveFileFormat).
    withSQLConf(HiveUtils.CONVERT_METASTORE_INSERT_DIR.key -> "false") {
      withSource {
        Seq("PARQUET", "ORC").foreach { format =>
          withTempDir { dir =>
            withClue(s"$format: ") {
              // InsertIntoHiveDirCommand wraps the failure in a SparkException.
              val e = intercept[SparkException] {
                sql(
                  s"""
                     |INSERT OVERWRITE LOCAL DIRECTORY '${dir.toURI.getPath}'
                     |STORED AS $format
                     |SELECT c FROM src
                   """.stripMargin)
              }
              checkRejected("c", FileType.external) {
                throw e.getCause
              }
            }
          }
        }
      }
    }
  }
}
