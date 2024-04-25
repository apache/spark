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

package org.apache.spark.sql.hive.orc

import java.io.File

import com.google.common.io.Files
import org.apache.hadoop.fs.Path
import org.apache.orc.OrcConf

import org.apache.spark.sql.{AnalysisException, Row}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.HiveTableRelation
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.execution.datasources.orc.OrcQueryTest
import org.apache.spark.sql.hive.{HiveSessionCatalog, HiveUtils}
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.internal.SQLConf

class HiveOrcQuerySuite extends OrcQueryTest with TestHiveSingleton {
  import testImplicits._

  override val orcImp: String = "hive"

  test("SPARK-8501: Avoids discovery schema from empty ORC files") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath

      withTable("empty_orc") {
        withTempView("empty", "single") {
          spark.sql(
            s"""CREATE TABLE empty_orc(key INT, value STRING)
               |STORED AS ORC
               |LOCATION '${dir.toURI}'
             """.stripMargin)

          val emptyDF = Seq.empty[(Int, String)].toDF("key", "value").coalesce(1)
          emptyDF.createOrReplaceTempView("empty")

          val zeroPath = new Path(path, "zero.orc")
          zeroPath.getFileSystem(spark.sessionState.newHadoopConf()).create(zeroPath)
          checkError(
            exception = intercept[AnalysisException] {
              spark.read.orc(path)
            },
            errorClass = "UNABLE_TO_INFER_SCHEMA",
            parameters = Map("format" -> "ORC")
          )

          val singleRowDF = Seq((0, "foo")).toDF("key", "value").coalesce(1)
          singleRowDF.createOrReplaceTempView("single")

          spark.sql(
            s"""INSERT INTO TABLE empty_orc
               |SELECT key, value FROM single
             """.stripMargin)

          val df = spark.read.orc(path)
          assert(df.schema === singleRowDF.schema.asNullable)
          checkAnswer(df, singleRowDF)
        }
      }
    }
  }

  test("Verify the ORC conversion parameter: CONVERT_METASTORE_ORC") {
    withTempView("single") {
      val singleRowDF = Seq((0, "foo")).toDF("key", "value")
      singleRowDF.createOrReplaceTempView("single")

      Seq("true", "false").foreach { orcConversion =>
        withSQLConf(HiveUtils.CONVERT_METASTORE_ORC.key -> orcConversion) {
          withTable("dummy_orc") {
            withTempPath { dir =>
              val path = dir.getCanonicalPath
              spark.sql(
                s"""
                   |CREATE TABLE dummy_orc(key INT, value STRING)
                   |STORED AS ORC
                   |LOCATION '${dir.toURI}'
                 """.stripMargin)

              spark.sql(
                s"""
                   |INSERT INTO TABLE dummy_orc
                   |SELECT key, value FROM single
                 """.stripMargin)

              val df = spark.sql("SELECT * FROM dummy_orc WHERE key=0")
              checkAnswer(df, singleRowDF)

              val queryExecution = df.queryExecution
              if (orcConversion == "true") {
                queryExecution.analyzed.collectFirst {
                  case _: LogicalRelation => ()
                }.getOrElse {
                  fail(s"Expecting the query plan to convert orc to data sources, " +
                    s"but got:\n$queryExecution")
                }
              } else {
                queryExecution.analyzed.collectFirst {
                  case _: HiveTableRelation => ()
                }.getOrElse {
                  fail(s"Expecting no conversion from orc to data sources, " +
                    s"but got:\n$queryExecution")
                }
              }
            }
          }
        }
      }
    }
  }

  test("converted ORC table supports resolving mixed case field") {
    withSQLConf(HiveUtils.CONVERT_METASTORE_ORC.key -> "true") {
      withTable("dummy_orc") {
        withTempPath { dir =>
          val df = spark.range(5).selectExpr("id", "id as valueField", "id as partitionValue")
          df.write
            .partitionBy("partitionValue")
            .mode("overwrite")
            .orc(dir.getAbsolutePath)

          spark.sql(s"""
            |create external table dummy_orc (id long, valueField long)
            |partitioned by (partitionValue int)
            |stored as orc
            |location "${dir.toURI}"""".stripMargin)
          spark.sql(s"msck repair table dummy_orc")
          checkAnswer(spark.sql("select * from dummy_orc"), df)
        }
      }
    }
  }

  test("SPARK-20728 Make ORCFileFormat configurable between sql/hive and sql/core") {
    Seq(
      ("native", classOf[org.apache.spark.sql.execution.datasources.orc.OrcFileFormat]),
      ("hive", classOf[org.apache.spark.sql.hive.orc.OrcFileFormat])).foreach {
      case (orcImpl, format) =>
        withSQLConf(SQLConf.ORC_IMPLEMENTATION.key -> orcImpl) {
          withTable("spark_20728") {
            sql("CREATE TABLE spark_20728(a INT) USING ORC")
            val fileFormat = sql("SELECT * FROM spark_20728").queryExecution.analyzed.collectFirst {
              case l: LogicalRelation =>
                l.relation.asInstanceOf[HadoopFsRelation].fileFormat.getClass
            }
            assert(fileFormat == Some(format))
          }
        }
    }
  }

  test("SPARK-22267 Spark SQL incorrectly reads ORC files when column order is different") {
    Seq("native", "hive").foreach { orcImpl =>
      withSQLConf(SQLConf.ORC_IMPLEMENTATION.key -> orcImpl) {
        withTempPath { f =>
          val path = f.getCanonicalPath
          Seq(1 -> 2).toDF("c1", "c2").write.orc(path)
          checkAnswer(spark.read.orc(path), Row(1, 2))

          Seq(true, false).foreach { convertMetastoreOrc =>
            withSQLConf(HiveUtils.CONVERT_METASTORE_ORC.key -> convertMetastoreOrc.toString) {
              withTable("t") {
                sql(s"CREATE EXTERNAL TABLE t(c2 INT, c1 INT) STORED AS ORC LOCATION '$path'")
                checkAnswer(spark.table("t"), Row(2, 1))
              }
            }
          }
        }
      }
    }
  }

  test("SPARK-19809 NullPointerException on zero-size ORC file") {
    Seq("native", "hive").foreach { orcImpl =>
      withSQLConf(SQLConf.ORC_IMPLEMENTATION.key -> orcImpl) {
        withTempPath { dir =>
          withTable("spark_19809") {
            sql(s"CREATE TABLE spark_19809(a int) STORED AS ORC LOCATION '$dir'")
            Files.touch(new File(s"${dir.getCanonicalPath}", "zero.orc"))

            Seq(true, false).foreach { convertMetastoreOrc =>
              withSQLConf(HiveUtils.CONVERT_METASTORE_ORC.key -> convertMetastoreOrc.toString) {
                checkAnswer(spark.table("spark_19809"), Seq.empty)
              }
            }
          }
        }
      }
    }
  }

  test("SPARK-23340 Empty float/double array columns raise EOFException") {
    withSQLConf(HiveUtils.CONVERT_METASTORE_ORC.key -> "false") {
      withTable("spark_23340") {
        sql("CREATE TABLE spark_23340(a array<float>, b array<double>) STORED AS ORC")
        sql("INSERT INTO spark_23340 VALUES (array(), array())")
        checkAnswer(spark.table("spark_23340"), Seq(Row(Array.empty[Float], Array.empty[Double])))
      }
    }
  }

  test("SPARK-26437 Can not query decimal type when value is 0") {
    withSQLConf(HiveUtils.CONVERT_METASTORE_ORC.key -> "false") {
      withTable("spark_26437") {
        sql("CREATE TABLE spark_26437 STORED AS ORCFILE AS SELECT 0.00 AS c1")
        checkAnswer(spark.table("spark_26437"), Seq(Row(0.00)))
      }
    }
  }

  private def getCachedDataSourceTable(table: TableIdentifier) = {
    spark.sessionState.catalog.asInstanceOf[HiveSessionCatalog].metastoreCatalog
      .getCachedDataSourceTable(table)
  }

  private def checkCached(tableIdentifier: TableIdentifier): Unit = {
    getCachedDataSourceTable(tableIdentifier) match {
      case null => fail(s"Converted ${tableIdentifier.table} should be cached in the cache.")
      case LogicalRelation(_: HadoopFsRelation, _, _, _) => // OK
      case other =>
        fail(
          s"The cached ${tableIdentifier.table} should be a HadoopFsRelation. " +
            s"However, $other is returned form the cache.")
    }
  }

  test("SPARK-28573 ORC conversation could be applied for partitioned table insertion") {
    withTempView("single") {
      val singleRowDF = Seq((0, "foo")).toDF("key", "value")
      singleRowDF.createOrReplaceTempView("single")
      Seq("true", "false").foreach { conversion =>
        withSQLConf(HiveUtils.CONVERT_METASTORE_ORC.key -> "true",
          HiveUtils.CONVERT_INSERTING_PARTITIONED_TABLE.key -> conversion) {
          withTable("dummy_orc_partitioned") {
            spark.sql(
              s"""
                 |CREATE TABLE dummy_orc_partitioned(key INT, value STRING)
                 |PARTITIONED by (`date` STRING)
                 |STORED AS ORC
                 """.stripMargin)

            spark.sql(
              s"""
                 |INSERT INTO TABLE dummy_orc_partitioned
                 |PARTITION (`date` = '2019-04-01')
                 |SELECT key, value FROM single
                 """.stripMargin)

            val orcPartitionedTable = TableIdentifier("dummy_orc_partitioned", Some("default"))
            if (conversion == "true") {
              // if converted, we refresh the cached relation.
              assert(getCachedDataSourceTable(orcPartitionedTable) === null)
            } else {
              // otherwise, not cached.
              assert(getCachedDataSourceTable(orcPartitionedTable) === null)
            }

            val df = spark.sql("SELECT key, value FROM dummy_orc_partitioned WHERE key=0")
            checkAnswer(df, singleRowDF)
          }
        }
      }
    }
  }

  test("SPARK-47850 ORC conversation could be applied for unpartitioned table insertion") {
    withTempView("single") {
      val singleRowDF = Seq((0, "foo")).toDF("key", "value")
      singleRowDF.createOrReplaceTempView("single")
      Seq("true", "false").foreach { conversion =>
        withSQLConf(HiveUtils.CONVERT_METASTORE_ORC.key -> "true",
          HiveUtils.CONVERT_INSERTING_UNPARTITIONED_TABLE.key -> conversion) {
          withTable("dummy_orc_unpartitioned") {
            spark.sql(
              s"""
                 |CREATE TABLE dummy_orc_unpartitioned(key INT, value STRING)
                 |STORED AS ORC
                 """.stripMargin)

            spark.sql(
              s"""
                 |INSERT INTO TABLE dummy_orc_unpartitioned
                 |SELECT key, value FROM single
                 """.stripMargin)

            val orcUnpartitionedTable = TableIdentifier("dummy_orc_unpartitioned", Some("default"))
            if (conversion == "true") {
              // if converted, we refresh the cached relation.
              assert(getCachedDataSourceTable(orcUnpartitionedTable) === null)
            } else {
              // otherwise, not cached.
              assert(getCachedDataSourceTable(orcUnpartitionedTable) === null)
            }

            val df = spark.sql("SELECT key, value FROM dummy_orc_unpartitioned WHERE key=0")
            checkAnswer(df, singleRowDF)
          }
        }
      }
    }
  }

  test("SPARK-32234 read ORC table with column names all starting with '_col'") {
    Seq("native", "hive").foreach { orcImpl =>
      Seq("false", "true").foreach { vectorized =>
        withSQLConf(
          SQLConf.ORC_IMPLEMENTATION.key -> orcImpl,
          SQLConf.ORC_VECTORIZED_READER_ENABLED.key -> vectorized) {
          withTable("test_hive_orc_impl") {
            spark.sql(
              s"""
                 | CREATE TABLE test_hive_orc_impl
                 | (_col1 INT, _col2 STRING, _col3 INT)
                 | STORED AS ORC
               """.stripMargin)
            spark.sql(
              s"""
                 | INSERT INTO
                 | test_hive_orc_impl
                 | VALUES(9, '12', 2020)
               """.stripMargin)

            val df = spark.sql("SELECT _col2 FROM test_hive_orc_impl")
            checkAnswer(df, Row("12"))
          }
        }
      }
    }
  }

  test("SPARK-32864: Support ORC forced positional evolution") {
    Seq("native", "hive").foreach { orcImpl =>
      Seq(true, false).foreach { forcePositionalEvolution =>
        Seq(true, false).foreach { convertMetastore =>
          withSQLConf(SQLConf.ORC_IMPLEMENTATION.key -> orcImpl,
            OrcConf.FORCE_POSITIONAL_EVOLUTION.getAttribute -> forcePositionalEvolution.toString,
            HiveUtils.CONVERT_METASTORE_ORC.key -> convertMetastore.toString) {
            withTempPath { f =>
              val path = f.getCanonicalPath
              Seq[(Integer, Integer)]((1, 2), (3, 4), (5, 6), (null, null))
                .toDF("c1", "c2").write.orc(path)
              val correctAnswer = Seq(Row(1, 2), Row(3, 4), Row(5, 6), Row(null, null))
              checkAnswer(spark.read.orc(path), correctAnswer)

              withTable("t") {
                sql(s"CREATE EXTERNAL TABLE t(c3 INT, c2 INT) STORED AS ORC LOCATION '$path'")

                val expected = if (forcePositionalEvolution) {
                  correctAnswer
                } else {
                  Seq(Row(null, 2), Row(null, 4), Row(null, 6), Row(null, null))
                }

                checkAnswer(spark.table("t"), expected)
              }
            }
          }
        }
      }
    }
  }

  test("SPARK-32864: Support ORC forced positional evolution with partitioned table") {
    Seq("native", "hive").foreach { orcImpl =>
      Seq(true, false).foreach { forcePositionalEvolution =>
        Seq(true, false).foreach { convertMetastore =>
          withSQLConf(SQLConf.ORC_IMPLEMENTATION.key -> orcImpl,
            OrcConf.FORCE_POSITIONAL_EVOLUTION.getAttribute -> forcePositionalEvolution.toString,
            HiveUtils.CONVERT_METASTORE_ORC.key -> convertMetastore.toString) {
            withTempPath { f =>
              val path = f.getCanonicalPath
              Seq[(Integer, Integer, Integer)]((1, 2, 1), (3, 4, 2), (5, 6, 3), (null, null, 4))
                .toDF("c1", "c2", "p").write.partitionBy("p").orc(path)
              val correctAnswer = Seq(Row(1, 2, 1), Row(3, 4, 2), Row(5, 6, 3), Row(null, null, 4))
              checkAnswer(spark.read.orc(path), correctAnswer)

              withTable("t") {
                sql(
                  s"""
                     |CREATE EXTERNAL TABLE t(c3 INT, c2 INT)
                     |PARTITIONED BY (p int)
                     |STORED AS ORC
                     |LOCATION '$path'
                     |""".stripMargin)
                sql("MSCK REPAIR TABLE t")
                val expected = if (forcePositionalEvolution) {
                  correctAnswer
                } else {
                  Seq(Row(null, 2, 1), Row(null, 4, 2), Row(null, 6, 3), Row(null, null, 4))
                }

                checkAnswer(spark.table("t"), expected)
              }
            }
          }
        }
      }
    }
  }
}
