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

import org.apache.spark.sql.{AnalysisException, Row}
import org.apache.spark.sql.catalyst.catalog.HiveTableRelation
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.execution.datasources.orc.OrcQueryTest
import org.apache.spark.sql.hive.HiveUtils
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

          // This creates 1 empty ORC file with Hive ORC SerDe.  We are using this trick because
          // Spark SQL ORC data source always avoids write empty ORC files.
          spark.sql(
            s"""INSERT INTO TABLE empty_orc
               |SELECT key, value FROM empty
             """.stripMargin)

          val errorMessage = intercept[AnalysisException] {
            spark.read.orc(path)
          }.getMessage

          assert(errorMessage.contains("Unable to infer schema for ORC"))

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

  // Since Hive 1.2.1 library code path still has this problem, users may hit this
  // when spark.sql.hive.convertMetastoreOrc=false. However, after SPARK-22279,
  // Apache Spark with the default configuration doesn't hit this bug.
  test("SPARK-22267 Spark SQL incorrectly reads ORC files when column order is different") {
    Seq("native", "hive").foreach { orcImpl =>
      withSQLConf(SQLConf.ORC_IMPLEMENTATION.key -> orcImpl) {
        withTempPath { f =>
          val path = f.getCanonicalPath
          Seq(1 -> 2).toDF("c1", "c2").write.orc(path)
          checkAnswer(spark.read.orc(path), Row(1, 2))

          withSQLConf(HiveUtils.CONVERT_METASTORE_ORC.key -> "true") { // default since 2.3.0
            withTable("t") {
              sql(s"CREATE EXTERNAL TABLE t(c2 INT, c1 INT) STORED AS ORC LOCATION '$path'")
              checkAnswer(spark.table("t"), Row(2, 1))
            }
          }
        }
      }
    }
  }

  // Since Hive 1.2.1 library code path still has this problem, users may hit this
  // when spark.sql.hive.convertMetastoreOrc=false. However, after SPARK-22279,
  // Apache Spark with the default configuration doesn't hit this bug.
  test("SPARK-19809 NullPointerException on zero-size ORC file") {
    Seq("native", "hive").foreach { orcImpl =>
      withSQLConf(SQLConf.ORC_IMPLEMENTATION.key -> orcImpl) {
        withTempPath { dir =>
          withTable("spark_19809") {
            sql(s"CREATE TABLE spark_19809(a int) STORED AS ORC LOCATION '$dir'")
            Files.touch(new File(s"${dir.getCanonicalPath}", "zero.orc"))

            withSQLConf(HiveUtils.CONVERT_METASTORE_ORC.key -> "true") { // default since 2.3.0
              checkAnswer(spark.table("spark_19809"), Seq.empty)
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
    assume(HiveUtils.isHive23, "bad test: This bug fixed by HIVE-13083(Hive 2.0.1)")
    withSQLConf(HiveUtils.CONVERT_METASTORE_ORC.key -> "false") {
      withTable("spark_26437") {
        sql("CREATE TABLE spark_26437 STORED AS ORCFILE AS SELECT 0.00 AS c1")
        checkAnswer(spark.table("spark_26437"), Seq(Row(0.00)))
      }
    }
  }
}
