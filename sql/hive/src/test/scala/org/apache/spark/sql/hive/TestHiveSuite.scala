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

import org.apache.spark.sql.{types, AnalysisException, SaveMode}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAlias, UnresolvedRelation}
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable, CatalogTableType}
import org.apache.spark.sql.catalyst.expressions.{Alias, Literal, NamedExpression, UnixTimestamp}
import org.apache.spark.sql.catalyst.expressions.aggregate.Max
import org.apache.spark.sql.catalyst.plans.logical.Aggregate
import org.apache.spark.sql.hive.execution.CreateHiveTableAsSelectCommand
import org.apache.spark.sql.hive.test.{TestHiveSingleton, TestHiveSparkSession}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SQLTestUtils
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}


class TestHiveSuite extends TestHiveSingleton with SQLTestUtils {
  test("load test table based on case sensitivity") {
    val testHiveSparkSession = spark.asInstanceOf[TestHiveSparkSession]

    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
      sql("SELECT * FROM SRC").queryExecution.analyzed
      assert(testHiveSparkSession.getLoadedTables.contains("src"))
      assert(testHiveSparkSession.getLoadedTables.size == 1)
    }
    testHiveSparkSession.reset()

    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
      val err = intercept[AnalysisException] {
        sql("SELECT * FROM SRC").queryExecution.analyzed
      }
      assert(err.message.contains("Table or view not found"))
    }
    testHiveSparkSession.reset()
  }

  test("SPARK-15887: hive-site.xml should be loaded") {
    assert(hiveClient.getConf("hive.in.test", "") == "true")
  }

  Seq(
    "parquet" -> ((
      "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
      "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
      "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    ))).foreach { case (provider, (inputFormat, outputFormat, serde)) =>

    test("replace parquet schema using CTAS") {
      withTable("alias", "alias_b") {
        withSQLConf(SQLConf.REPLACE_SCHEMA_ALIAS_AS_COLUMN.key -> "true",
          "hive.default.fileformat" -> "parquet") {
          sql("CREATE TABLE `alias` (\n  `a` INT,\n  `b` STRING)\nUSING parquet")
          val analyzed = sql(" create table  alias_b as select a, " +
            "max(unix_timestamp(b)) from alias group by a")
          import hiveContext._
          val hiveTable =
            sessionState.catalog.getTableMetadata(TableIdentifier("alias_b", Some("default")))
          assert(hiveTable.storage.serde == Some(serde))
          assert(hiveTable.storage.inputFormat == Some(inputFormat))
          assert(hiveTable.storage.outputFormat == Some(outputFormat))
          assert(hiveTable.provider == Some("hive"))
          import org.apache.spark.sql.catalyst.dsl.expressions._

          val originQuery = Aggregate(Seq('a.int),
            Seq('a.int,
              UnresolvedAlias(Max(UnixTimestamp('b.string, Literal("yyyy-MM-dd HH:mm:ss"))),
                Option(x => "max_unix_timestamp_b__yyyy-MM-dd_HH:mm:ss__")))
              .asInstanceOf[Seq[NamedExpression]],
            UnresolvedRelation(Seq("alias"))
          )
          val query = Aggregate(Seq('a.int),
            Seq('a.int,
              UnresolvedAlias(Max(UnixTimestamp('b.string, Literal("yyyy-MM-dd HH:mm:ss"))),
                Option(x => "max_unix_timestamp_b__yyyy-MM-dd_HH:mm:ss__")))
              .asInstanceOf[Seq[NamedExpression]],
            UnresolvedRelation(Seq("alias"))
          )

          val createTable = CreateHiveTableAsSelectCommand(
            CatalogTable(TableIdentifier("alias_b", Some("defualt")),
              CatalogTableType.MANAGED,
              CatalogStorageFormat.empty,
              provider = Some("hive"),
              schema = StructType(Seq(types.StructField("a", IntegerType),
                StructField("b", StringType)))),
            query = query,
            outputColumnNames = Seq("a", "max_unix_timestamp_b__yyyy-MM-dd_HH:mm:ss__"),
            mode = SaveMode.ErrorIfExists
          )

          val testHiveSparkSession = spark.asInstanceOf[TestHiveSparkSession]
          val analyer = testHiveSparkSession.sessionState.analyzer
          val analysis = analyer.execute(createTable)

          assert(analyer.execute(createTable.copy(query = originQuery))
            .find(_.isInstanceOf[Aggregate]).
            get.asInstanceOf[Aggregate].
            aggregateExpressions.drop(1).head.asInstanceOf[Alias].name
            == analysis.find(_.isInstanceOf[Aggregate]).
            get.asInstanceOf[Aggregate].
            aggregateExpressions.drop(1).head.asInstanceOf[Alias].name)
        }
      }
    }
  }
}
