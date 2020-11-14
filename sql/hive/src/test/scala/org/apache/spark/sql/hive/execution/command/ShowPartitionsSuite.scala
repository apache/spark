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

package org.apache.spark.sql.hive.execution.command

import org.apache.spark.sql.{AnalysisException, Row, SaveMode}
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.execution.command.v1
import org.apache.spark.sql.hive.test.TestHiveSingleton

class ShowPartitionsSuite extends v1.ShowPartitionsSuiteBase with TestHiveSingleton {
  override def version: String = "Hive V1"
  override def defaultUsing: String = "USING HIVE"

  override protected def createDateTable(table: String): Unit = {
    sql(s"""
        |CREATE TABLE $table (price int, qty int)
        |partitioned by (year int, month int)""".stripMargin)
    sql(s"INSERT INTO $table PARTITION(year = 2015, month = 1) SELECT 1, 1")
    sql(s"INSERT INTO $table PARTITION(year = 2015, month = 2) SELECT 2, 2")
    sql(s"INSERT INTO $table PARTITION(year = 2016, month = 2) SELECT 3, 3")
    sql(s"INSERT INTO $table PARTITION(year = 2016, month = 3) SELECT 3, 3")
  }

  ignore("show partitions - show everything more than 5 part keys") {
    checkAnswer(
      sql("show partitions parquet_tab5"),
      Row("year=2016/month=3/hour=10/minute=10/sec=10/extra=1") ::
        Row("year=2016/month=4/hour=10/minute=10/sec=10/extra=1") :: Nil)
  }

  ignore("show partitions - empty row") {
    withTempView("parquet_temp") {
      sql(
        """
          |CREATE TEMPORARY VIEW parquet_temp (c1 INT, c2 STRING)
          |USING org.apache.spark.sql.parquet.DefaultSource
        """.stripMargin)
      // An empty sequence of row is returned for session temporary table.
      intercept[NoSuchTableException] {
        sql("SHOW PARTITIONS parquet_temp")
      }

      val message1 = intercept[AnalysisException] {
        sql("SHOW PARTITIONS parquet_tab3")
      }.getMessage
      assert(message1.contains("not allowed on a table that is not partitioned"))

      val message2 = intercept[AnalysisException] {
        sql("SHOW PARTITIONS parquet_tab4 PARTITION(abcd=2015, xyz=1)")
      }.getMessage
      assert(message2.contains("Non-partitioning column(s) [abcd, xyz] are specified"))

      val message3 = intercept[AnalysisException] {
        sql("SHOW PARTITIONS parquet_view1")
      }.getMessage
      assert(message3.contains("is not allowed on a view"))
    }
  }

  ignore("show partitions - datasource") {
    import spark.implicits._
    withTable("part_datasrc") {
      val df = (1 to 3).map(i => (i, s"val_$i", i * 2)).toDF("a", "b", "c")
      df.write
        .partitionBy("a")
        .format("parquet")
        .mode(SaveMode.Overwrite)
        .saveAsTable("part_datasrc")

      assert(sql("SHOW PARTITIONS part_datasrc").count() == 3)
    }
  }
}
