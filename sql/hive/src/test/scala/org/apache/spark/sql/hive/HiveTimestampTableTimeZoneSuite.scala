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

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.datasources.{BaseTimestampTableTimeZoneSuite, TimestampTableTimeZone}
import org.apache.spark.sql.hive.test.TestHiveSingleton

class HiveTimestampTableTimeZoneSuite extends BaseTimestampTableTimeZoneSuite
    with TestHiveSingleton {

  override protected def createAndSaveTableFunctions(): Seq[CreateAndSaveTable] = {
    super.createAndSaveTableFunctions() ++ Seq(CreateHiveTableAndInsert)
  }

  override protected def ctasFunctions(): Seq[CTAS] = {
    super.ctasFunctions() ++ Seq(CreateHiveTableWithTimezoneAndInsert)
  }

  object CreateHiveTableAndInsert extends CreateAndSaveTable {
    override def createAndSave(
        df: DataFrame,
        table: String,
        tzOpt: Option[String],
        format: String): Boolean = {
      if (format == "parquet") {
        val tblProperties = tzOpt.map { tz =>
          s"""TBLPROPERTIES ("${TimestampTableTimeZone.TIMEZONE_PROPERTY}"="$tz")"""
        }.getOrElse("")
        spark.sql(
          s"""CREATE TABLE $table (
             |  display string,
             |  ts timestamp
             |)
             |STORED AS parquet
             |$tblProperties
             |""".stripMargin)
        df.write.insertInto(table)
        true
      } else {
        false
      }
    }
  }

  object CreateHiveTableWithTimezoneAndInsert extends CTAS {
    override def createTableFromSourceTable(
        source: String,
        dest: String,
        destTz: Option[String],
        destFormat: String): Boolean = {
      if (destFormat == "parquet") {
        val tblProperties = destTz.map { tz =>
          s"""TBLPROPERTIES ("${TimestampTableTimeZone.TIMEZONE_PROPERTY}"="$tz")"""
        }.getOrElse("")
        // this isn't just a "ctas" sql statement b/c that doesn't let us specify the table tz
        spark.sql(
          s"""CREATE TABLE $dest (
             |  display string,
             |  ts timestamp
             |)
             |STORED AS parquet
             |$tblProperties
             |""".stripMargin)
        spark.sql(s"insert into $dest select * from $source")
        true
      } else {
        false
      }

    }
  }

  test("SPARK-12297: copy table timezone in CREATE TABLE LIKE") {
    val key = TimestampTableTimeZone.TIMEZONE_PROPERTY
    withTable("orig_hive", "copy_hive", "orig_ds", "copy_ds") {
      spark.sql(
        s"""CREATE TABLE orig_hive (
           |  display string,
           |  ts timestamp
           |)
           |STORED AS parquet
           |TBLPROPERTIES ("$key"="UTC")
           |""".
          stripMargin)
      spark.sql("CREATE TABLE copy_hive LIKE orig_hive")
      checkHasTz(spark, "copy_hive", Some("UTC"))

      createRawData(spark).write.option(key, "America/New_York").saveAsTable("orig_ds")
      spark.sql("CREATE TABLE copy_ds LIKE orig_ds")
      checkHasTz(spark, "copy_ds", Some("America/New_York"))
    }

  }
}
