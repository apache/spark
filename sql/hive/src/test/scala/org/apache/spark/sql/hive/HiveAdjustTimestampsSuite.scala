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
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.execution.datasources.{AdjustTimestamps, BaseAdjustTimestampsSuite}
import org.apache.spark.sql.hive.test.TestHiveSingleton

class HiveAdjustTimestampsSuite extends BaseAdjustTimestampsSuite with TestHiveSingleton {

  override protected def createAndSaveTableFunctions(): Seq[CreateAndSaveTable] = {
    super.createAndSaveTableFunctions() ++
      Seq(true, false).map(new CreateHiveTableAndInsert(_))
  }

  override protected def ctasFunctions(): Seq[CTAS] = {
    super.ctasFunctions() ++
      Seq(true, false).map(new CreateHiveTableWithTimezoneAndInsert(_))
  }

  class CreateHiveTableAndInsert(convertMetastore: Boolean) extends CreateAndSaveTable {
    override def createAndSave(
        df: DataFrame,
        table: String,
        tzOpt: Option[String],
        format: String): Boolean = {
      if (format != "parquet") {
        return false
      }

      withSQLConf(HiveUtils.CONVERT_METASTORE_PARQUET.key -> convertMetastore.toString) {
        val tblProperties = tzOpt.map { tz =>
          s"""TBLPROPERTIES ("${DateTimeUtils.TIMEZONE_PROPERTY}"="$tz")"""
        }.getOrElse("")
        spark.sql(
          s"""CREATE TABLE $table (
             |  display string,
             |  ts timestamp
             |)
             |STORED AS $format
             |$tblProperties
             |""".stripMargin)
        df.write.insertInto(table)
      }

      true
    }
  }

  class CreateHiveTableWithTimezoneAndInsert(convertMetastore: Boolean) extends CTAS {
    override def createTableFromSourceTable(
        source: String,
        dest: String,
        destTz: Option[String],
        destFormat: String): Boolean = {
      if (destFormat != "parquet") {
        return false
      }

      withSQLConf(HiveUtils.CONVERT_METASTORE_PARQUET.key -> convertMetastore.toString) {
        val tblProperties = destTz.map { tz =>
          s"""TBLPROPERTIES ("${DateTimeUtils.TIMEZONE_PROPERTY}"="$tz")"""
        }.getOrElse("")
        // this isn't just a "ctas" sql statement b/c that doesn't let us specify the table tz
        spark.sql(
          s"""CREATE TABLE $dest (
             |  display string,
             |  ts timestamp
             |)
             |STORED AS $destFormat
             |$tblProperties
             |""".stripMargin)
        spark.sql(s"insert into $dest select * from $source")
      }

      true
    }
  }

  test("SPARK-12297: copy table timezone in CREATE TABLE LIKE") {
    val key = DateTimeUtils.TIMEZONE_PROPERTY
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
