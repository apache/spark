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
import org.apache.spark.sql.execution.datasources.BaseAdjustTimestampsSuite
import org.apache.spark.sql.hive.test.TestHiveSingleton

class HiveAdjustTimestampsSuite extends BaseAdjustTimestampsSuite with TestHiveSingleton {

  override protected def createAndSaveTableFunctions(): Map[String, CreateAndSaveTable] = {
    val hiveFns = Map(
      "hive_parquet" -> new CreateHiveTableAndInsert())

    super.createAndSaveTableFunctions() ++ hiveFns
  }

  override protected def ctasFunctions(): Map[String, CTAS] = {
    // Disabling metastore conversion will also modify how data is read when the the CTAS query is
    // run if the source is a Hive table; so, the test that uses "hive_parquet" as the source and
    // "hive_parquet_no_conversion" as the target is actually using the "no metastore conversion"
    // path for both, making it unnecessary to also have the "no conversion" case in the save
    // functions.
    val hiveFns = Map(
      "hive_parquet" -> new CreateHiveTableWithTimezoneAndInsert(true),
      "hive_parquet_no_conversion" ->  new CreateHiveTableWithTimezoneAndInsert(false))

    super.ctasFunctions() ++ hiveFns
  }

  class CreateHiveTableAndInsert extends CreateAndSaveTable {
    override def createAndSave(df: DataFrame, table: String, tzOpt: Option[String]): Unit = {
      val tblProperties = tzOpt.map { tz =>
        s"""TBLPROPERTIES ("$TZ_KEY"="$tz")"""
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
    }

    override val format: String = "parquet"
  }

  class CreateHiveTableWithTimezoneAndInsert(convertMetastore: Boolean) extends CTAS {
    override def createFromSource(source: String, dest: String, destTz: Option[String]): Unit = {
      withSQLConf(HiveUtils.CONVERT_METASTORE_PARQUET.key -> convertMetastore.toString) {
        val tblProperties = destTz.map { tz =>
          s"""TBLPROPERTIES ("$TZ_KEY"="$tz")"""
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
      }
    }

    override val format: String = "parquet"
  }

  test("copy table timezone in CREATE TABLE LIKE") {
    withTable("orig_hive", "copy_hive", "orig_ds", "copy_ds") {
      spark.sql(
        s"""CREATE TABLE orig_hive (
           |  display string,
           |  ts timestamp
           |)
           |STORED AS parquet
           |TBLPROPERTIES ("$TZ_KEY"="$UTC")
           |""".
          stripMargin)
      spark.sql("CREATE TABLE copy_hive LIKE orig_hive")
      checkHasTz(spark, "copy_hive", Some(UTC))

      createRawData(spark).write.option(TZ_KEY, TABLE_TZ).saveAsTable("orig_ds")
      spark.sql("CREATE TABLE copy_ds LIKE orig_ds")
      checkHasTz(spark, "copy_ds", Some(TABLE_TZ))
    }

  }
}
