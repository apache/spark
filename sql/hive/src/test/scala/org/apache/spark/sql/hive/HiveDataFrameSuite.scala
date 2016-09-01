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

import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SQLTestUtils

class HiveDataFrameSuite extends QueryTest with SQLTestUtils with TestHiveSingleton {
  test("table name with schema") {
    // regression test for SPARK-11778
    spark.sql("create schema usrdb")
    spark.sql("create table usrdb.test(c int)")
    spark.read.table("usrdb.test")
    spark.sql("drop table usrdb.test")
    spark.sql("drop schema usrdb")
  }

  test("inputFiles") {
    Seq("SEQUENCEFILE", "ORC", "PARQUET").foreach { format =>
      withTempDir { tmpDir =>
        withTable("tab1") {
          withView("view1") {
            sql(
              s"""
                 |create table tab1
                 |stored as $format
                 |location '$tmpDir'
                 |as select 1 as a, 2 as b
               """.stripMargin)
            sql("create view view1 as select * from tab1")
            val tabDF = sql("select * from tab1")
            val viewDF = sql("select * from view1")
            assert(tabDF.inputFiles === viewDF.inputFiles)
            assert(tabDF.inputFiles.length == 1)
            if (format == "SEQUENCEFILE") {
              assert(tabDF.inputFiles(0).endsWith(tmpDir.toString))
            } else {
              assert(tabDF.inputFiles(0).toLowerCase.endsWith(format.toLowerCase))
            }
          }
        }
      }
    }
  }

  test("inputFiles - partitions") {
    Seq("SEQUENCEFILE", "ORC", "PARQUET").foreach { format =>
      withTable("tab1") {
        sql(s"create table tab1 (id int) partitioned by (p string) stored as $format")
        sql("INSERT INTO TABLE tab1 PARTITION(p='part0') SELECT 1")
        sql("INSERT INTO TABLE tab1 PARTITION(p='part1') SELECT 2")
        val dfAll = sql("select * from tab1")
        val dfPart1 = sql("select * from tab1 where p = 'part1'")
        assert(dfAll.inputFiles === dfPart1.inputFiles)
        assert(dfAll.inputFiles.length == 2)
      }
    }
  }

  test("SPARK-15887: hive-site.xml should be loaded") {
    val hiveClient = spark.sharedState.externalCatalog.asInstanceOf[HiveExternalCatalog].client
    assert(hiveClient.getConf("hive.in.test", "") == "true")
  }
}
