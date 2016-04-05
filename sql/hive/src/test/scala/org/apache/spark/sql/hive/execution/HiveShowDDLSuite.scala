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

package org.apache.spark.sql.hive.execution

import org.apache.spark.sql.{AnalysisException, QueryTest, Row}
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.test.SQLTestUtils
import org.apache.spark.util.Utils

class HiveShowDDLSuite extends QueryTest with SQLTestUtils with TestHiveSingleton {
  test("show create table - hive table - no row format") {
    withTempDir { tmpDir =>
      withTable("t1") {
        sql(
          s"""
            |create table t1(c1 int, c2 string)
            |stored as parquet
            |location '${tmpDir}'
          """.stripMargin)
        sql("show create table t1").show(false)
      }
    }
  }

  test("show create table - hive non-external table") {
    withTempDir { tmpDir =>
      withTable("t1") {
        sql(
          s"""
            |create table t1(c1 int COMMENT 'abc', c2 string) COMMENT 'my table'
            |row format delimited fields terminated by ','
            |stored as parquet
            |location '${tmpDir}'
          """.stripMargin)
        sql("show create table t1").show(false)
      }
    }
  }

  test("show create table - hive external table") {
    withTempDir { tmpDir =>
      withTable("t1") {
        sql(
          s"""
            |create external table t1(c1 int, c2 string)
            |PARTITIONED BY (c3 int COMMENT 'partition column', c4 string)
            |row format delimited fields terminated by ','
            |stored as parquet
            |location '${tmpDir}'
            |TBLPROPERTIES ('my.property.one'='true', 'my.property.two'='1',
            |'my.property.three'='2', 'my.property.four'='false')
          """.stripMargin)
        sql("show create table t1").show(false)
      }
    }
  }

  test("show create table - hive table - cluster bucket and skew") {
    withTempDir { tmpDir =>
      withTable("t1") {
        sql(
          s"""
            |create external table t1(c1 int COMMENT 'first column', c2 string)
            |COMMENT 'some table'
            |PARTITIONED BY (c3 int COMMENT 'partition column', c4 string)
            |CLUSTERED BY (c1, c2) SORTED BY (c1 ASC, C2 DESC) INTO 5 BUCKETS
            |row format delimited fields terminated by ','
            |COLLECTION ITEMS TERMINATED BY ','
            |MAP KEYS TERMINATED BY ','
            |NULL DEFINED AS 'NnN'
            |stored as parquet
            |location '${tmpDir}'
            |TBLPROPERTIES ('my.property.one'='true', 'my.property.two'='1',
            |'my.property.three'='2', 'my.property.four'='false')
          """.stripMargin)
        sql("show create table t1").show(false)
      }
    }
  }

  test(
    "show create table - hive temp table") {
    withTempDir { tmpDir =>
      withTable("t1") {
        sql(
          s"""
            |create TEMPORARY table t1(c1 int, c2 string)
            |row format delimited fields terminated by ','
            |stored as parquet
            |location '${tmpDir}'
          """.stripMargin)
        sql("show create table t1").show(false)
      }
    }
  }

  test("show create table - hive TEMPORARY external table") {
    withTempDir { tmpDir =>
      withTable("t1") {
        sql(
          s"""
            |create TEMPORARY external table t1(c1 int, c2 string)
            |row format delimited fields terminated by ','
            |stored as parquet
            |location '${tmpDir}'
          """.stripMargin)
        sql("show create table t1").show(false)
      }
    }
  }

  test("show create table - hive view") {
    withTempDir { tmpDir =>
      withTable("t1") {
        withView("v1") {
          sql(
            s"""
              |create table t1(c1 int, c2 string)
              |row format delimited fields terminated by ','
              |stored as parquet
              |location '${tmpDir}'
            """.stripMargin)
          sql(
            """
              |create view v1 as select * from t1
            """.stripMargin)
          sql("show create table v1").show(false)
        }
      }
    }
  }

  test("show create temp table") {
    withTable("t1") {
      sql(
        """
          |create temporary table t1(c1 int, c2 string)
        """.stripMargin)
      sql("show create table t1").show(false)
    }
  }

  test("show create table -- datasource table") {
    withTable("t_datasource") {
      sql("select 1, 'abc'").write.saveAsTable("t_datasource")
      sql("show create table t_datasource").show(false)
    }
  }

  test("show create table -- partitioned") {
    val jsonFilePath = Utils.getSparkClassLoader.getResource("sample.json").getFile
    withTable("t_datasource") {
      val df = sqlContext.read.json(jsonFilePath)
      df.write.format("json").saveAsTable("t_datasource")
      sql("show create table t_datasource").show(false)
    }
  }

  test("show create table -- USING and OPTIONS") {
    val jsonFilePath = Utils.getSparkClassLoader.getResource("sample.json").getFile
    withTable("jsonTable") {
      sql(
        s"""
           |CREATE TABLE jsonTable
           |USING org.apache.spark.sql.json.DefaultSource
           |OPTIONS (
           |  path '$jsonFilePath'
           |)
         """.stripMargin)
      sql("show create table jsonTable").show(false)
    }
  }

  test("show create table -- USING and OPTIONS with column definition") {
    val jsonFilePath = Utils.getSparkClassLoader.getResource("sample.json").getFile
    withTable("jsonTable") {
      sql(
        s"""
           |CREATE TABLE jsonTable (c1 string, c2 string, c3 int)
           |USING org.apache.spark.sql.json.DefaultSource
           |OPTIONS (
           |  path '$jsonFilePath'
           |)
         """.stripMargin)
      sql("show create table jsonTable").show(false)
    }
  }
}
