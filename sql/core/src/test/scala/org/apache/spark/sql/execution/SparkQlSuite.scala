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

package org.apache.spark.sql.execution

import org.apache.spark.sql.catalyst.plans.PlanTest

class SparkQlSuite extends PlanTest {
  val parser = new SparkQl()

  test("create database") {
    parser.parsePlan("CREATE DATABASE IF NOT EXISTS database_name " +
      "COMMENT 'database_comment' LOCATION '/home/user/db' " +
      "WITH DBPROPERTIES ('a'='a', 'b'='b', 'c'='c')")
  }

  test("create function") {
    parser.parsePlan("CREATE TEMPORARY FUNCTION helloworld as " +
      "'com.matthewrathbone.example.SimpleUDFExample' USING JAR '/path/to/jar', " +
      "FILE 'path/to/file'")
  }

  test("alter table") {
    // Rename table
    parser.parsePlan("ALTER TABLE table_name RENAME TO new_table_name")

    // Alter table properties
    parser.parsePlan("ALTER TABLE table_name SET TBLPROPERTIES ('test' = 'test', " +
      "'comment' = 'new_comment')")
    parser.parsePlan("ALTER TABLE table_name UNSET TBLPROPERTIES ('comment', 'test')")
    parser.parsePlan("ALTER TABLE table_name UNSET TBLPROPERTIES IF EXISTS ('comment', 'test')")

    // Alter table SerDe properties
    parser.parsePlan("ALTER TABLE table_name SET SERDE 'org.apache.class'")
    parser.parsePlan("ALTER TABLE table_name SET SERDE 'org.apache.class' " +
      "WITH SERDEPROPERTIES ('columns'='foo,bar', 'field.delim' = ',')")
    parser.parsePlan("ALTER TABLE table_name SET SERDEPROPERTIES ('columns'='foo,bar', " +
      "'field.delim' = ',')")
    parser.parsePlan("ALTER TABLE table_name PARTITION (test, dt='2008-08-08', " +
      "country='us') SET SERDE 'org.apache.class' WITH SERDEPROPERTIES ('columns'='foo,bar', " +
      "'field.delim' = ',')")
    parser.parsePlan("ALTER TABLE table_name PARTITION (test, dt='2008-08-08', " +
      "country='us') SET SERDEPROPERTIES ('columns'='foo,bar', 'field.delim' = ',')")

    // Alter Table storage properties
    parser.parsePlan("ALTER TABLE table_name CLUSTERED BY (dt, country) " +
      "INTO 10 BUCKETS")
    parser.parsePlan("ALTER TABLE table_name CLUSTERED BY (dt, country) SORTED BY " +
      "(dt, country DESC) INTO 10 BUCKETS")
    parser.parsePlan("ALTER TABLE table_name INTO 20 BUCKETS")
    parser.parsePlan("ALTER TABLE table_name NOT CLUSTERED")
    parser.parsePlan("ALTER TABLE table_name NOT SORTED")

    // Alter Table skewed
    parser.parsePlan("ALTER TABLE table_name SKEWED BY (dt, country) ON " +
      "(('2008-08-08', 'us'), ('2009-09-09', 'uk')) STORED AS DIRECTORIES")
    parser.parsePlan("ALTER TABLE table_name SKEWED BY (dt, country) ON " +
      "('2008-08-08', 'us') STORED AS DIRECTORIES")
    parser.parsePlan("ALTER TABLE table_name SKEWED BY (dt, country) ON " +
      "(('2008-08-08', 'us'), ('2009-09-09', 'uk'))")

    // Alter Table skewed location
    parser.parsePlan("ALTER TABLE table_name SET SKEWED LOCATION " +
      "('123'='location1', 'test'='location2')")
    parser.parsePlan("ALTER TABLE table_name SET SKEWED LOCATION " +
      "(('2008-08-08', 'us')='location1', 'test'='location2')")

    // Alter Table add partition
    parser.parsePlan("ALTER TABLE table_name ADD IF NOT EXISTS PARTITION " +
      "(dt='2008-08-08', country='us') LOCATION 'location1' PARTITION " +
      "(dt='2009-09-09', country='uk')")

    // Alter Table rename partition
    parser.parsePlan("ALTER TABLE table_name PARTITION (dt='2008-08-08', country='us') " +
      "RENAME TO PARTITION (dt='2008-09-09', country='uk')")

    // Alter Table exchange partition
    parser.parsePlan("ALTER TABLE table_name_1 EXCHANGE PARTITION " +
      "(dt='2008-08-08', country='us') WITH TABLE table_name_2")

    // Alter Table drop partitions
    parser.parsePlan("ALTER TABLE table_name DROP IF EXISTS PARTITION " +
      "(dt='2008-08-08', country='us'), PARTITION (dt='2009-09-09', country='uk')")
    parser.parsePlan("ALTER TABLE table_name DROP IF EXISTS PARTITION " +
      "(dt='2008-08-08', country='us'), PARTITION (dt='2009-09-09', country='uk') " +
      "PURGE FOR METADATA REPLICATION ('test')")

    // Alter Table archive partition
    parser.parsePlan("ALTER TABLE table_name ARCHIVE PARTITION (dt='2008-08-08', country='us')")

    // Alter Table unarchive partition
    parser.parsePlan("ALTER TABLE table_name UNARCHIVE PARTITION (dt='2008-08-08', country='us')")

    // Alter Table set file format
    parser.parsePlan("ALTER TABLE table_name SET FILEFORMAT INPUTFORMAT 'test' " +
      "OUTPUTFORMAT 'test' SERDE 'test' INPUTDRIVER 'test' OUTPUTDRIVER 'test'")
    parser.parsePlan("ALTER TABLE table_name SET FILEFORMAT INPUTFORMAT 'test' " +
      "OUTPUTFORMAT 'test' SERDE 'test'")
    parser.parsePlan("ALTER TABLE table_name PARTITION (dt='2008-08-08', country='us') " +
      "SET FILEFORMAT PARQUET")

    // Alter Table set location
    parser.parsePlan("ALTER TABLE table_name SET LOCATION 'new location'")
    parser.parsePlan("ALTER TABLE table_name PARTITION (dt='2008-08-08', country='us') " +
      "SET LOCATION 'new location'")

    // Alter Table touch
    parser.parsePlan("ALTER TABLE table_name TOUCH")
    parser.parsePlan("ALTER TABLE table_name TOUCH PARTITION (dt='2008-08-08', country='us')")

    // Alter Table compact
    parser.parsePlan("ALTER TABLE table_name COMPACT 'compaction_type'")
    parser.parsePlan("ALTER TABLE table_name PARTITION (dt='2008-08-08', country='us') " +
      "COMPACT 'MAJOR'")

    // Alter Table concatenate
    parser.parsePlan("ALTER TABLE table_name CONCATENATE")
    parser.parsePlan("ALTER TABLE table_name PARTITION (dt='2008-08-08', country='us') CONCATENATE")

    // Alter Table change column name/type/position/comment
    parser.parsePlan("ALTER TABLE table_name CHANGE col_old_name col_new_name INT")

    parser.parsePlan("ALTER TABLE table_name CHANGE COLUMN col_old_name col_new_name INT " +
      "COMMENT 'col_comment' FIRST CASCADE")
    parser.parsePlan("ALTER TABLE table_name CHANGE COLUMN col_old_name col_new_name INT " +
      "COMMENT 'col_comment' AFTER column_name RESTRICT")

    // Alter Table add/replace columns
    parser.parsePlan("ALTER TABLE table_name PARTITION (dt='2008-08-08', country='us') " +
      "ADD COLUMNS (new_col1 INT COMMENT 'test_comment', new_col2 LONG " +
      "COMMENT 'test_comment2') CASCADE")
    parser.parsePlan("ALTER TABLE table_name REPLACE COLUMNS (new_col1 INT " +
      "COMMENT 'test_comment', new_col2 LONG COMMENT 'test_comment2') RESTRICT")
  }
}
