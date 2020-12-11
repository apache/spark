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

import org.scalatest.BeforeAndAfterEach

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.test.SQLTestUtils

class HiveDropPartsWithFilterSuite
  extends QueryTest with SQLTestUtils with TestHiveSingleton with BeforeAndAfterEach {
  protected override def beforeAll(): Unit = {
    super.beforeAll()
    sql("create table test_drop_part(c1 int) partitioned by(year string, month string, day string)")
  }

  override protected def afterAll(): Unit = {
    try {
      sql("DROP TABLE IF EXISTS test_drop_part")
    } finally {
      super.afterAll()
    }
  }

  protected def addParts(): Unit = {
    sql("alter table test_drop_part add if not exists partition(year='2020', month='08', day='01')")
    sql("alter table test_drop_part add if not exists partition(year='2020', month='08', day='02')")
    sql("alter table test_drop_part add if not exists partition(year='2020', month='08', day='03')")
    sql("alter table test_drop_part add if not exists partition(year='2020', month='08', day='04')")
    sql("alter table test_drop_part add if not exists partition(year='2020', month='08', day='05')")
    sql("alter table test_drop_part add if not exists partition(year='2020', month='08', day='06')")
    sql("alter table test_drop_part add if not exists partition(year='2020', month='08', day='07')")
    sql("alter table test_drop_part add if not exists partition(year='2020', month='08', day='08')")
    sql("alter table test_drop_part add if not exists partition(year='2020', month='08', day='09')")
    sql("alter table test_drop_part add if not exists partition(year='2020', month='08', day='10')")

  }

  test("Case 1: The part column on the left and the value on the right like year > 2020") {
    addParts()
    sql("set spark.sql.batch.drop.partitions.enable = true;")

    // delete year=2020/month=08/day=10
    sql("alter table test_drop_part drop if exists partition (year='2020', month='08', day=='10')")
    checkAnswer(
      sql("SHOW PARTITIONS test_drop_part"),
      Row("year=2020/month=08/day=01") ::
      Row("year=2020/month=08/day=02") ::
      Row("year=2020/month=08/day=03") ::
      Row("year=2020/month=08/day=04") ::
      Row("year=2020/month=08/day=05") ::
      Row("year=2020/month=08/day=06") ::
      Row("year=2020/month=08/day=07") ::
      Row("year=2020/month=08/day=08") ::
      Row("year=2020/month=08/day=09") :: Nil)

    // delete year=2020/month=08/day=01
    sql("alter table test_drop_part drop if exists partition(year='2020', month='08', day < '02')")
    checkAnswer(
      sql("SHOW PARTITIONS test_drop_part"),
        Row("year=2020/month=08/day=02") ::
        Row("year=2020/month=08/day=03") ::
        Row("year=2020/month=08/day=04") ::
        Row("year=2020/month=08/day=05") ::
        Row("year=2020/month=08/day=06") ::
        Row("year=2020/month=08/day=07") ::
        Row("year=2020/month=08/day=08") ::
        Row("year=2020/month=08/day=09") :: Nil)

    // delete year=2020/month=08/day=02 year=2020/month=08/day=03
    sql("alter table test_drop_part drop if exists partition(year='2020', month='08', day<='03')")
    checkAnswer(
      sql("SHOW PARTITIONS test_drop_part"),
      Row("year=2020/month=08/day=04") ::
        Row("year=2020/month=08/day=05") ::
        Row("year=2020/month=08/day=06") ::
        Row("year=2020/month=08/day=07") ::
        Row("year=2020/month=08/day=08") ::
        Row("year=2020/month=08/day=09") :: Nil)

    // delete year=2020/month=08/day=04 year=2020/month=08/day=05
    sql("alter table test_drop_part drop if exists partition(year='2020', month='08', day !>'05')")
    checkAnswer(
      sql("SHOW PARTITIONS test_drop_part"),
      Row("year=2020/month=08/day=06") ::
        Row("year=2020/month=08/day=07") ::
        Row("year=2020/month=08/day=08") ::
        Row("year=2020/month=08/day=09") :: Nil)

    // delete year=2020/month=08/day=09
    sql("alter table test_drop_part drop if exists partition(year='2020', month='08', day > '08')")
    checkAnswer(
      sql("SHOW PARTITIONS test_drop_part"),
      Row("year=2020/month=08/day=06") ::
        Row("year=2020/month=08/day=07") ::
        Row("year=2020/month=08/day=08") :: Nil)

    // delete year=2020/month=08/day=07 year=2020/month=08/day=08
    sql("alter table test_drop_part drop if exists partition(year='2020', month='08', day >= '07')")
    checkAnswer(
      sql("SHOW PARTITIONS test_drop_part"),
      Row("year=2020/month=08/day=06") :: Nil)

    // delete year=2020/month=08/day=06
    sql("alter table test_drop_part drop if exists partition(year='2020', month='08', day !< '06')")
    checkAnswer(
      sql("SHOW PARTITIONS test_drop_part"), Nil)
  }

  test("Case 2: The part column on the right and the value on the left like 2020 > year") {
    addParts()
    sql("set spark.sql.batch.drop.partitions.enable = true;")

    // delete year=2020/month=08/day=10
    sql("alter table test_drop_part drop if exists partition (year='2020', month='08', '10'==day)")
    checkAnswer(
      sql("SHOW PARTITIONS test_drop_part"),
      Row("year=2020/month=08/day=01") ::
        Row("year=2020/month=08/day=02") ::
        Row("year=2020/month=08/day=03") ::
        Row("year=2020/month=08/day=04") ::
        Row("year=2020/month=08/day=05") ::
        Row("year=2020/month=08/day=06") ::
        Row("year=2020/month=08/day=07") ::
        Row("year=2020/month=08/day=08") ::
        Row("year=2020/month=08/day=09") :: Nil)

    // delete year=2020/month=08/day=01
    sql("alter table test_drop_part drop if exists partition(year='2020', month='08', '02' > day)")
    checkAnswer(
      sql("SHOW PARTITIONS test_drop_part"),
      Row("year=2020/month=08/day=02") ::
        Row("year=2020/month=08/day=03") ::
        Row("year=2020/month=08/day=04") ::
        Row("year=2020/month=08/day=05") ::
        Row("year=2020/month=08/day=06") ::
        Row("year=2020/month=08/day=07") ::
        Row("year=2020/month=08/day=08") ::
        Row("year=2020/month=08/day=09") :: Nil)

    // delete year=2020/month=08/day=02 year=2020/month=08/day=03
    sql("alter table test_drop_part drop if exists partition(year='2020', month='08', '03' >= day)")
    checkAnswer(
      sql("SHOW PARTITIONS test_drop_part"),
      Row("year=2020/month=08/day=04") ::
        Row("year=2020/month=08/day=05") ::
        Row("year=2020/month=08/day=06") ::
        Row("year=2020/month=08/day=07") ::
        Row("year=2020/month=08/day=08") ::
        Row("year=2020/month=08/day=09") :: Nil)

    // delete year=2020/month=08/day=04 year=2020/month=08/day=05
    sql("alter table test_drop_part drop if exists partition(year='2020', month='08', '05' !< day)")
    checkAnswer(
      sql("SHOW PARTITIONS test_drop_part"),
      Row("year=2020/month=08/day=06") ::
        Row("year=2020/month=08/day=07") ::
        Row("year=2020/month=08/day=08") ::
        Row("year=2020/month=08/day=09") :: Nil)

    // delete year=2020/month=08/day=09
    sql("alter table test_drop_part drop if exists partition(year='2020', month='08', '08' < day)")
    checkAnswer(
      sql("SHOW PARTITIONS test_drop_part"),
      Row("year=2020/month=08/day=06") ::
        Row("year=2020/month=08/day=07") ::
        Row("year=2020/month=08/day=08") :: Nil)

    // delete year=2020/month=08/day=07 year=2020/month=08/day=08
    sql("alter table test_drop_part drop if exists partition(year='2020', month='08', '07' <= day)")
    checkAnswer(
      sql("SHOW PARTITIONS test_drop_part"),
      Row("year=2020/month=08/day=06") :: Nil)

    // delete year=2020/month=08/day=06
    sql("alter table test_drop_part drop if exists partition(year='2020', month='08', '06' !> day)")
    checkAnswer(
      sql("SHOW PARTITIONS test_drop_part"), Nil)
  }
}
