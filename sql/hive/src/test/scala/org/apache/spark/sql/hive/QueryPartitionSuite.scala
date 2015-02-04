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

import org.scalatest.BeforeAndAfter

import org.apache.spark.util.Utils
import org.apache.spark.sql.hive.test.TestHive._
import org.apache.spark.sql.hive.execution.HiveComparisonTest

abstract class QueryPartitionSuite extends HiveComparisonTest with BeforeAndAfter {
  protected val tmpDir = Utils.createTempDir()

  override def beforeAll() {
    sql(
      s"""CREATE TABLE table_with_partition(key int,value string)
         |PARTITIONED by (ds string) location '${tmpDir.toURI.toString}'
         |""".stripMargin)
    sql(
      s"""INSERT OVERWRITE TABLE table_with_partition
         | partition (ds='1') SELECT key,value FROM src LIMIT 1""".stripMargin)
    sql(
      s"""INSERT OVERWRITE TABLE table_with_partition
         | partition (ds='2') SELECT key,value FROM src LIMIT 1""".stripMargin)
    sql(
      s"""INSERT OVERWRITE TABLE table_with_partition
         | partition (ds='3') SELECT key,value FROM src LIMIT 1""".stripMargin)
    sql(
      s"""INSERT OVERWRITE TABLE table_with_partition
         | partition (ds='4') SELECT key,value FROM src LIMIT 1""".stripMargin)
  }

  override def afterAll() {
    sql("DROP TABLE table_with_partition")
  }
}

class QueryPartitionSuite0 extends QueryPartitionSuite {
  //test for the exist path
  createQueryTest("SPARK-5068 scan partition with existed path",
    "select key,value from table_with_partition", false)
}

class QueryPartitionSuite1 extends QueryPartitionSuite {
  override def beforeAll(): Unit = {
    super.beforeAll()
    //delete the one of the partition manually
    val folders = tmpDir.listFiles.filter(_.isDirectory)
    Utils.deleteRecursively(folders(0))
  }

  //test for the after deleting the partition path
  createQueryTest("SPARK-5068 scan partition with non-existed path",
    "select key,value from table_with_partition", false)
}

class QueryPartitionSuite2 extends QueryPartitionSuite {
  override def beforeAll(): Unit = {
    super.beforeAll()
    // delete the path of the table file
    Utils.deleteRecursively(tmpDir)
  }

  createQueryTest("SPARK-5068 scan table with non-existed path",
    "select key,value from table_with_partition", false)
}
