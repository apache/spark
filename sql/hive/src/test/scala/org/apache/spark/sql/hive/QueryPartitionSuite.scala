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

import java.io.File

import com.google.common.io.Files
import org.apache.hadoop.fs.FileSystem

import org.apache.spark.sql._
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SQLTestUtils
import org.apache.spark.util.Utils

class QueryPartitionSuite extends QueryTest with SQLTestUtils with TestHiveSingleton {
  import spark.implicits._

  test("SPARK-5068: query data when path doesn't exist") {
    withSQLConf((SQLConf.HIVE_VERIFY_PARTITION_PATH.key, "true")) {
      val testData = sparkContext.parallelize(
        (1 to 10).map(i => TestData(i, i.toString))).toDF()
      testData.createOrReplaceTempView("testData")

      val tmpDir = Files.createTempDir()
      // create the table for test
      sql(s"CREATE TABLE table_with_partition(key int,value string) " +
        s"PARTITIONED by (ds string) location '${tmpDir.toURI.toString}' ")
      sql("INSERT OVERWRITE TABLE table_with_partition  partition (ds='1') " +
        "SELECT key,value FROM testData")
      sql("INSERT OVERWRITE TABLE table_with_partition  partition (ds='2') " +
        "SELECT key,value FROM testData")
      sql("INSERT OVERWRITE TABLE table_with_partition  partition (ds='3') " +
        "SELECT key,value FROM testData")
      sql("INSERT OVERWRITE TABLE table_with_partition  partition (ds='4') " +
        "SELECT key,value FROM testData")

      // test for the exist path
      checkAnswer(sql("select key,value from table_with_partition"),
        testData.toDF.collect ++ testData.toDF.collect
          ++ testData.toDF.collect ++ testData.toDF.collect)

      // delete the path of one partition
      tmpDir.listFiles
        .find { f => f.isDirectory && f.getName().startsWith("ds=") }
        .foreach { f => Utils.deleteRecursively(f) }

      // test for after delete the path
      checkAnswer(sql("select key,value from table_with_partition"),
        testData.toDF.collect ++ testData.toDF.collect ++ testData.toDF.collect)

      sql("DROP TABLE IF EXISTS table_with_partition")
      sql("DROP TABLE IF EXISTS createAndInsertTest")
    }
  }

  test("SPARK-13709: reading partitioned Avro table with nested schema") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      val tableName = "spark_13709"
      val tempTableName = "spark_13709_temp"

      new File(path, tableName).mkdir()
      new File(path, tempTableName).mkdir()

      val avroSchema =
        """{
          |  "name": "test_record",
          |  "type": "record",
          |  "fields": [ {
          |    "name": "f0",
          |    "type": "int"
          |  }, {
          |    "name": "f1",
          |    "type": {
          |      "type": "record",
          |      "name": "inner",
          |      "fields": [ {
          |        "name": "f10",
          |        "type": "int"
          |      }, {
          |        "name": "f11",
          |        "type": "double"
          |      } ]
          |    }
          |  } ]
          |}
        """.stripMargin

      withTable(tableName, tempTableName) {
        // Creates the external partitioned Avro table to be tested.
        sql(
          s"""CREATE EXTERNAL TABLE $tableName
             |PARTITIONED BY (ds STRING)
             |ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
             |STORED AS
             |  INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
             |  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
             |LOCATION '$path/$tableName'
             |TBLPROPERTIES ('avro.schema.literal' = '$avroSchema')
           """.stripMargin
        )

        // Creates an temporary Avro table used to prepare testing Avro file.
        sql(
          s"""CREATE EXTERNAL TABLE $tempTableName
             |ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
             |STORED AS
             |  INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
             |  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
             |LOCATION '$path/$tempTableName'
             |TBLPROPERTIES ('avro.schema.literal' = '$avroSchema')
           """.stripMargin
        )

        // Generates Avro data.
        sql(s"INSERT OVERWRITE TABLE $tempTableName SELECT 1, STRUCT(2, 2.5)")

        // Adds generated Avro data as a new partition to the testing table.
        sql(s"ALTER TABLE $tableName ADD PARTITION (ds = 'foo') LOCATION '$path/$tempTableName'")

        // The following query fails before SPARK-13709 is fixed. This is because when reading data
        // from table partitions, Avro deserializer needs the Avro schema, which is defined in
        // table property "avro.schema.literal". However, we only initializes the deserializer using
        // partition properties, which doesn't include the wanted property entry. Merging two sets
        // of properties solves the problem.
        checkAnswer(
          sql(s"SELECT * FROM $tableName"),
          Row(1, Row(2, 2.5D), "foo")
        )
      }
    }
  }
}
