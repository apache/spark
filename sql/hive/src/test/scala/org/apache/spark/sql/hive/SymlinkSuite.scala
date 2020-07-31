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

import java.io.{FileWriter, PrintWriter}

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.internal.SQLConf

class SymlinkSuite extends QueryTest with TestHiveSingleton {

  test("symlink csv") {
    withTempDir { temp =>
      val drop = "DROP TABLE IF EXISTS symlink_csv"
      spark.sql(drop)

      val ddl =
        s"""CREATE TABLE symlink_csv(
           |a BIGINT,
           |b BIGINT,
           |c BIGINT
           |)
           |ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
           |STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat'
           |OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
               """.stripMargin
      spark.sql(ddl)

      val testSymlinkPath = s"${temp.getCanonicalPath}/symlink_csv.txt"
      val prefix = System.getProperty("user.dir")
      val testData1 = s"file://$prefix/src/test/resources/data/files/sample1.csv\n"
      val testData2 = s"file://$prefix/src/test/resources/data/files/sample2.csv\n"
      val writer = new PrintWriter(new FileWriter(testSymlinkPath, false))
      writer.write(testData1)
      writer.write(testData2)
      writer.close()

      val load = s"LOAD DATA LOCAL INPATH '${testSymlinkPath}' INTO TABLE symlink_csv"
      spark.sql(load)

      val dml = "SELECT * FROM symlink_csv"
      val df = spark.sql(dml)

      df.show()
      assert(df.count() == 3)
    }
  }

  test("symlink csv partitioned") {
    withTempDir { temp =>
      val drop = "DROP TABLE IF EXISTS symlink_csv_partitioned"
      spark.sql(drop)

      val ddl =
        s"""CREATE TABLE symlink_csv_partitioned(
           |a BIGINT,
           |b BIGINT,
           |c BIGINT
           |)
           |PARTITIONED BY (dt STRING)
           |ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
           |STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat'
           |OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
                 """.stripMargin
      spark.sql(ddl)

      val testSymlinkPath1 = s"${temp.getCanonicalPath}/symlink_csv1.txt"
      val testSymlinkPath2 = s"${temp.getCanonicalPath}/symlink_csv2.txt"
      val prefix = System.getProperty("user.dir")
      val testData1 = s"file://$prefix/src/test/resources/data/files/sample1.csv\n"
      val testData2 = s"file://$prefix/src/test/resources/data/files/sample2.csv\n"
      val writer1 = new PrintWriter(new FileWriter(testSymlinkPath1, false))
      val writer2 = new PrintWriter(new FileWriter(testSymlinkPath2, false))
      writer1.write(testData1)
      writer2.write(testData2)
      writer1.close()
      writer2.close()

      val load1 = s"LOAD DATA LOCAL INPATH '${testSymlinkPath1}' " +
        "INTO TABLE symlink_csv_partitioned PARTITION (dt=20200726)"
      val load2 = s"LOAD DATA LOCAL INPATH '${testSymlinkPath2}' " +
        "INTO TABLE symlink_csv_partitioned PARTITION (dt=20200727)"
      spark.sql(load1)
      spark.sql(load2)

      val dml = "SELECT * FROM symlink_csv_partitioned"
      val df = spark.sql(dml)

      df.show()
      assert(df.count() == 3)
    }
  }

  test("symlink orc") {
    withTempDir { temp =>
      val drop = "DROP TABLE IF EXISTS symlink_orc"
      spark.sql(drop)

      val ddl =
        s"""CREATE TABLE symlink_orc(
           |a BIGINT,
           |b BIGINT,
           |c BIGINT
           |)
           |ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
           |STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat'
           |OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
                 """.stripMargin
      spark.sql(ddl)

      val testSymlinkPath = s"${temp.getCanonicalPath}/symlink_orc.txt"
      val prefix = System.getProperty("user.dir")
      val testData1 = s"file://$prefix/src/test/resources/data/files/sample1.snappy.orc\n"
      val testData2 = s"file://$prefix/src/test/resources/data/files/sample2.snappy.orc\n"
      val writer = new PrintWriter(new FileWriter(testSymlinkPath, false))
      writer.write(testData1)
      writer.write(testData2)
      writer.close()

      val load = s"LOAD DATA LOCAL INPATH '${testSymlinkPath}' INTO TABLE symlink_orc"
      spark.sql(load)

      val dml = "SELECT * FROM symlink_orc"
      val df = spark.sql(dml)

      df.show()
      assert(df.count() == 3)
    }
  }

  test("symlink orc partitioned") {
    withTempDir { temp =>
      val drop = "DROP TABLE IF EXISTS symlink_orc_partitioned"
      spark.sql(drop)

      val ddl =
        s"""CREATE TABLE symlink_orc_partitioned(
           |a BIGINT,
           |b BIGINT,
           |c BIGINT
           |)
           |PARTITIONED BY (dt STRING)
           |ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
           |STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat'
           |OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
                 """.stripMargin
      spark.sql(ddl)

      val testSymlinkPath1 = s"${temp.getCanonicalPath}/symlink_orc1.txt"
      val testSymlinkPath2 = s"${temp.getCanonicalPath}/symlink_orc2.txt"
      val prefix = System.getProperty("user.dir")
      val testData1 = s"file://$prefix/src/test/resources/data/files/sample1.snappy.orc\n"
      val testData2 = s"file://$prefix/src/test/resources/data/files/sample2.snappy.orc\n"
      val writer1 = new PrintWriter(new FileWriter(testSymlinkPath1, false))
      val writer2 = new PrintWriter(new FileWriter(testSymlinkPath2, false))
      writer1.write(testData1)
      writer2.write(testData2)
      writer1.close()
      writer2.close()

      val load1 = s"LOAD DATA LOCAL INPATH '${testSymlinkPath1}' " +
        "INTO TABLE symlink_orc_partitioned PARTITION (dt=20200726)"
      val load2 = s"LOAD DATA LOCAL INPATH '${testSymlinkPath2}' " +
        "INTO TABLE symlink_orc_partitioned PARTITION (dt=20200727)"
      spark.sql(load1)
      spark.sql(load2)

      val dml = "SELECT * FROM symlink_orc_partitioned"
      val df = spark.sql(dml)

      df.show()
      assert(df.count() == 3)
    }
  }

  test("symlink orc partitioned, lazy pruning disabled") {
    withTempDir { temp =>
      spark.conf.set(SQLConf.HIVE_MANAGE_FILESOURCE_PARTITIONS.key, false)

      val drop = "DROP TABLE IF EXISTS symlink_orc_partitioned_nonlazy"
      spark.sql(drop)

      val ddl =
        s"""CREATE TABLE symlink_orc_partitioned_nonlazy(
           |a BIGINT,
           |b BIGINT,
           |c BIGINT
           |)
           |PARTITIONED BY (dt STRING)
           |ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
           |STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat'
           |OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
                 """.stripMargin
      spark.sql(ddl)

      val testSymlinkPath1 = s"${temp.getCanonicalPath}/symlink_orc1.txt"
      val testSymlinkPath2 = s"${temp.getCanonicalPath}/symlink_orc2.txt"
      val prefix = System.getProperty("user.dir")
      val testData1 = s"file://$prefix/src/test/resources/data/files/sample1.snappy.orc\n"
      val testData2 = s"file://$prefix/src/test/resources/data/files/sample2.snappy.orc\n"
      val writer1 = new PrintWriter(new FileWriter(testSymlinkPath1, false))
      val writer2 = new PrintWriter(new FileWriter(testSymlinkPath2, false))
      writer1.write(testData1)
      writer2.write(testData2)
      writer1.close()
      writer2.close()

      val load1 = s"LOAD DATA LOCAL INPATH '${testSymlinkPath1}' " +
        "INTO TABLE symlink_orc_partitioned_nonlazy PARTITION (dt=20200726)"
      val load2 = s"LOAD DATA LOCAL INPATH '${testSymlinkPath2}' " +
        "INTO TABLE symlink_orc_partitioned_nonlazy PARTITION (dt=20200727)"
      spark.sql(load1)
      spark.sql(load2)

      val dml = "SELECT * FROM symlink_orc_partitioned_nonlazy"
      val df = spark.sql(dml)

      df.show()
      assert(df.count() == 3)

      spark.conf.set(SQLConf.HIVE_MANAGE_FILESOURCE_PARTITIONS.key, true)
    }
  }

  test("symlink parquet") {
    withTempDir { temp =>
      val drop = "DROP TABLE IF EXISTS symlink_parquet"
      spark.sql(drop)

      val ddl =
        s"""CREATE TABLE symlink_parquet(
           |a BIGINT,
           |b BIGINT,
           |c BIGINT
           |)
           |ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
           |STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat'
           |OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
                 """.stripMargin
      spark.sql(ddl)

      val testSymlinkPath = s"${temp.getCanonicalPath}/symlink_parquet.txt"
      val prefix = System.getProperty("user.dir")
      val testData1 = s"file://$prefix/src/test/resources/data/files/sample1.snappy.parquet\n"
      val testData2 = s"file://$prefix/src/test/resources/data/files/sample2.snappy.parquet\n"
      val writer = new PrintWriter(new FileWriter(testSymlinkPath, false))
      writer.write(testData1)
      writer.write(testData2)
      writer.close()

      val load = s"LOAD DATA LOCAL INPATH '${testSymlinkPath}' INTO TABLE symlink_parquet"
      spark.sql(load)

      val dml = "SELECT * FROM symlink_parquet"
      val df = spark.sql(dml)

      df.show()
      assert(df.count() == 3)
    }
  }

  test("symlink parquet partitioned") {
    withTempDir { temp =>
      val drop = "DROP TABLE IF EXISTS symlink_parquet_partitioned"
      spark.sql(drop)

      val ddl =
        s"""CREATE TABLE symlink_parquet_partitioned(
           |a BIGINT,
           |b BIGINT,
           |c BIGINT
           |)
           |PARTITIONED BY (dt STRING)
           |ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
           |STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat'
           |OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
                 """.stripMargin
      spark.sql(ddl)

      val prefix = System.getProperty("user.dir")
      val testSymlinkPath1 = s"${temp.getCanonicalPath}/symlink_parquet1.txt"
      val testSymlinkPath2 = s"${temp.getCanonicalPath}/symlink_parquet2.txt"
      val testData1 = s"file://$prefix/src/test/resources/data/files/sample1.snappy.parquet\n"
      val testData2 = s"file://$prefix/src/test/resources/data/files/sample2.snappy.parquet\n"
      val writer1 = new PrintWriter(new FileWriter(testSymlinkPath1, false))
      val writer2 = new PrintWriter(new FileWriter(testSymlinkPath2, false))
      writer1.write(testData1)
      writer2.write(testData2)
      writer1.close()
      writer2.close()

      val load1 = s"LOAD DATA LOCAL INPATH '${testSymlinkPath1}' " +
        "INTO TABLE symlink_parquet_partitioned PARTITION (dt=20200726)"
      val load2 = s"LOAD DATA LOCAL INPATH '${testSymlinkPath2}' " +
        "INTO TABLE symlink_parquet_partitioned PARTITION (dt=20200727)"
      spark.sql(load1)
      spark.sql(load2)

      val dml = "SELECT * FROM symlink_parquet_partitioned"
      val df = spark.sql(dml)

      df.show()
      assert(df.count() == 3)
    }
  }
}
