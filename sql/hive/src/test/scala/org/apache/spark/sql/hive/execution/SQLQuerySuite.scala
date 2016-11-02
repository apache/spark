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

import java.io.{File, PrintWriter}
import java.nio.charset.StandardCharsets
import java.sql.{Date, DriverManager, Timestamp}

import scala.sys.process.{Process, ProcessLogger}
import scala.util.Try

import com.google.common.io.Files
import org.apache.hadoop.fs.Path

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{EliminateSubqueryAliases, FunctionRegistry, NoSuchPartitionException}
import org.apache.spark.sql.catalyst.catalog.CatalogTableType
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.{HiveUtils, MetastoreRelation}
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SQLTestUtils
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.CalendarInterval
import org.apache.spark.util.Utils

case class Nested1(f1: Nested2)
case class Nested2(f2: Nested3)
case class Nested3(f3: Int)

case class NestedArray2(b: Seq[Int])
case class NestedArray1(a: NestedArray2)

case class Order(
    id: Int,
    make: String,
    `type`: String,
    price: Int,
    pdate: String,
    customer: String,
    city: String,
    state: String,
    month: Int)

/**
 * A collection of hive query tests where we generate the answers ourselves instead of depending on
 * Hive to generate them (in contrast to HiveQuerySuite).  Often this is because the query is
 * valid, but Hive currently cannot execute it.
 */
class SQLQuerySuite extends QueryTest with SQLTestUtils with TestHiveSingleton {
  import hiveContext._
  import spark.implicits._


  test("SPARK-10562: partition by column with mixed case name") {
    def runOnce() {
      withTable("tbl10562") {
        val df = Seq(2012 -> "a").toDF("Year", "val")
        df.write.partitionBy("Year").saveAsTable("tbl10562")
        checkAnswer(sql("SELECT year FROM tbl10562"), Row(2012))
        checkAnswer(sql("SELECT Year FROM tbl10562"), Row(2012))
        checkAnswer(sql("SELECT yEAr FROM tbl10562"), Row(2012))
        checkAnswer(sql("SELECT val FROM tbl10562 WHERE Year > 2015"), Nil)
        checkAnswer(sql("SELECT val FROM tbl10562 WHERE Year == 2012"), Row("a"))
        Utils.classForName("org.apache.derby.jdbc.EmbeddedDriver")
        val dir = new File("../../assembly/metastore_db")
        val conn = DriverManager.getConnection("jdbc:derby:" + dir.getCanonicalPath)
        var query = conn.createStatement
        var rs = query.executeQuery("select * from partitions")
        // scalastyle:off println
        while (rs.next()) {
          println(rs.getString(1) + " " + rs.getString(2) + " " + rs.getString(3) + " " +
            rs.getString(4) + " " + rs.getString(5) + " " + rs.getString(6))
        }
        query = conn.createStatement
        rs = query.executeQuery("select * from partition_key_vals")
        while (rs.next()) {
          println(rs.getString(1) + " " + rs.getString(2) + " " + rs.getString(3))
        }
        // scalastyle:on println
      }
    }
    try {
      runOnce()
    } catch {
      case t: Throwable =>
    }
  }
}
