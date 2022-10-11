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

package org.apache.spark.sql.connect.planner

import java.io.File
import java.nio.file.{Files, Paths}
import java.util.UUID

import org.apache.commons.io.FileUtils

import org.apache.spark.{SparkClassNotFoundException, SparkFunSuite}
import org.apache.spark.connect.proto
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.connect.command.{InvalidCommandInput, SparkConnectCommandPlanner}
import org.apache.spark.sql.connect.dsl.commands._
import org.apache.spark.sql.test.SharedSparkSession

class SparkConnectCommandPlannerSuite
    extends SparkFunSuite
    with SparkConnectPlanTest
    with SharedSparkSession {

  lazy val localRelation = createLocalRelationProto(Seq($"id".int))

  /**
   * Returns a unique path name on every invocation.
   * @return
   */
  private def path(): String = s"/tmp/${UUID.randomUUID()}"

  /**
   * Returns a unique valid table name indentifier on each invocation.
   * @return
   */
  private def table(): String = s"table${UUID.randomUUID().toString.replace("-", "")}"

  /**
   * Helper method that takes a closure as an argument to handle cleanup of the resource created.
   * @param thunk
   */
  def withTable(thunk: String => Any): Unit = {
    val name = table()
    thunk(name)
    spark.sql(s"drop table if exists ${name}")
  }

  /**
   * Helper method that takes a closure as an arugment and handles cleanup of the file system
   * resource created.
   * @param thunk
   */
  def withPath(thunk: String => Any): Unit = {
    val name = path()
    thunk(name)
    FileUtils.deleteDirectory(new File(name))
  }

  def transform(cmd: proto.Command): Unit = {
    new SparkConnectCommandPlanner(spark, cmd).process()
  }

  test("Writes fails without path or table") {
    assertThrows[UnsupportedOperationException] {
      transform(localRelation.write())
    }
  }

  test("Write fails with unknown table - AnalysisException") {
    val cmd = readRel.write(tableName = Some("dest"))
    assertThrows[AnalysisException] {
      transform(cmd)
    }
  }

  test("Write with partitions") {
    val name = table()
    val cmd = localRelation.write(
      tableName = Some(name),
      format = Some("parquet"),
      partitionByCols = Seq("noid"))
    assertThrows[AnalysisException] {
      transform(cmd)
    }
  }

  test("Write with invalid bucketBy configuration") {
    val cmd = localRelation.write(bucketByCols = Seq("id"), numBuckets = Some(0))
    assertThrows[InvalidCommandInput] {
      transform(cmd)
    }
  }

  test("Write to Path") {
    withPath { name =>
      val cmd = localRelation.write(format = Some("parquet"), path = Some(name))
      transform(cmd)
      assert(Files.exists(Paths.get(name)), s"Output file must exist: ${name}")
    }
  }

  test("Write to Path with invalid input") {
    // Wrong data source.
    assertThrows[SparkClassNotFoundException](
      transform(localRelation.write(path = Some(path), format = Some("ThisAintNoFormat"))))

    // Default data source not found.
    assertThrows[SparkClassNotFoundException](transform(localRelation.write(path = Some(path))))
  }

  test("Write with sortBy") {
    // Sort by existing column.
    transform(
      localRelation.write(
        tableName = Some(table),
        format = Some("parquet"),
        sortByColumns = Seq("id"),
        bucketByCols = Seq("id"),
        numBuckets = Some(10)))

    // Sort by non-existing column
    assertThrows[AnalysisException](
      transform(
        localRelation
          .write(
            tableName = Some(table),
            format = Some("parquet"),
            sortByColumns = Seq("noid"),
            bucketByCols = Seq("id"),
            numBuckets = Some(10))))
  }

  test("Write to Table") {
    withTable { name =>
      val cmd = localRelation.write(format = Some("parquet"), tableName = Some(name))
      transform(cmd)
      // Check that we can find and drop the table.
      spark.sql(s"select count(*) from ${name}").collect()
    }
  }
}
