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

import java.nio.file.{Files, Paths}

import org.apache.spark.SparkClassNotFoundException
import org.apache.spark.connect.proto
import org.apache.spark.sql.{AnalysisException, SaveMode}
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.connect.command.{InvalidCommandInput, SparkConnectCommandPlanner}
import org.apache.spark.sql.connect.dsl.commands._
import org.apache.spark.sql.test.{SharedSparkSession, SQLTestUtils}

class SparkConnectCommandPlannerSuite
    extends SQLTestUtils
    with SparkConnectPlanTest
    with SharedSparkSession {

  lazy val localRelation = createLocalRelationProto(Seq($"id".int))

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
    val cmd = localRelation.write(
      tableName = Some("testtable"),
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
    withTempDir { f =>
      val cmd = localRelation.write(
        format = Some("parquet"),
        path = Some(f.getPath),
        mode = Some("Overwrite"))
      transform(cmd)
      assert(Files.exists(Paths.get(f.getPath)), s"Output file must exist: ${f.getPath}")
    }
  }

  test("Write to Path with invalid input") {
    // Wrong data source.
    assertThrows[SparkClassNotFoundException](
      transform(
        localRelation.write(path = Some("/tmp/tmppath"), format = Some("ThisAintNoFormat"))))

    // Default data source not found.
    assertThrows[SparkClassNotFoundException](
      transform(localRelation.write(path = Some("/tmp/tmppath"))))
  }

  test("Write with sortBy") {
    // Sort by existing column.
    withTable("testtable") {
      transform(
        localRelation.write(
          tableName = Some("testtable"),
          format = Some("parquet"),
          sortByColumns = Seq("id"),
          bucketByCols = Seq("id"),
          numBuckets = Some(10)))
    }

    // Sort by non-existing column
    assertThrows[AnalysisException](
      transform(
        localRelation
          .write(
            tableName = Some("testtable"),
            format = Some("parquet"),
            sortByColumns = Seq("noid"),
            bucketByCols = Seq("id"),
            numBuckets = Some(10))))
  }

  test("Write to Table") {
    withTable("testtable") {
      val cmd = localRelation.write(format = Some("parquet"), tableName = Some("testtable"))
      transform(cmd)
      // Check that we can find and drop the table.
      spark.sql(s"select count(*) from testtable").collect()
    }
  }

  test("SaveMode conversion tests") {
    assertThrows[IllegalArgumentException](
      DataTypeProtoConverter.toSaveMode(proto.WriteOperation.SaveMode.SAVE_MODE_UNSPECIFIED))

    val combinations = Seq(
      (SaveMode.Append, proto.WriteOperation.SaveMode.SAVE_MODE_APPEND),
      (SaveMode.Ignore, proto.WriteOperation.SaveMode.SAVE_MODE_IGNORE),
      (SaveMode.Overwrite, proto.WriteOperation.SaveMode.SAVE_MODE_OVERWRITE),
      (SaveMode.ErrorIfExists, proto.WriteOperation.SaveMode.SAVE_MODE_ERROR_IF_EXISTS))
    combinations.foreach { a =>
      assert(DataTypeProtoConverter.toSaveModeProto(a._1) == a._2)
      assert(DataTypeProtoConverter.toSaveMode(a._2) == a._1)
    }
  }

}
