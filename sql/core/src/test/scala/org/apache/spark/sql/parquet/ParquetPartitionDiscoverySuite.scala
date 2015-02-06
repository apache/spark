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
package org.apache.spark.sql.parquet

import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.fs.Path
import org.scalatest.FunSuite

import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.parquet.ParquetRelation2._
import org.apache.spark.sql.test.TestSQLContext
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}

class ParquetPartitionDiscoverySuite extends FunSuite with ParquetTest {
  override val sqlContext: SQLContext = TestSQLContext

  val defaultPartitionName = "__NULL__"

  test("column type inference") {
    def check(raw: String, literal: Literal): Unit = {
      assert(inferPartitionColumnValue(raw, defaultPartitionName) === literal)
    }

    check("10", Literal(10, IntegerType))
    check("1000000000000000", Literal(1000000000000000L, LongType))
    check("1.5", Literal(1.5, FloatType))
    check("hello", Literal("hello", StringType))
    check(defaultPartitionName, Literal(null, NullType))
  }

  test("parse partition") {
    def check(path: String, expected: PartitionValues): Unit = {
      assert(expected === parsePartition(new Path(path), defaultPartitionName))
    }

    def checkThrows[T <: Throwable: Manifest](path: String, expected: String): Unit = {
      val message = intercept[T] {
        parsePartition(new Path(path), defaultPartitionName)
      }.getMessage

      assert(message.contains(expected))
    }

    check(
      "file:///",
      PartitionValues(
        ArrayBuffer.empty[String],
        ArrayBuffer.empty[Literal]))

    check(
      "file://path/a=10",
      PartitionValues(
        ArrayBuffer("a"),
        ArrayBuffer(Literal(10, IntegerType))))

    check(
      "file://path/a=10/b=hello/c=1.5",
      PartitionValues(
        ArrayBuffer("a", "b", "c"),
        ArrayBuffer(
          Literal(10, IntegerType),
          Literal("hello", StringType),
          Literal(1.5, FloatType))))

    check(
      "file://path/a=10/b_hello/c=1.5",
      PartitionValues(
        ArrayBuffer("c"),
        ArrayBuffer(Literal(1.5, FloatType))))

    checkThrows[AssertionError]("file://path/=10", "Empty partition column name")
    checkThrows[AssertionError]("file://path/a=", "Empty partition column value")
  }

  test("parse partitions") {
    def check(paths: Seq[String], spec: PartitionSpec): Unit = {
      assert(parsePartitions(paths.map(new Path(_)), defaultPartitionName) === spec)
    }

    check(Seq(
      "hdfs://host:9000/path/a=10/b=hello"),
      PartitionSpec(
        StructType(Seq(
          StructField("a", IntegerType),
          StructField("b", StringType))),
        Seq(Partition(Row(10, "hello"), "hdfs://host:9000/path/a=10/b=hello"))))

    check(Seq(
      "hdfs://host:9000/path/a=10/b=20",
      "hdfs://host:9000/path/a=10.5/b=hello"),
      PartitionSpec(
        StructType(Seq(
          StructField("a", FloatType),
          StructField("b", StringType))),
        Seq(
          Partition(Row(10, "20"), "hdfs://host:9000/path/a=10/b=20"),
          Partition(Row(10.5, "hello"), "hdfs://host:9000/path/a=10.5/b=hello"))))

    check(Seq(
      s"hdfs://host:9000/path/a=10/b=$defaultPartitionName",
      s"hdfs://host:9000/path/a=10.5/b=$defaultPartitionName"),
      PartitionSpec(
        StructType(Seq(
          StructField("a", FloatType),
          StructField("b", StringType))),
        Seq(
          Partition(Row(10, null), s"hdfs://host:9000/path/a=10/b=$defaultPartitionName"),
          Partition(Row(10.5, null), s"hdfs://host:9000/path/a=10.5/b=$defaultPartitionName"))))
  }
}
