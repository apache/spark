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

package org.apache.spark.sql

import scala.collection.mutable

import com.google.common.collect.ImmutableMap
import org.apache.hadoop.fs.{FileStatus, Path}

import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions.{And, Expression, IsNull, LessThan}
import org.apache.spark.sql.execution.datasources.{PartitioningAwareFileIndex, PartitionSpec}
import org.apache.spark.sql.execution.datasources.v2.FileScan
import org.apache.spark.sql.execution.datasources.v2.csv.CSVScan
import org.apache.spark.sql.execution.datasources.v2.json.JsonScan
import org.apache.spark.sql.execution.datasources.v2.orc.OrcScan
import org.apache.spark.sql.execution.datasources.v2.parquet.ParquetScan
import org.apache.spark.sql.execution.datasources.v2.text.TextScan
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

trait FileScanSuiteBase extends SharedSparkSession {
  private def newPartitioningAwareFileIndex() = {
    new PartitioningAwareFileIndex(spark, Map.empty, None) {
      override def partitionSpec(): PartitionSpec = {
        PartitionSpec.emptySpec
      }

      override protected def leafFiles: mutable.LinkedHashMap[Path, FileStatus] = {
        mutable.LinkedHashMap.empty
      }

      override protected def leafDirToChildrenFiles: Map[Path, Array[FileStatus]] = {
        Map.empty
      }

      override def rootPaths: Seq[Path] = {
        Seq.empty
      }

      override def refresh(): Unit = {}
    }
  }

  type ScanBuilder = (
    SparkSession,
      PartitioningAwareFileIndex,
      StructType,
      StructType,
      StructType,
      Array[Filter],
      CaseInsensitiveStringMap,
      Seq[Expression],
      Seq[Expression]) => FileScan

  def run(scanBuilders: Seq[(String, ScanBuilder, Seq[String])]): Unit = {
    val dataSchema = StructType.fromDDL("data INT, partition INT, other INT")
    val dataSchemaNotEqual = StructType.fromDDL("data INT, partition INT, other INT, new INT")
    val readDataSchema = StructType.fromDDL("data INT")
    val readDataSchemaNotEqual = StructType.fromDDL("data INT, other INT")
    val readPartitionSchema = StructType.fromDDL("partition INT")
    val readPartitionSchemaNotEqual = StructType.fromDDL("partition INT, other INT")
    val pushedFilters =
      Array[Filter](sources.And(sources.IsNull("data"), sources.LessThan("data", 0)))
    val pushedFiltersNotEqual =
      Array[Filter](sources.And(sources.IsNull("data"), sources.LessThan("data", 1)))
    val optionsMap = ImmutableMap.of("key", "value")
    val options = new CaseInsensitiveStringMap(ImmutableMap.copyOf(optionsMap))
    val optionsNotEqual =
      new CaseInsensitiveStringMap(ImmutableMap.copyOf(ImmutableMap.of("key2", "value2")))
    val partitionFilters = Seq(And(IsNull('data.int), LessThan('data.int, 0)))
    val partitionFiltersNotEqual = Seq(And(IsNull('data.int), LessThan('data.int, 1)))
    val dataFilters = Seq(And(IsNull('data.int), LessThan('data.int, 0)))
    val dataFiltersNotEqual = Seq(And(IsNull('data.int), LessThan('data.int, 1)))

    scanBuilders.foreach { case (name, scanBuilder, exclusions) =>
      test(s"SPARK-33482: Test $name equals") {
        val partitioningAwareFileIndex = newPartitioningAwareFileIndex()

        val scan = scanBuilder(
          spark,
          partitioningAwareFileIndex,
          dataSchema,
          readDataSchema,
          readPartitionSchema,
          pushedFilters,
          options,
          partitionFilters,
          dataFilters)

        val scanEquals = scanBuilder(
          spark,
          partitioningAwareFileIndex,
          dataSchema.copy(),
          readDataSchema.copy(),
          readPartitionSchema.copy(),
          pushedFilters.clone(),
          new CaseInsensitiveStringMap(ImmutableMap.copyOf(optionsMap)),
          Seq(partitionFilters: _*),
          Seq(dataFilters: _*))

        assert(scan === scanEquals)
      }

      test(s"SPARK-33482: Test $name fileIndex not equals") {
        val partitioningAwareFileIndex = newPartitioningAwareFileIndex()

        val scan = scanBuilder(
          spark,
          partitioningAwareFileIndex,
          dataSchema,
          readDataSchema,
          readPartitionSchema,
          pushedFilters,
          options,
          partitionFilters,
          dataFilters)

        val partitioningAwareFileIndexNotEqual = newPartitioningAwareFileIndex()

        val scanNotEqual = scanBuilder(
          spark,
          partitioningAwareFileIndexNotEqual,
          dataSchema,
          readDataSchema,
          readPartitionSchema,
          pushedFilters,
          options,
          partitionFilters,
          dataFilters)

        assert(scan !== scanNotEqual)
      }

      if (!exclusions.contains("dataSchema")) {
        test(s"SPARK-33482: Test $name dataSchema not equals") {
          val partitioningAwareFileIndex = newPartitioningAwareFileIndex()

          val scan = scanBuilder(
            spark,
            partitioningAwareFileIndex,
            dataSchema,
            readDataSchema,
            readPartitionSchema,
            pushedFilters,
            options,
            partitionFilters,
            dataFilters)

          val scanNotEqual = scanBuilder(
            spark,
            partitioningAwareFileIndex,
            dataSchemaNotEqual,
            readDataSchema,
            readPartitionSchema,
            pushedFilters,
            options,
            partitionFilters,
            dataFilters)

          assert(scan !== scanNotEqual)
        }
      }

      test(s"SPARK-33482: Test $name readDataSchema not equals") {
        val partitioningAwareFileIndex = newPartitioningAwareFileIndex()

        val scan = scanBuilder(
          spark,
          partitioningAwareFileIndex,
          dataSchema,
          readDataSchema,
          readPartitionSchema,
          pushedFilters,
          options,
          partitionFilters,
          dataFilters)

        val scanNotEqual = scanBuilder(
          spark,
          partitioningAwareFileIndex,
          dataSchema,
          readDataSchemaNotEqual,
          readPartitionSchema,
          pushedFilters,
          options,
          partitionFilters,
          dataFilters)

        assert(scan !== scanNotEqual)
      }

      test(s"SPARK-33482: Test $name readPartitionSchema not equals") {
        val partitioningAwareFileIndex = newPartitioningAwareFileIndex()

        val scan = scanBuilder(
          spark,
          partitioningAwareFileIndex,
          dataSchema,
          readDataSchema,
          readPartitionSchema,
          pushedFilters,
          options,
          partitionFilters,
          dataFilters)

        val scanNotEqual = scanBuilder(
          spark,
          partitioningAwareFileIndex,
          dataSchema,
          readDataSchema,
          readPartitionSchemaNotEqual,
          pushedFilters,
          options,
          partitionFilters,
          dataFilters)

        assert(scan !== scanNotEqual)
      }

      if (!exclusions.contains("pushedFilters")) {
        test(s"SPARK-33482: Test $name pushedFilters not equals") {
          val partitioningAwareFileIndex = newPartitioningAwareFileIndex()

          val scan = scanBuilder(
            spark,
            partitioningAwareFileIndex,
            dataSchema,
            readDataSchema,
            readPartitionSchema,
            pushedFilters,
            options,
            partitionFilters,
            dataFilters)

          val scanNotEqual = scanBuilder(
            spark,
            partitioningAwareFileIndex,
            dataSchema,
            readDataSchema,
            readPartitionSchema,
            pushedFiltersNotEqual,
            options,
            partitionFilters,
            dataFilters)

          assert(scan !== scanNotEqual)
        }
      }

      test(s"SPARK-33482: Test $name options not equals") {
        val partitioningAwareFileIndex = newPartitioningAwareFileIndex()

        val scan = scanBuilder(
          spark,
          partitioningAwareFileIndex,
          dataSchema,
          readDataSchema,
          readPartitionSchema,
          pushedFilters,
          options,
          partitionFilters,
          dataFilters)

        val scanNotEqual = scanBuilder(
          spark,
          partitioningAwareFileIndex,
          dataSchema,
          readDataSchema,
          readPartitionSchema,
          pushedFilters,
          optionsNotEqual,
          partitionFilters,
          dataFilters)

        assert(scan !== scanNotEqual)
      }

      test(s"SPARK-33482: Test $name partitionFilters not equals") {
        val partitioningAwareFileIndex = newPartitioningAwareFileIndex()

        val scan = scanBuilder(
          spark,
          partitioningAwareFileIndex,
          dataSchema,
          readDataSchema,
          readPartitionSchema,
          pushedFilters,
          options,
          partitionFilters,
          dataFilters)

        val scanNotEqual = scanBuilder(
          spark,
          partitioningAwareFileIndex,
          dataSchema,
          readDataSchema,
          readPartitionSchema,
          pushedFilters,
          options,
          partitionFiltersNotEqual,
          dataFilters)
        assert(scan !== scanNotEqual)
      }

      test(s"SPARK-33482: Test $name dataFilters not equals") {
        val partitioningAwareFileIndex = newPartitioningAwareFileIndex()

        val scan = scanBuilder(
          spark,
          partitioningAwareFileIndex,
          dataSchema,
          readDataSchema,
          readPartitionSchema,
          pushedFilters,
          options,
          partitionFilters,
          dataFilters)

        val scanNotEqual = scanBuilder(
          spark,
          partitioningAwareFileIndex,
          dataSchema,
          readDataSchema,
          readPartitionSchema,
          pushedFilters,
          options,
          partitionFilters,
          dataFiltersNotEqual)
        assert(scan !== scanNotEqual)
      }
    }
  }
}

class FileScanSuite extends FileScanSuiteBase {
  val scanBuilders = Seq[(String, ScanBuilder, Seq[String])](
    ("ParquetScan",
      (s, fi, ds, rds, rps, f, o, pf, df) =>
        ParquetScan(s, s.sessionState.newHadoopConf(), fi, ds, rds, rps, f, o, None, pf, df),
      Seq.empty),
    ("OrcScan",
      (s, fi, ds, rds, rps, f, o, pf, df) =>
        OrcScan(s, s.sessionState.newHadoopConf(), fi, ds, rds, rps, o, None, f, pf, df),
      Seq.empty),
    ("CSVScan",
      (s, fi, ds, rds, rps, f, o, pf, df) => CSVScan(s, fi, ds, rds, rps, o, f, pf, df),
      Seq.empty),
    ("JsonScan",
      (s, fi, ds, rds, rps, f, o, pf, df) => JsonScan(s, fi, ds, rds, rps, o, f, pf, df),
      Seq.empty),
    ("TextScan",
      (s, fi, ds, rds, rps, _, o, pf, df) => TextScan(s, fi, ds, rds, rps, o, pf, df),
      Seq("dataSchema", "pushedFilters")))

  run(scanBuilders)
}
