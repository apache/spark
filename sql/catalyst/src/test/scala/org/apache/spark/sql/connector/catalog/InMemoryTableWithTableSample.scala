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

package org.apache.spark.sql.connector.catalog

import java.util
import java.util.Locale

import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.connector.join.JoinType
import org.apache.spark.sql.connector.read.{InputPartition, SampleMethod, Scan, ScanBuilder, SupportsPushDownJoin, SupportsPushDownTableSample, SupportsPushDownV2Filters}
import org.apache.spark.sql.connector.read.SupportsPushDownJoin.ColumnWithAlias
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.util.ArrayImplicits._

/**
 * An in-memory table that supports TABLESAMPLE pushdown (both BERNOULLI and SYSTEM).
 *
 * For SYSTEM sampling, entire splits (InputPartitions) are included or skipped based on
 * a hash of their index and the seed. For BERNOULLI sampling, the pushdown is accepted
 * but rows are not actually filtered (Spark's row-level Sample operator handles it).
 */
class InMemoryTableWithTableSample(
    name: String,
    columns: Array[Column],
    partitioning: Array[Transform],
    properties: util.Map[String, String])
  extends InMemoryBaseTable(name, columns, partitioning, properties) {

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
    InMemoryBaseTable.maybeSimulateFailedTableWrite(new CaseInsensitiveStringMap(properties))
    InMemoryBaseTable.maybeSimulateFailedTableWrite(info.options)
    new InMemoryWriterBuilder(info) {
      override def truncate(): WriteBuilder = {
        writer = new TruncateAndAppend(this.info)
        streamingWriter = new StreamingTruncateAndAppend(this.info)
        this
      }
    }
  }

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    new InMemoryTableSampleScanBuilder(schema, options)
  }

  class InMemoryTableSampleScanBuilder(
      tableSchema: StructType,
      options: CaseInsensitiveStringMap)
    extends InMemoryScanBuilder(tableSchema, options) with SupportsPushDownTableSample {

    private var sampleFraction: Double = 1.0
    private var sampleSeed: Long = 0L
    private var sampleMethod: SampleMethod = SampleMethod.BERNOULLI
    private var sampleWithReplacement: Boolean = false
    private var samplePushed: Boolean = false

    override def pushTableSample(
        lowerBound: Double,
        upperBound: Double,
        withReplacement: Boolean,
        seed: Long): Boolean = {
      this.sampleFraction = upperBound - lowerBound
      this.sampleSeed = seed
      this.sampleMethod = SampleMethod.BERNOULLI
      this.sampleWithReplacement = withReplacement
      this.samplePushed = true
      true
    }

    override def pushTableSample(
        lowerBound: Double,
        upperBound: Double,
        withReplacement: Boolean,
        seed: Long,
        sampleMethod: SampleMethod): Boolean = {
      this.sampleFraction = upperBound - lowerBound
      this.sampleSeed = seed
      this.sampleMethod = sampleMethod
      this.sampleWithReplacement = withReplacement
      this.samplePushed = true
      true
    }

    override def build: Scan = {
      val allPartitions = data.map(_.asInstanceOf[InputPartition]).toImmutableArraySeq
      val filteredPartitions = if (samplePushed && sampleMethod == SampleMethod.SYSTEM) {
        // SYSTEM sampling: include/skip entire splits based on hash of index + seed
        allPartitions.zipWithIndex.filter { case (_, idx) =>
          val hash = ((idx.toLong * 31 + sampleSeed) & Long.MaxValue).toDouble / Long.MaxValue
          hash < sampleFraction
        }.map(_._1)
      } else {
        allPartitions
      }
      if (samplePushed) {
        new InMemoryBatchScanWithSample(
          filteredPartitions, schema, tableSchema, options,
          sampleFraction, sampleSeed, sampleMethod, sampleWithReplacement)
      } else {
        InMemoryBatchScan(filteredPartitions, schema, tableSchema, options)
      }
    }
  }

  private class InMemoryBatchScanWithSample(
      data: Seq[InputPartition],
      readSchema: StructType,
      tableSchema: StructType,
      options: CaseInsensitiveStringMap,
      sampleFraction: Double,
      sampleSeed: Long,
      sampleMethod: SampleMethod,
      sampleWithReplacement: Boolean)
    extends InMemoryBatchScan(data, readSchema, tableSchema, options) {

    override def description(): String = {
      val pct = sampleFraction * 100
      val method = sampleMethod.toString.toUpperCase(Locale.ROOT)
      s"${super.description()} $method SAMPLE ($pct) $sampleWithReplacement SEED($sampleSeed)"
    }
  }
}

/**
 * An in-memory table that supports both TABLESAMPLE pushdown and JOIN pushdown.
 * Used to test the guard that prevents join pushdown when a side has a pushed sample.
 */
class InMemoryTableWithJoinAndSample(
    name: String,
    columns: Array[Column],
    partitioning: Array[Transform],
    properties: util.Map[String, String])
  extends InMemoryTableWithTableSample(name, columns, partitioning, properties) {

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    new InMemoryJoinAndSampleScanBuilder(schema, options)
  }

  class InMemoryJoinAndSampleScanBuilder(
      tableSchema: StructType,
      options: CaseInsensitiveStringMap)
    extends InMemoryTableSampleScanBuilder(tableSchema, options)
      with SupportsPushDownJoin with SupportsPushDownV2Filters {

    private[catalog] val ownSchema: StructType = tableSchema
    private var pushed: Array[Predicate] = Array.empty
    private var joinedSchema: Option[StructType] = None

    override def pushPredicates(predicates: Array[Predicate]): Array[Predicate] = {
      pushed = predicates
      // Return empty - all predicates accepted (not actually filtered, just cleared
      // so that the join pushdown pattern's Nil filter requirement is satisfied).
      Array.empty
    }

    // Override V1 pushFilters (inherited from InMemoryScanBuilder) to also accept all
    // filters. PushDownUtils.pushFilters matches SupportsPushDownFilters before
    // SupportsPushDownV2Filters, so without this override isnotnull predicates remain
    // as post-scan Filter nodes and block the join pushdown pattern match.
    override def pushFilters(filters: Array[Filter]): Array[Filter] = {
      Array.empty
    }

    override def pushedPredicates(): Array[Predicate] = pushed

    override def isOtherSideCompatibleForJoin(other: SupportsPushDownJoin): Boolean = true

    override def pushDownJoin(
        other: SupportsPushDownJoin,
        joinType: JoinType,
        leftSideRequiredColumnsWithAliases: Array[ColumnWithAlias],
        rightSideRequiredColumnsWithAliases: Array[ColumnWithAlias],
        condition: Predicate): Boolean = {
      val otherSchema = other.asInstanceOf[InMemoryJoinAndSampleScanBuilder].ownSchema
      val leftFields = leftSideRequiredColumnsWithAliases.map { col =>
        val name = if (col.alias() != null) col.alias() else col.colName()
        tableSchema(col.colName()).copy(name = name)
      }
      val rightFields = rightSideRequiredColumnsWithAliases.map { col =>
        val name = if (col.alias() != null) col.alias() else col.colName()
        otherSchema(col.colName()).copy(name = name)
      }
      joinedSchema = Some(StructType(leftFields ++ rightFields))
      true
    }

    override def build: Scan = {
      joinedSchema match {
        case Some(js) =>
          InMemoryBatchScan(
            data.map(_.asInstanceOf[InputPartition]).toImmutableArraySeq,
            js, tableSchema, options)
        case None => super.build
      }
    }
  }
}

/**
 * An in-memory table that supports TABLESAMPLE pushdown using only the legacy 4-arg
 * pushTableSample method (does NOT override the 5-arg default). Used to test backward
 * compatibility: BERNOULLI should push down via the default delegation, and SYSTEM
 * should fail because the default returns false for SYSTEM.
 */
class InMemoryTableWithLegacyTableSample(
    name: String,
    columns: Array[Column],
    partitioning: Array[Transform],
    properties: util.Map[String, String])
  extends InMemoryBaseTable(name, columns, partitioning, properties) {

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
    InMemoryBaseTable.maybeSimulateFailedTableWrite(new CaseInsensitiveStringMap(properties))
    InMemoryBaseTable.maybeSimulateFailedTableWrite(info.options)
    new InMemoryWriterBuilder(info) {
      override def truncate(): WriteBuilder = {
        writer = new TruncateAndAppend(this.info)
        streamingWriter = new StreamingTruncateAndAppend(this.info)
        this
      }
    }
  }

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    new InMemoryLegacySampleScanBuilder(schema, options)
  }

  class InMemoryLegacySampleScanBuilder(
      tableSchema: StructType,
      options: CaseInsensitiveStringMap)
    extends InMemoryScanBuilder(tableSchema, options) with SupportsPushDownTableSample {

    // Only the 4-arg method is overridden; the 5-arg default method is inherited.
    override def pushTableSample(
        lowerBound: Double,
        upperBound: Double,
        withReplacement: Boolean,
        seed: Long): Boolean = true
  }
}
