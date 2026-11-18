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

package org.apache.spark.sql.internal.connector

import scala.annotation.nowarn

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.connector.expressions.NamedReference
import org.apache.spark.sql.connector.read.{InputPartition, Scan}

/**
 * A mix-in interface for [[Scan]]. Data sources can implement this interface if they can
 * plan their [[InputPartition]]s using Catalyst [[Expression]]s Spark infers at runtime.
 * A scan must not implement this interface together with
 * [[org.apache.spark.sql.connector.read.SupportsRuntimeV2Filtering]] or its subinterface
 * [[org.apache.spark.sql.connector.read.SupportsRuntimeFiltering]]; Spark rejects such a scan.
 *
 * Spark considers a runtime predicate fully pushed when all attributes referenced by the
 * predicate are returned by [[fullyPushedFilterAttributes]]. Fully pushed predicates are not
 * evaluated again after the scan.
 *
 * Note that Spark will push runtime filters only if they are beneficial.
 */
trait SupportsRuntimeCatalystFiltering extends Scan {

  /**
   * Returns attributes this scan can be filtered by at runtime.
   *
   * Spark hands the runtime filters it derives for any of these attributes to
   * [[planInputPartitionsWithRuntimeFilters]]. A scan whose classes do not declare that method gets
   * them through the deprecated [[filter]] instead (see [[filter]]). Each reference must resolve
   * against the scan relation output when Spark builds it. Attributes pruned out of
   * [[Scan.readSchema]] fail to resolve.
   *
   * The scan may prune conservatively and return rows that fail a runtime filter: the join a
   * dynamic partition pruning filter was derived from drops them, a row-level operation evaluates
   * its own condition on each row it reads, and the `FilterExec` above the scan drops them unless
   * the filter is fully pushed (see [[fullyPushedFilterAttributes]]). It must never drop a row
   * that satisfies the filter.
   */
  def filterAttributes(): Array[NamedReference]

  /**
   * Returns attributes for which this scan fully evaluates runtime predicates.
   *
   * Any runtime predicate that references only attributes in this set is considered fully pushed
   * and will not be evaluated again after the scan. These attributes must also be returned by
   * [[filterAttributes]]. Each attribute's value must therefore be fixed within every
   * [[org.apache.spark.sql.connector.read.InputPartition]] the scan returns, since pruning
   * partitions cannot fully evaluate a predicate on a column that varies within a partition.
   *
   * Spark relies on the scan alone here, so the scan must return only partitions it has proven
   * satisfy such a predicate. Spark passes these expressions through as they are, leaving both
   * translation and capability checking to the scan, so evaluating one can fail, for example on
   * an ANSI cast or overflow error, or on a nested access that the scan matches differently
   * against its partition layout. Declare an attribute only when the scan can evaluate every
   * predicate over it.
   *
   * Each reference must be a top-level attribute present in [[Scan.readSchema]]. Nested
   * references are rejected, and attributes pruned out of the read schema fail to resolve, when
   * Spark builds the scan relation. Spark cannot currently represent an individual fully pushed
   * nested path. A scan must not return the root struct as a substitute unless it can fully
   * evaluate predicates over every nested field, since Spark would remove their post-scan
   * evaluation as well.
   */
  def fullyPushedFilterAttributes(): Array[NamedReference] = Array.empty

  /**
   * Plans this scan's input partitions with runtime Catalyst expressions applied.
   *
   * The provided expressions must be interpreted as a set of predicates that are ANDed together,
   * and applied on top of the filters already pushed at query compilation. Implementations may use
   * them to plan fewer [[InputPartition]]s than [[org.apache.spark.sql.connector.read.Batch]]
   * would without them.
   *
   * Spark calls this method at execution time for every scan node that has runtime filters, with
   * the ones that survive screening, unless the scan's classes do not declare it (see [[filter]]).
   * The array may be empty, which applies no runtime filter. The partitions it returns replace the
   * ones that scan node would otherwise have read. Spark may also call
   * `Batch.planInputPartitions()` on the same scan, so neither method may assume it is the only one
   * used. One scan instance can back several scan nodes (e.g. the two branches of a group-based
   * UPDATE), each with its own runtime filters, so an implementation must derive its result from
   * the given expressions alone and must not carry state over from a previous call. A group-based
   * scan whose write replaces the groups that were read records what each call returned; a scan
   * node without runtime filters reads what `planInputPartitions()` returns.
   *
   * Spark currently tracks runtime-filter eligibility by root attribute. If [[filterAttributes]]
   * returns a nested reference, an expression may access another nested field under the same root.
   * The scan must match each access against its own partition layout and use only expressions it
   * can apply.
   *
   * If the scan also implements
   * [[org.apache.spark.sql.connector.read.SupportsReportPartitioning]], it must preserve
   * the originally reported partitioning. While applying runtime predicates, the scan may detect
   * that some [[InputPartition]]s have no matching data, in which case it can either replace
   * them with empty [[InputPartition]]s, or report only a subset of the original partition
   * values (omitting those with no data). The scan must not report new partition values that
   * were not present in the original partitioning.
   *
   * The default calls the deprecated [[filter]] when `expressions` is not empty and then returns
   * `toBatch.planInputPartitions()`, which is what Spark 4.3 did when a runtime filter survived
   * screening. Like `filter` in Spark 4.3, successive calls on such a scan are additive.
   */
  @nowarn("cat=deprecation")
  def planInputPartitionsWithRuntimeFilters(
      expressions: Array[Expression]): Array[InputPartition] = {
    if (expressions.nonEmpty) filter(expressions)
    toBatch.planInputPartitions()
  }

  /**
   * Filters this scan with runtime Catalyst expressions, the method Spark 4.3 called. A scan whose
   * classes do not declare [[planInputPartitionsWithRuntimeFilters]], such as one built against
   * Spark 4.3 or a Java class that overrides only this, still gets its screened runtime filters
   * here, as in Spark 4.3, and reads the partitions it planned when screening removes every runtime
   * filter, provided the classes named in its public method signatures can be loaded. A Scala scan
   * compiled against this version gets a forwarder for that method, so Spark takes it for a new
   * scan and calls the default, which calls this method when a filter survives screening but plans
   * the scan again when screening removes every runtime filter. Such a scan should implement
   * [[planInputPartitionsWithRuntimeFilters]], as a new scan does.
   */
  @deprecated("Implement planInputPartitionsWithRuntimeFilters instead", "4.4.0")
  def filter(expressions: Array[Expression]): Unit =
    throw SparkException.internalError(
      s"${getClass.getName} must implement planInputPartitionsWithRuntimeFilters")
}
