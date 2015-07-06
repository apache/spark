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

package org.apache.spark.sql.catalyst

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.expressions.{Ascending, SortOrder}
import org.apache.spark.sql.catalyst.plans.physical._

/* Implicit conversions */
import org.apache.spark.sql.catalyst.dsl.expressions._

class DistributionSuite extends SparkFunSuite {

  protected def checkGap(
      inputPartitioning: Partitioning,
      requiredDistribution: Distribution,
      expectedGap: Gap) {
    val gap = inputPartitioning.gap(requiredDistribution)
    if (expectedGap != gap) {
      fail(
        s"""
        |== Input Partitioning ==
        |$inputPartitioning
        |== Required Distribution ==
        |$requiredDistribution
        |== Does input partitioning satisfy required distribution? ==
        |Expected $expectedGap got $gap
        """.stripMargin)
    }
  }

  test("UnspecifiedDistribution is the required distribution") {
    checkGap(
      HashPartition(Seq('a, 'b, 'c)),
      UnspecifiedDistribution,
      NoGap)

    checkGap(
      SinglePartition(),
      UnspecifiedDistribution,
      NoGap)

    checkGap(
      RangePartition(Seq(SortOrder('b, Ascending), SortOrder('c, Ascending))),
      UnspecifiedDistribution,
      NoGap)

    checkGap(
      HashPartition(Seq('a, 'b, 'c)),
      UnspecifiedDistribution,
      NoGap)

    checkGap(
      HashPartition(Seq('a, 'b, 'c)).withAdditionalNullClusterKeyGenerated(false),
      UnspecifiedDistribution,
      NoGap)
  }

  test("ClusteredDistribution is the required distribution") {
    checkGap(
      UnknownPartitioning,
      ClusteredDistribution(Seq('a, 'b, 'c), true),
      RepartitionKey(Seq('a, 'b, 'c)))

    checkGap(
      HashPartition(Seq('a, 'b, 'c)).withAdditionalNullClusterKeyGenerated(true),
      ClusteredDistribution(Seq('a, 'b, 'c), nullKeysSensitive = true),
      RepartitionKey(Seq('a, 'b, 'c)))

    checkGap(
      HashPartition(Seq('a, 'b, 'c)).withAdditionalNullClusterKeyGenerated(false),
      ClusteredDistribution(Seq('a, 'b, 'c), nullKeysSensitive = true),
      NoGap)

    checkGap(
      HashPartition(Seq('a, 'b, 'c)).withAdditionalNullClusterKeyGenerated(false),
      ClusteredDistribution(Seq('a, 'b, 'c), nullKeysSensitive = false),
      NoGap)

    checkGap(
      HashPartition(Seq('a, 'b, 'c)).withAdditionalNullClusterKeyGenerated(false),
      ClusteredDistribution(Seq('a, 'b, 'c),
        nullKeysSensitive = true,
        Seq(SortOrder('b, Ascending), SortOrder('c, Ascending))),
      SortKeyWithinPartition(Seq(SortOrder('b, Ascending), SortOrder('c, Ascending))))

    checkGap(
      HashPartition(Seq('a, 'b, 'c)).withAdditionalNullClusterKeyGenerated(false),
      ClusteredDistribution(Seq('a, 'b, 'c),
        nullKeysSensitive = false,
        Seq(SortOrder('b, Ascending), SortOrder('c, Ascending))),
      SortKeyWithinPartition(Seq(SortOrder('b, Ascending), SortOrder('c, Ascending))))

    checkGap(
      HashPartition(Seq('a, 'b)).withAdditionalNullClusterKeyGenerated(false),
      ClusteredDistribution(Seq('a, 'b),
        nullKeysSensitive = false,
        Seq(SortOrder('b, Ascending), SortOrder('a, Ascending))),
      SortKeyWithinPartition(Seq(SortOrder('b, Ascending), SortOrder('a, Ascending))))

    checkGap(
      HashPartition(Seq('a, 'b)).withAdditionalNullClusterKeyGenerated(false),
      ClusteredDistribution(Seq('b, 'a),
        nullKeysSensitive = false,
        Seq(SortOrder('a, Ascending), SortOrder('b, Ascending))),
      RepartitionKeyAndSort(Seq('b, 'a), Seq(SortOrder('a, Ascending), SortOrder('b, Ascending))))

    checkGap(
      HashPartitionWithSort(Seq('b, 'c), Seq(SortOrder('b, Ascending), SortOrder('c, Ascending))),
      ClusteredDistribution(Seq('b, 'c), nullKeysSensitive = false),
      NoGap)

    checkGap(
      HashPartitionWithSort(Seq('b, 'c), Seq(SortOrder('b, Ascending), SortOrder('c, Ascending))),
      ClusteredDistribution(Seq('b, 'c),
        nullKeysSensitive = false,
        Seq(SortOrder('b, Ascending), SortOrder('c, Ascending))),
      NoGap)

    checkGap(
      HashPartitionWithSort(Seq('b, 'c), Seq(SortOrder('b, Ascending), SortOrder('c, Ascending))),
      ClusteredDistribution(Seq('c, 'd),
        nullKeysSensitive = false,
        Seq(SortOrder('e, Ascending), SortOrder('f, Ascending))),
      RepartitionKeyAndSort(Seq('c, 'd), Seq(SortOrder('e, Ascending), SortOrder('f, Ascending))))

    checkGap(
      HashPartitionWithSort(Seq('b, 'c), Seq(SortOrder('b, Ascending), SortOrder('c, Ascending)))
        .withAdditionalNullClusterKeyGenerated(true),
      ClusteredDistribution(Seq('b, 'c), nullKeysSensitive = true),
      RepartitionKey(Seq('b, 'c)))

    checkGap(
      HashPartitionWithSort(Seq('b, 'c),
        Seq(SortOrder('b, Ascending), SortOrder('c, Ascending)))
        .withAdditionalNullClusterKeyGenerated(false),
      ClusteredDistribution(Seq('b, 'c), nullKeysSensitive = true),
      NoGap)

    checkGap(
      HashPartitionWithSort(Seq('c, 'b),
        Seq(SortOrder('b, Ascending), SortOrder('c, Ascending)))
        .withAdditionalNullClusterKeyGenerated(false),
      ClusteredDistribution(Seq('d, 'e), nullKeysSensitive = true),
      RepartitionKey(Seq('d, 'e)))

    checkGap(
      HashPartitionWithSort(Seq('b, 'c),
        Seq(SortOrder('b, Ascending),
          SortOrder('c, Ascending))).withAdditionalNullClusterKeyGenerated(false),
      ClusteredDistribution(Seq('b, 'c),
        nullKeysSensitive = true,
        Seq(SortOrder('b, Ascending), SortOrder('c, Ascending))),
      NoGap)
  }

  test("RangePartitioning is the required distribution") {
    checkGap(
      UnknownPartitioning,
      OrderedDistribution(Seq(SortOrder('b, Ascending), SortOrder('c, Ascending))),
      GlobalOrdering(Seq(SortOrder('b, Ascending), SortOrder('c, Ascending))))

    checkGap(
      SinglePartition(),
      OrderedDistribution(Seq(SortOrder('b, Ascending), SortOrder('c, Ascending))),
      GlobalOrdering(Seq(SortOrder('b, Ascending), SortOrder('c, Ascending))))

    checkGap(
      HashPartition(Seq('b, 'c)).withAdditionalNullClusterKeyGenerated(false),
      OrderedDistribution(Seq(SortOrder('b, Ascending), SortOrder('c, Ascending))),
      GlobalOrdering(Seq(SortOrder('b, Ascending), SortOrder('c, Ascending))))

    checkGap(
      HashPartition(Seq('b, 'c)),
      OrderedDistribution(Seq(SortOrder('b, Ascending), SortOrder('c, Ascending))),
      GlobalOrdering(Seq(SortOrder('b, Ascending), SortOrder('c, Ascending))))

    checkGap(
      HashPartitionWithSort(Seq('b, 'c),
        Seq(SortOrder('b, Ascending), SortOrder('c, Ascending)))
        .withAdditionalNullClusterKeyGenerated(false),
      OrderedDistribution(Seq(SortOrder('b, Ascending), SortOrder('c, Ascending))),
      GlobalOrdering(Seq(SortOrder('b, Ascending), SortOrder('c, Ascending))))

    checkGap(
      RangePartition(Seq(SortOrder('b, Ascending), SortOrder('c, Ascending))),
      OrderedDistribution(Seq(SortOrder('b, Ascending), SortOrder('c, Ascending))),
      NoGap)

    checkGap(
      RangePartition(Seq(SortOrder('c, Ascending), SortOrder('b, Ascending))),
      OrderedDistribution(Seq(SortOrder('b, Ascending), SortOrder('c, Ascending))),
      GlobalOrdering(Seq(SortOrder('b, Ascending), SortOrder('c, Ascending))))
  }
}
