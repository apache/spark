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
/* Implicit conversions */
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions.{Literal, Murmur3Hash, Pmod}
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.types.IntegerType

class DistributionSuite extends SparkFunSuite {

  protected def checkSatisfied(
      inputPartitioning: Partitioning,
      requiredDistribution: Distribution,
      satisfied: Boolean): Unit = {
    if (inputPartitioning.satisfies(requiredDistribution) != satisfied) {
      fail(
        s"""
        |== Input Partitioning ==
        |$inputPartitioning
        |== Required Distribution ==
        |$requiredDistribution
        |== Does input partitioning satisfy required distribution? ==
        |Expected $satisfied got ${inputPartitioning.satisfies(requiredDistribution)}
        """.stripMargin)
    }
  }

  test("UnspecifiedDistribution and AllTuples") {
    // all partitioning can satisfy UnspecifiedDistribution
    checkSatisfied(
      UnknownPartitioning(-1),
      UnspecifiedDistribution,
      true)

    checkSatisfied(
      RoundRobinPartitioning(10),
      UnspecifiedDistribution,
      true)

    checkSatisfied(
      SinglePartition,
      UnspecifiedDistribution,
      true)

    checkSatisfied(
      HashPartitioning(Seq($"a"), 10),
      UnspecifiedDistribution,
      true)

    checkSatisfied(
      RangePartitioning(Seq($"a".asc), 10),
      UnspecifiedDistribution,
      true)

    checkSatisfied(
      BroadcastPartitioning(IdentityBroadcastMode),
      UnspecifiedDistribution,
      true)

    // except `BroadcastPartitioning`, all other partitioning can satisfy AllTuples if they have
    // only one partition.
    checkSatisfied(
      UnknownPartitioning(1),
      AllTuples,
      true)

    checkSatisfied(
      UnknownPartitioning(10),
      AllTuples,
      false)

    checkSatisfied(
      RoundRobinPartitioning(1),
      AllTuples,
      true)

    checkSatisfied(
      RoundRobinPartitioning(10),
      AllTuples,
      false)

    checkSatisfied(
      SinglePartition,
      AllTuples,
      true)

    checkSatisfied(
      HashPartitioning(Seq($"a"), 1),
      AllTuples,
      true)

    checkSatisfied(
      HashPartitioning(Seq($"a"), 10),
      AllTuples,
      false)

    checkSatisfied(
      RangePartitioning(Seq($"a".asc), 1),
      AllTuples,
      true)

    checkSatisfied(
      RangePartitioning(Seq($"a".asc), 10),
      AllTuples,
      false)

    checkSatisfied(
      BroadcastPartitioning(IdentityBroadcastMode),
      AllTuples,
      false)
  }

  test("SinglePartition is the output partitioning") {
    // SinglePartition can satisfy all the distributions except `BroadcastDistribution`
    checkSatisfied(
      SinglePartition,
      ClusteredDistribution(Seq($"a", $"b", $"c")),
      true)

    checkSatisfied(
      SinglePartition,
      OrderedDistribution(Seq($"a".asc, $"b".asc, $"c".asc)),
      true)

    checkSatisfied(
      SinglePartition,
      BroadcastDistribution(IdentityBroadcastMode),
      false)
  }

  test("HashPartitioning is the output partitioning") {
    // HashPartitioning can satisfy ClusteredDistribution iff its hash expressions are a subset of
    // the required clustering expressions.
    checkSatisfied(
      HashPartitioning(Seq($"a", $"b", $"c"), 10),
      ClusteredDistribution(Seq($"a", $"b", $"c")),
      true)

    checkSatisfied(
      HashPartitioning(Seq($"b", $"c"), 10),
      ClusteredDistribution(Seq($"a", $"b", $"c")),
      true)

    checkSatisfied(
      HashPartitioning(Seq($"a", $"b", $"c"), 10),
      ClusteredDistribution(Seq($"b", $"c")),
      false)

    checkSatisfied(
      HashPartitioning(Seq($"a", $"b", $"c"), 10),
      ClusteredDistribution(Seq($"d", $"e")),
      false)

    // HashPartitioning cannot satisfy OrderedDistribution
    checkSatisfied(
      HashPartitioning(Seq($"a", $"b", $"c"), 10),
      OrderedDistribution(Seq($"a".asc, $"b".asc, $"c".asc)),
      false)

    checkSatisfied(
      HashPartitioning(Seq($"a", $"b", $"c"), 1),
      OrderedDistribution(Seq($"a".asc, $"b".asc, $"c".asc)),
      false) // TODO: this can be relaxed.

    checkSatisfied(
      HashPartitioning(Seq($"b", $"c"), 10),
      OrderedDistribution(Seq($"a".asc, $"b".asc, $"c".asc)),
      false)
  }

  test("RangePartitioning is the output partitioning") {
    // RangePartitioning can satisfy OrderedDistribution iff its ordering is a prefix
    // of the required ordering, or the required ordering is a prefix of its ordering.
    checkSatisfied(
      RangePartitioning(Seq($"a".asc, $"b".asc, $"c".asc), 10),
      OrderedDistribution(Seq($"a".asc, $"b".asc, $"c".asc)),
      true)

    checkSatisfied(
      RangePartitioning(Seq($"a".asc, $"b".asc, $"c".asc), 10),
      OrderedDistribution(Seq($"a".asc, $"b".asc)),
      true)

    checkSatisfied(
      RangePartitioning(Seq($"a".asc, $"b".asc, $"c".asc), 10),
      OrderedDistribution(Seq($"a".asc, $"b".asc, $"c".asc, $"d".desc)),
      true)

    // TODO: We can have an optimization to first sort the dataset
    // by a.asc and then sort b, and c in a partition. This optimization
    // should tradeoff the benefit of a less number of Exchange operators
    // and the parallelism.
    checkSatisfied(
      RangePartitioning(Seq($"a".asc, $"b".asc, $"c".asc), 10),
      OrderedDistribution(Seq($"a".asc, $"b".desc, $"c".asc)),
      false)

    checkSatisfied(
      RangePartitioning(Seq($"a".asc, $"b".asc, $"c".asc), 10),
      OrderedDistribution(Seq($"b".asc, $"a".asc)),
      false)

    checkSatisfied(
      RangePartitioning(Seq($"a".asc, $"b".asc, $"c".asc), 10),
      OrderedDistribution(Seq($"a".asc, $"b".asc, $"d".desc)),
      false)

    // RangePartitioning can satisfy ClusteredDistribution iff its ordering expressions are a subset
    // of the required clustering expressions.
    checkSatisfied(
      RangePartitioning(Seq($"a".asc, $"b".asc, $"c".asc), 10),
      ClusteredDistribution(Seq($"a", $"b", $"c")),
      true)

    checkSatisfied(
      RangePartitioning(Seq($"a".asc, $"b".asc, $"c".asc), 10),
      ClusteredDistribution(Seq($"c", $"b", $"a")),
      true)

    checkSatisfied(
      RangePartitioning(Seq($"a".asc, $"b".asc, $"c".asc), 10),
      ClusteredDistribution(Seq($"b", $"c", $"a", $"d")),
      true)

    checkSatisfied(
      RangePartitioning(Seq($"a".asc, $"b".asc, $"c".asc), 10),
      ClusteredDistribution(Seq($"a", $"b")),
      false)

    checkSatisfied(
      RangePartitioning(Seq($"a".asc, $"b".asc, $"c".asc), 10),
      ClusteredDistribution(Seq($"c", $"d")),
      false)
  }

  test("Partitioning.numPartitions must match Distribution.requiredNumPartitions to satisfy it") {
    checkSatisfied(
      SinglePartition,
      ClusteredDistribution(Seq($"a", $"b", $"c"), Some(10)),
      false)

    checkSatisfied(
      HashPartitioning(Seq($"a", $"b", $"c"), 10),
      ClusteredDistribution(Seq($"a", $"b", $"c"), Some(5)),
      false)

    checkSatisfied(
      RangePartitioning(Seq($"a".asc, $"b".asc, $"c".asc), 10),
      ClusteredDistribution(Seq($"a", $"b", $"c"), Some(5)),
      false)
  }

  test("Structured Streaming output partitioning and distribution") {
    // Validate HashPartitioning.partitionIdExpression to be exactly expected format, because
    // Structured Streaming state store requires it to be consistent across Spark versions.
    val expressions = Seq($"a", $"b", $"c")
    val hashPartitioning = HashPartitioning(expressions, 10)
    hashPartitioning.partitionIdExpression match {
      case Pmod(Murmur3Hash(es, 42), Literal(10, IntegerType), _) =>
        assert(es.length == expressions.length && es.zip(expressions).forall {
          case (l, r) => l.semanticEquals(r)
        })
      case x => fail(s"Unexpected partitionIdExpression $x for $hashPartitioning")
    }

    // Validate only HashPartitioning (and HashPartitioning in PartitioningCollection) can satisfy
    // StatefulOpClusteredDistribution. SinglePartition can also satisfy this distribution when
    // `_requiredNumPartitions` is 1.
    checkSatisfied(
      HashPartitioning(Seq($"a", $"b", $"c"), 10),
      StatefulOpClusteredDistribution(Seq($"a", $"b", $"c"), 10),
      true)

    checkSatisfied(
      PartitioningCollection(Seq(
        HashPartitioning(Seq($"a", $"b", $"c"), 10),
        RangePartitioning(Seq($"a".asc, $"b".asc, $"c".asc), 10))),
      StatefulOpClusteredDistribution(Seq($"a", $"b", $"c"), 10),
      true)

    checkSatisfied(
      SinglePartition,
      StatefulOpClusteredDistribution(Seq($"a", $"b", $"c"), 1),
      true)

    checkSatisfied(
      PartitioningCollection(Seq(
        HashPartitioning(Seq($"a", $"b"), 1),
        SinglePartition)),
      StatefulOpClusteredDistribution(Seq($"a", $"b", $"c"), 1),
      true)

    checkSatisfied(
      HashPartitioning(Seq($"a", $"b"), 10),
      StatefulOpClusteredDistribution(Seq($"a", $"b", $"c"), 10),
      false)

    checkSatisfied(
      HashPartitioning(Seq($"a", $"b", $"c"), 5),
      StatefulOpClusteredDistribution(Seq($"a", $"b", $"c"), 10),
      false)

    checkSatisfied(
      RangePartitioning(Seq($"a".asc, $"b".asc, $"c".asc), 10),
      StatefulOpClusteredDistribution(Seq($"a", $"b", $"c"), 10),
      false)

    checkSatisfied(
      SinglePartition,
      StatefulOpClusteredDistribution(Seq($"a", $"b", $"c"), 10),
      false)

    checkSatisfied(
      BroadcastPartitioning(IdentityBroadcastMode),
      StatefulOpClusteredDistribution(Seq($"a", $"b", $"c"), 10),
      false)

    checkSatisfied(
      RoundRobinPartitioning(10),
      StatefulOpClusteredDistribution(Seq($"a", $"b", $"c"), 10),
      false)

    checkSatisfied(
      UnknownPartitioning(10),
      StatefulOpClusteredDistribution(Seq($"a", $"b", $"c"), 10),
      false)
  }
}
