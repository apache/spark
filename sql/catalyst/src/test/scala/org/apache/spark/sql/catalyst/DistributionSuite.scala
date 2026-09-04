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
import org.apache.spark.sql.catalyst.expressions.{Ascending, AttributeReference, CollationAwareMurmur3Hash, Expression, Literal, Pmod, SortOrder}
import org.apache.spark.sql.catalyst.plans.SQLHelper
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.IntegerType

class DistributionSuite extends SparkFunSuite with SQLHelper {

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

  private def testHashPartitioningLike(
      partitioningName: String,
      create: (Seq[Expression], Int) => Partitioning): Unit = {

    test(s"$partitioningName is the output partitioning") {
      // HashPartitioning can satisfy ClusteredDistribution iff its hash expressions are a subset of
      // the required clustering expressions.
      checkSatisfied(
        create(Seq($"a", $"b", $"c"), 10),
        ClusteredDistribution(Seq($"a", $"b", $"c")),
        true)

      checkSatisfied(
        create(Seq($"b", $"c"), 10),
        ClusteredDistribution(Seq($"a", $"b", $"c")),
        true)

      checkSatisfied(
        create(Seq($"a", $"b", $"c"), 10),
        ClusteredDistribution(Seq($"b", $"c")),
        false)

      checkSatisfied(
        create(Seq($"a", $"b", $"c"), 10),
        ClusteredDistribution(Seq($"d", $"e")),
        false)

      // When ClusteredDistribution.requireAllClusterKeys is set to true,
      // HashPartitioning can only satisfy ClusteredDistribution iff its hash expressions are
      // exactly same as the required clustering expressions.
      checkSatisfied(
        create(Seq($"a", $"b", $"c"), 10),
        ClusteredDistribution(Seq($"a", $"b", $"c"), requireAllClusterKeys = true),
        true)

      checkSatisfied(
        create(Seq($"b", $"c"), 10),
        ClusteredDistribution(Seq($"a", $"b", $"c"), requireAllClusterKeys = true),
        false)

      checkSatisfied(
        create(Seq($"b", $"a", $"c"), 10),
        ClusteredDistribution(Seq($"a", $"b", $"c"), requireAllClusterKeys = true),
        false)

      // HashPartitioning cannot satisfy OrderedDistribution
      checkSatisfied(
        create(Seq($"a", $"b", $"c"), 10),
        OrderedDistribution(Seq($"a".asc, $"b".asc, $"c".asc)),
        false)

      checkSatisfied(
        create(Seq($"a", $"b", $"c"), 1),
        OrderedDistribution(Seq($"a".asc, $"b".asc, $"c".asc)),
        false) // TODO: this can be relaxed.

      checkSatisfied(
        create(Seq($"b", $"c"), 10),
        OrderedDistribution(Seq($"a".asc, $"b".asc, $"c".asc)),
        false)
    }
  }

  testHashPartitioningLike("HashPartitioning",
    (expressions, numPartitions) => HashPartitioning(expressions, numPartitions))

  testHashPartitioningLike("CoalescedHashPartitioning", (expressions, numPartitions) =>
      CoalescedHashPartitioning(
        HashPartitioning(expressions, numPartitions), Seq(CoalescedBoundary(0, numPartitions))))

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

    // When ClusteredDistribution.requireAllClusterKeys is set to true,
    // RangePartitioning can only satisfy ClusteredDistribution iff its ordering expressions are
    // exactly same as the required clustering expressions.
    checkSatisfied(
      RangePartitioning(Seq($"a".asc, $"b".asc, $"c".asc), 10),
      ClusteredDistribution(Seq($"a", $"b", $"c"), requireAllClusterKeys = true),
      true)

    checkSatisfied(
      RangePartitioning(Seq($"a".asc, $"b".asc), 10),
      ClusteredDistribution(Seq($"a", $"b", $"c"), requireAllClusterKeys = true),
      false)

    checkSatisfied(
      RangePartitioning(Seq($"b".asc, $"a".asc, $"c".asc), 10),
      ClusteredDistribution(Seq($"a", $"b", $"c"), requireAllClusterKeys = true),
      false)
  }

  test("Partitioning.numPartitions must match Distribution.requiredNumPartitions to satisfy it") {
    checkSatisfied(
      SinglePartition,
      ClusteredDistribution(Seq($"a", $"b", $"c"), requiredNumPartitions = Some(10)),
      false)

    checkSatisfied(
      HashPartitioning(Seq($"a", $"b", $"c"), 10),
      ClusteredDistribution(Seq($"a", $"b", $"c"), requiredNumPartitions = Some(5)),
      false)

    checkSatisfied(
      RangePartitioning(Seq($"a".asc, $"b".asc, $"c".asc), 10),
      ClusteredDistribution(Seq($"a", $"b", $"c"), requiredNumPartitions = Some(5)),
      false)
  }

  test("Structured Streaming output partitioning and distribution") {
    // Validate HashPartitioning.partitionIdExpression to be exactly expected format, because
    // Structured Streaming state store requires it to be consistent across Spark versions.
    val expressions = Seq($"a", $"b", $"c")
    val hashPartitioning = HashPartitioning(expressions, 10)
    hashPartitioning.partitionIdExpression match {
      case Pmod(CollationAwareMurmur3Hash(es, 42), Literal(10, IntegerType), _) =>
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

  test("SPARK-56615: non-grouped KeyedPartitioning does not satisfy ClusteredDistribution") {
    val x = AttributeReference("x", IntegerType)()

    // Non-grouped: duplicate partition key (1 appears twice), so isGrouped=false.
    val nonGroupedKP =
      KeyedPartitioning(Seq(x), Seq(InternalRow(1), InternalRow(1), InternalRow(2)))
    assert(!nonGroupedKP.isGrouped)
    // satisfies() must return false: the partitions are not yet grouped.
    checkSatisfied(nonGroupedKP, ClusteredDistribution(Seq(x)), false)
    // mayGroupToSatisfy() returns true, because grouping them makes it satisfy.
    assert(nonGroupedKP.mayGroupToSatisfy(ClusteredDistribution(Seq(x))))

    // Grouped: all distinct keys, so isGrouped=true and satisfies() delegates to
    // keysSatisfy().
    val groupedKP = KeyedPartitioning(Seq(x), Seq(InternalRow(1), InternalRow(2), InternalRow(3)))
    assert(groupedKP.isGrouped)
    checkSatisfied(groupedKP, ClusteredDistribution(Seq(x)), true)
  }

  test("SPARK-56877: fromPartitionings reuses already-consistent nested collections") {
    val x = AttributeReference("x", IntegerType)()
    val y = AttributeReference("y", IntegerType)()

    // No KeyedPartitioning anywhere in the subtree: the nested collection is returned as-is.
    // Rebuilding it would make outputPartitioning of deeply nested collections (e.g. chains of
    // same-key shuffle joins) quadratic in the nesting depth.
    val hashCollection = PartitioningCollection.fromPartitionings(
      Seq(HashPartitioning(Seq(x), 10), HashPartitioning(Seq(y), 10)))
    val wrapped = PartitioningCollection.fromPartitionings(
      Seq(hashCollection, HashPartitioning(Seq(y), 10)))
    assert(wrapped.partitionings.head eq hashCollection)

    // KeyedPartitionings already share the canonical partitionKeys reference: also as-is.
    val kpX = KeyedPartitioning(Seq(x), Seq(InternalRow(1), InternalRow(2), InternalRow(3)))
    val kpY = kpX.copy(expressions = Seq(y))
    val keyedCollection = PartitioningCollection.fromPartitionings(Seq(kpX, kpY))
    val keyedWrapped = PartitioningCollection.fromPartitionings(Seq(keyedCollection, kpX))
    assert(keyedWrapped.partitionings.head eq keyedCollection)
  }

  test("SPARK-56877: fromPartitionings interns partitionKeys across nested collections") {
    val x = AttributeReference("x", IntegerType)()
    val y = AttributeReference("y", IntegerType)()

    val kpX = KeyedPartitioning(Seq(x), Seq(InternalRow(1), InternalRow(2)))
    val nested = PartitioningCollection.fromPartitionings(Seq(kpX))
    // Structurally equal but reference-distinct partitionKeys.
    val kpY = KeyedPartitioning(Seq(y), Seq(InternalRow(1), InternalRow(2)))
    assert(kpX.partitionKeys ne kpY.partitionKeys)

    val combined = PartitioningCollection.fromPartitionings(Seq(nested, kpY))
    val interned = combined.partitionings.last.asInstanceOf[KeyedPartitioning]
    assert(interned.partitionKeys eq kpX.partitionKeys)
  }

  test("SPARK-59050: a marked one-partition layout keeps the global ordering claim") {
    // The one-partition exemption inside `keysSatisfy`'s ordered branch: a single partition
    // holds every row, so an out-of-set key cannot break the cross-partition sequence, while
    // two partitions can (the e2e `ORDER BY` repro measures that). Positive control: an
    // always-false gate would shuffle these plans for nothing.
    val a = AttributeReference("a", IntegerType)()
    val ordered = OrderedDistribution(Seq(SortOrder(a, Ascending)))
    val markedOne = KeyedPartitioning(Seq(a), Seq(InternalRow(1)))
      .copy(mayContainUnknownPartitionKeys = true)
    val markedTwo = KeyedPartitioning(Seq(a), Seq(InternalRow(1), InternalRow(2)))
      .copy(mayContainUnknownPartitionKeys = true)
    withSQLConf(SQLConf.V2_BUCKETING_SORTING_ENABLED.key -> "true") {
      checkSatisfied(markedOne, ordered, true)
      checkSatisfied(markedTwo, ordered, false)
    }
  }

  test("SPARK-59050: fromPartitionings normalizes the unknown-keys marker by OR") {
    val x = AttributeReference("x", IntegerType)()
    val y = AttributeReference("y", IntegerType)()
    val marked = KeyedPartitioning(Seq(x), Seq(InternalRow(1), InternalRow(2)))
      .copy(mayContainUnknownPartitionKeys = true)
    val plain = KeyedPartitioning(Seq(y), Seq(InternalRow(1), InternalRow(2)))
    val combined = PartitioningCollection.fromPartitionings(Seq(marked, plain))
    // The conservative direction: an unmarked member must never excuse marked data, because
    // the OR'd-on marker only costs a shuffle while the AND'd-off one could cost correctness.
    val members = combined.partitionings.map(_.asInstanceOf[KeyedPartitioning])
    assert(members.forall(_.mayContainUnknownPartitionKeys), members.toString)
    assert(members.last.partitionKeys eq members.head.partitionKeys)
  }

  test("SPARK-59050: PartitioningCollection requires members to agree on the marker") {
    val x = AttributeReference("x", IntegerType)()
    val y = AttributeReference("y", IntegerType)()
    val keys = Seq(InternalRow(1), InternalRow(2))
    val base = KeyedPartitioning(Seq(x), keys)
    val marked = base.copy(mayContainUnknownPartitionKeys = true)
    // Same keys reference, arity and isCollapsed, only the marker disagrees: it has to reach
    // the marker require rather than the reference check ahead of it.
    val disagree = marked.copy(expressions = Seq(y), mayContainUnknownPartitionKeys = false)
    val err = intercept[IllegalArgumentException] {
      PartitioningCollection(Seq(marked, disagree))
    }
    assert(err.getMessage.contains("agree on mayContainUnknownPartitionKeys"))
  }

  test("SPARK-59050: fromPartitionings normalizes the unknown-keys marker through nesting") {
    val x = AttributeReference("x", IntegerType)()
    val y = AttributeReference("y", IntegerType)()
    val keys = Seq(InternalRow(1), InternalRow(2))
    val marked = KeyedPartitioning(Seq(x), keys).copy(mayContainUnknownPartitionKeys = true)
    val plainNested = PartitioningCollection.fromPartitionings(
      Seq(KeyedPartitioning(Seq(y), keys)))
    val combined = PartitioningCollection.fromPartitionings(Seq(marked, plainNested))
    // The nested collection must be rebuilt with the OR'd-on marker, not excused: only the
    // recursive arm of `fromPartitionings` carries the flag into it.
    val leaves = PartitioningCollection.flatten(combined)
      .collect { case k: KeyedPartitioning => k }
    assert(leaves.length === 2, combined.toString)
    assert(leaves.forall(_.mayContainUnknownPartitionKeys), leaves.toString)
    assert(leaves.forall(_.partitionKeys eq leaves.head.partitionKeys),
      "the interned keys must survive the rebuild")
  }

  test("SPARK-59050: PartitioningCollection requires a nested collection to agree on the " +
    "marker") {
    val x = AttributeReference("x", IntegerType)()
    val y = AttributeReference("y", IntegerType)()
    val keys = Seq(InternalRow(1), InternalRow(2))
    val base = KeyedPartitioning(Seq(x), keys)
    val markedNested = PartitioningCollection.fromPartitionings(Seq(
      base.copy(mayContainUnknownPartitionKeys = true)))
    val plain = base.copy(expressions = Seq(y))
    // The nested collection is internally uniform, so its own construction passes; the outer
    // constructor must still catch its representative against the unmarked sibling. Same keys
    // reference, arity and isCollapsed, only the marker disagrees.
    val err = intercept[IllegalArgumentException] {
      PartitioningCollection(Seq(markedNested, plain))
    }
    assert(err.getMessage.contains("agree on mayContainUnknownPartitionKeys"))
  }

  test("SPARK-56877: PartitioningCollection enforces the invariant through nesting") {
    val x = AttributeReference("x", IntegerType)()
    val y = AttributeReference("y", IntegerType)()

    val kpX = KeyedPartitioning(Seq(x), Seq(InternalRow(1), InternalRow(2)))
    val nested = PartitioningCollection.fromPartitionings(Seq(kpX))

    val kpY = KeyedPartitioning(Seq(y), Seq(InternalRow(1), InternalRow(2)))
    val refMismatch = intercept[IllegalArgumentException] {
      PartitioningCollection(Seq(nested, kpY))
    }
    assert(refMismatch.getMessage.contains("share the same partitionKeys reference"))

    val kpXY = KeyedPartitioning(Seq(x, y), Seq(InternalRow(1, 1), InternalRow(2, 2)))
    val arityMismatch = intercept[IllegalArgumentException] {
      PartitioningCollection(Seq(nested, kpXY))
    }
    assert(arityMismatch.getMessage.contains("matching expression arity"))
  }

  test("SPARK-59057: toGrouped and KeyedShuffleSpec.createPartitioning keep isCollapsed sticky") {
    val x = AttributeReference("x", IntegerType)()
    val y = AttributeReference("y", IntegerType)()

    val collapsedKP = KeyedPartitioning(Seq(x), Seq(InternalRow(1), InternalRow(1), InternalRow(2)))
      .copy(isCollapsed = true)
    assert(collapsedKP.toGrouped.isCollapsed, "toGrouped must keep isCollapsed sticky")

    val spec = KeyedShuffleSpec(collapsedKP, ClusteredDistribution(Seq(x)))
    val created = spec.createPartitioning(Seq(y)).asInstanceOf[KeyedPartitioning]
    assert(created.isCollapsed, "createPartitioning must keep isCollapsed sticky")
  }
}
