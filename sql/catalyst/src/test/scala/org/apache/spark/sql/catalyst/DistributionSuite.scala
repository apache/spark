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
import org.apache.spark.sql.catalyst.plans.physical._

class DistributionSuite extends SparkFunSuite {

  protected def checkSatisfied(
      inputPartitioning: Partitioning,
      requiredDistribution: Distribution,
      satisfied: Boolean) {
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

  test("HashPartitioning (with nullSafe = true) is the output partitioning") {
    // Cases which do not need an exchange between two data properties.
    checkSatisfied(
      HashPartitioning(Seq('a, 'b, 'c), 10),
      UnspecifiedDistribution,
      true)

    checkSatisfied(
      HashPartitioning(Seq('a, 'b, 'c), 10),
      ClusteredDistribution(Seq('a, 'b, 'c)),
      true)

    checkSatisfied(
      HashPartitioning(Seq('b, 'c), 10),
      ClusteredDistribution(Seq('a, 'b, 'c)),
      true)

    checkSatisfied(
      SinglePartition,
      ClusteredDistribution(Seq('a, 'b, 'c)),
      true)

    checkSatisfied(
      SinglePartition,
      OrderedDistribution(Seq('a.asc, 'b.asc, 'c.asc)),
      true)

    // Cases which need an exchange between two data properties.
    checkSatisfied(
      HashPartitioning(Seq('a, 'b, 'c), 10),
      ClusteredDistribution(Seq('b, 'c)),
      false)

    checkSatisfied(
      HashPartitioning(Seq('a, 'b, 'c), 10),
      ClusteredDistribution(Seq('d, 'e)),
      false)

    checkSatisfied(
      HashPartitioning(Seq('a, 'b, 'c), 10),
      AllTuples,
      false)

    checkSatisfied(
      HashPartitioning(Seq('a, 'b, 'c), 10),
      OrderedDistribution(Seq('a.asc, 'b.asc, 'c.asc)),
      false)

    checkSatisfied(
      HashPartitioning(Seq('b, 'c), 10),
      OrderedDistribution(Seq('a.asc, 'b.asc, 'c.asc)),
      false)

    // TODO: We should check functional dependencies
    /*
    checkSatisfied(
      ClusteredDistribution(Seq('b)),
      ClusteredDistribution(Seq('b + 1)),
      true)
    */
  }

  test("RangePartitioning is the output partitioning") {
    // Cases which do not need an exchange between two data properties.
    checkSatisfied(
      RangePartitioning(Seq('a.asc, 'b.asc, 'c.asc), 10),
      UnspecifiedDistribution,
      true)

    checkSatisfied(
      RangePartitioning(Seq('a.asc, 'b.asc, 'c.asc), 10),
      OrderedDistribution(Seq('a.asc, 'b.asc, 'c.asc)),
      true)

    checkSatisfied(
      RangePartitioning(Seq('a.asc, 'b.asc, 'c.asc), 10),
      OrderedDistribution(Seq('a.asc, 'b.asc)),
      true)

    checkSatisfied(
      RangePartitioning(Seq('a.asc, 'b.asc, 'c.asc), 10),
      OrderedDistribution(Seq('a.asc, 'b.asc, 'c.asc, 'd.desc)),
      true)

    checkSatisfied(
      RangePartitioning(Seq('a.asc, 'b.asc, 'c.asc), 10),
      ClusteredDistribution(Seq('a, 'b, 'c)),
      true)

    checkSatisfied(
      RangePartitioning(Seq('a.asc, 'b.asc, 'c.asc), 10),
      ClusteredDistribution(Seq('c, 'b, 'a)),
      true)

    checkSatisfied(
      RangePartitioning(Seq('a.asc, 'b.asc, 'c.asc), 10),
      ClusteredDistribution(Seq('b, 'c, 'a, 'd)),
      true)

    // Cases which need an exchange between two data properties.
    // TODO: We can have an optimization to first sort the dataset
    // by a.asc and then sort b, and c in a partition. This optimization
    // should tradeoff the benefit of a less number of Exchange operators
    // and the parallelism.
    checkSatisfied(
      RangePartitioning(Seq('a.asc, 'b.asc, 'c.asc), 10),
      OrderedDistribution(Seq('a.asc, 'b.desc, 'c.asc)),
      false)

    checkSatisfied(
      RangePartitioning(Seq('a.asc, 'b.asc, 'c.asc), 10),
      OrderedDistribution(Seq('b.asc, 'a.asc)),
      false)

    checkSatisfied(
      RangePartitioning(Seq('a.asc, 'b.asc, 'c.asc), 10),
      ClusteredDistribution(Seq('a, 'b)),
      false)

    checkSatisfied(
      RangePartitioning(Seq('a.asc, 'b.asc, 'c.asc), 10),
      ClusteredDistribution(Seq('c, 'd)),
      false)

    checkSatisfied(
      RangePartitioning(Seq('a.asc, 'b.asc, 'c.asc), 10),
      AllTuples,
      false)
  }

  test("Output partitioning after projection") {
    checkSatisfied(
      HashPartitioning(Seq(('a + 1).asc, ('b + 1).asc), 10)
        .project(Seq('a.as("a1"), ('b + 1).as("b1"))),
      ClusteredDistribution(Seq(('a1 + 1).asc, 'b1.asc)),
      true
    )

    checkSatisfied(
      HashPartitioning(Seq(('a + 1).asc, ('b + 1).asc), 10)
        .project(Seq('a.as("a1"), ('b + 1).as("b1"))),
      ClusteredDistribution(Seq('b1.asc, ('a1 + 1).asc)),
      true
    )

    checkSatisfied(
      HashPartitioning(Seq(('a + 1).asc, ('b + 1).asc), 10)
        .project(Seq('a.as("a1"), ('b + 1).as("b1"))),
      ClusteredDistribution(Seq('a1.asc, 'b1.asc)),
      false
    )

    checkSatisfied(
      RangePartitioning(Seq(('a + 'c).asc, ('b + 'd).asc), 10)
        .project(Seq('a.as("a1"), ('b + 'd).as("bd"), 'c.as("c1"))),
      OrderedDistribution(Seq(('a1 + 'c1).asc, 'bd.asc)),
      true
    )

    checkSatisfied(
      RangePartitioning(Seq(('a + 'c).asc, ('b + 'd).asc), 10)
        .project(Seq('a.as("a1"), ('b + 'd).as("bd"), 'c.as("c1"))),
      OrderedDistribution(Seq('bd.asc, ('a1 + 'c1).asc)),
      false
    )

    checkSatisfied(
      PartitioningCollection(
        Seq(
          RangePartitioning(Seq(('a + 1).asc, ('b + 1).asc), 10),
          RangePartitioning(Seq(('c - 1).asc, ('d - 1).asc), 10)
        )
      ).project(Seq('a.as("a1"), ('b + 1).as("b1"), ('c - 1).as("c1"), 'd.as("d1"))),
      OrderedDistribution(Seq(('a1 + 1).asc, 'b1.asc)),
      true
    )

    checkSatisfied(
      PartitioningCollection(
        Seq(
          RangePartitioning(Seq(('a + 1).asc, ('b + 1).asc), 10),
          RangePartitioning(Seq(('c - 1).asc, ('d - 1).asc), 10)
        )
      ).project(Seq('a.as("a1"), ('b + 1).as("b1"), ('c - 1).as("c1"), 'd.as("d1"))),
      OrderedDistribution(Seq('c1.asc, ('d1 - 1).asc)),
      true
    )
  }
}
