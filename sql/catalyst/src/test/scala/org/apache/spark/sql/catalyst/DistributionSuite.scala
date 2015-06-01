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
import org.apache.spark.sql.catalyst.plans.physical._

/* Implicit conversions */
import org.apache.spark.sql.catalyst.dsl.expressions._

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

  test("HashPartitioning is the output partitioning") {
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
}
