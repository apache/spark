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

package org.apache.spark.sql.execution.local

import scala.util.Random

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.SortOrder


class TakeOrderedAndProjectNodeSuite extends LocalNodeTest {

  private def testTakeOrderedAndProject(desc: Boolean): Unit = {
    val limit = 10
    val ascOrDesc = if (desc) "desc" else "asc"
    test(ascOrDesc) {
      val inputData = Random.shuffle((1 to 100).toList).map { i => (i, i) }.toArray
      val inputNode = new DummyNode(kvIntAttributes, inputData)
      val firstColumn = inputNode.output(0)
      val sortDirection = if (desc) Descending else Ascending
      val sortOrder = SortOrder(firstColumn, sortDirection)
      val takeOrderAndProjectNode = new TakeOrderedAndProjectNode(
        conf, limit, Seq(sortOrder), Some(Seq(firstColumn)), inputNode)
      val expectedOutput = inputData
        .map { case (k, _) => k }
        .sortBy { k => k * (if (desc) -1 else 1) }
        .take(limit)
      val actualOutput = takeOrderAndProjectNode.collect().map { row => row.getInt(0) }
      assert(actualOutput === expectedOutput)
    }
  }

  testTakeOrderedAndProject(desc = false)
  testTakeOrderedAndProject(desc = true)
}
