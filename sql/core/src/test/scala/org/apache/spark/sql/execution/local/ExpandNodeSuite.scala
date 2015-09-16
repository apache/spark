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

import org.apache.spark.sql.catalyst.dsl.expressions._


class ExpandNodeSuite extends LocalNodeTest {

  private def testExpand(inputData: Array[(Int, Int)] = Array.empty): Unit = {
    val inputNode = new DummyNode(kvIntAttributes, inputData)
    val projections = Seq(Seq('k + 'v, 'k - 'v), Seq('k * 'v, 'k / 'v))
    val expandNode = new ExpandNode(conf, projections, inputNode.output, inputNode)
    val resolvedNode = resolveExpressions(expandNode)
    val expectedOutput = {
      val firstHalf = inputData.map { case (k, v) => (k + v, k - v) }
      val secondHalf = inputData.map { case (k, v) => (k * v, k / v) }
      firstHalf ++ secondHalf
    }
    val actualOutput = resolvedNode.collect().map { case row =>
      (row.getInt(0), row.getInt(1))
    }
    assert(actualOutput.toSet === expectedOutput.toSet)
  }

  test("empty") {
    testExpand()
  }

  test("basic") {
    testExpand((1 to 100).map { i => (i, i * 1000) }.toArray)
  }

}
