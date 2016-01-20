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


class UnionNodeSuite extends LocalNodeTest {

  private def testUnion(inputData: Seq[Array[(Int, Int)]]): Unit = {
    val inputNodes = inputData.map { data =>
      new DummyNode(kvIntAttributes, data)
    }
    val unionNode = new UnionNode(conf, inputNodes)
    val expectedOutput = inputData.flatten
    val actualOutput = unionNode.collect().map { case row =>
      (row.getInt(0), row.getInt(1))
    }
    assert(actualOutput === expectedOutput)
  }

  test("empty") {
    testUnion(Seq(Array.empty))
    testUnion(Seq(Array.empty, Array.empty))
  }

  test("self") {
    val data = (1 to 100).map { i => (i, i) }.toArray
    testUnion(Seq(data))
    testUnion(Seq(data, data))
    testUnion(Seq(data, data, data))
  }

  test("basic") {
    val zero = Array.empty[(Int, Int)]
    val one = (1 to 100).map { i => (i, i) }.toArray
    val two = (50 to 150).map { i => (i, i) }.toArray
    val three = (800 to 900).map { i => (i, i) }.toArray
    testUnion(Seq(zero, one, two, three))
  }

}
