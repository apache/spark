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

import org.apache.spark.sql.test.SharedSQLContext

class UnionNodeSuite extends LocalNodeTest with SharedSQLContext {

  test("basic") {
    checkAnswer2(
      testData,
      testData,
      (node1, node2) => UnionNode(conf, Seq(node1, node2)),
      testData.unionAll(testData).collect()
    )
  }

  test("empty") {
    checkAnswer2(
      emptyTestData,
      emptyTestData,
      (node1, node2) => UnionNode(conf, Seq(node1, node2)),
      emptyTestData.unionAll(emptyTestData).collect()
    )
  }

  test("complicated union") {
    val dfs = Seq(testData, emptyTestData, emptyTestData, testData, testData, emptyTestData,
      emptyTestData, emptyTestData, testData, emptyTestData)
    doCheckAnswer(
      dfs,
      nodes => UnionNode(conf, nodes),
      dfs.reduce(_.unionAll(_)).collect()
    )
  }

}
