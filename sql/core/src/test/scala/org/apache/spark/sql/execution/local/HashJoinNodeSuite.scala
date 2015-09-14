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

import org.apache.spark.sql.SQLConf
import org.apache.spark.sql.execution.joins

class HashJoinNodeSuite extends LocalNodeTest {

  import testImplicits._

  def joinSuite(suiteName: String, confPairs: (String, String)*): Unit = {
    test(s"$suiteName: inner join with one match per row") {
      withSQLConf(confPairs: _*) {
        checkAnswer2(
          upperCaseData,
          lowerCaseData,
          wrapForUnsafe(
            (node1, node2) => HashJoinNode(
              conf,
              Seq(upperCaseData.col("N").expr),
              Seq(lowerCaseData.col("n").expr),
              joins.BuildLeft,
              node1,
              node2)
          ),
          upperCaseData.join(lowerCaseData, $"n" === $"N").collect()
        )
      }
    }

    test(s"$suiteName: inner join with multiple matches") {
      withSQLConf(confPairs: _*) {
        val x = testData2.where($"a" === 1).as("x")
        val y = testData2.where($"a" === 1).as("y")
        checkAnswer2(
          x,
          y,
          wrapForUnsafe(
            (node1, node2) => HashJoinNode(
              conf,
              Seq(x.col("a").expr),
              Seq(y.col("a").expr),
              joins.BuildLeft,
              node1,
              node2)
          ),
          x.join(y).where($"x.a" === $"y.a").collect()
        )
      }
    }

    test(s"$suiteName: inner join, no matches") {
      withSQLConf(confPairs: _*) {
        val x = testData2.where($"a" === 1).as("x")
        val y = testData2.where($"a" === 2).as("y")
        checkAnswer2(
          x,
          y,
          wrapForUnsafe(
            (node1, node2) => HashJoinNode(
              conf,
              Seq(x.col("a").expr),
              Seq(y.col("a").expr),
              joins.BuildLeft,
              node1,
              node2)
          ),
          Nil
        )
      }
    }

    test(s"$suiteName: big inner join, 4 matches per row") {
      withSQLConf(confPairs: _*) {
        val bigData = testData.unionAll(testData).unionAll(testData).unionAll(testData)
        val bigDataX = bigData.as("x")
        val bigDataY = bigData.as("y")

        checkAnswer2(
          bigDataX,
          bigDataY,
          wrapForUnsafe(
            (node1, node2) =>
              HashJoinNode(
                conf,
                Seq(bigDataX.col("key").expr),
                Seq(bigDataY.col("key").expr),
                joins.BuildLeft,
                node1,
                node2)
          ),
          bigDataX.join(bigDataY).where($"x.key" === $"y.key").collect())
      }
    }
  }

  joinSuite(
    "general", SQLConf.CODEGEN_ENABLED.key -> "false", SQLConf.UNSAFE_ENABLED.key -> "false")
  joinSuite("tungsten", SQLConf.CODEGEN_ENABLED.key -> "true", SQLConf.UNSAFE_ENABLED.key -> "true")
}
