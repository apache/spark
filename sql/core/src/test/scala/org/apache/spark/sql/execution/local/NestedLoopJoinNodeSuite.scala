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
import org.apache.spark.sql.catalyst.plans.{FullOuter, LeftOuter, RightOuter}
import org.apache.spark.sql.execution.joins.{BuildLeft, BuildRight, BuildSide}

class NestedLoopJoinNodeSuite extends LocalNodeTest {

  import testImplicits._

  private def joinSuite(
      suiteName: String, buildSide: BuildSide, confPairs: (String, String)*): Unit = {
    test(s"$suiteName: left outer join") {
      withSQLConf(confPairs: _*) {
        checkAnswer2(
          upperCaseData,
          lowerCaseData,
          wrapForUnsafe(
            (node1, node2) => NestedLoopJoinNode(
              conf,
              node1,
              node2,
              buildSide,
              LeftOuter,
              Some((upperCaseData.col("N") === lowerCaseData.col("n")).expr))
          ),
          upperCaseData.join(lowerCaseData, $"n" === $"N", "left").collect())

        checkAnswer2(
          upperCaseData,
          lowerCaseData,
          wrapForUnsafe(
            (node1, node2) => NestedLoopJoinNode(
              conf,
              node1,
              node2,
              buildSide,
              LeftOuter,
              Some(
                (upperCaseData.col("N") === lowerCaseData.col("n") &&
                  lowerCaseData.col("n") > 1).expr))
          ),
          upperCaseData.join(lowerCaseData, $"n" === $"N" && $"n" > 1, "left").collect())

        checkAnswer2(
          upperCaseData,
          lowerCaseData,
          wrapForUnsafe(
            (node1, node2) => NestedLoopJoinNode(
              conf,
              node1,
              node2,
              buildSide,
              LeftOuter,
              Some(
                (upperCaseData.col("N") === lowerCaseData.col("n") &&
                  upperCaseData.col("N") > 1).expr))
          ),
          upperCaseData.join(lowerCaseData, $"n" === $"N" && $"N" > 1, "left").collect())

        checkAnswer2(
          upperCaseData,
          lowerCaseData,
          wrapForUnsafe(
            (node1, node2) => NestedLoopJoinNode(
              conf,
              node1,
              node2,
              buildSide,
              LeftOuter,
              Some(
                (upperCaseData.col("N") === lowerCaseData.col("n") &&
                  lowerCaseData.col("l") > upperCaseData.col("L")).expr))
          ),
          upperCaseData.join(lowerCaseData, $"n" === $"N" && $"l" > $"L", "left").collect())
      }
    }

    test(s"$suiteName: right outer join") {
      withSQLConf(confPairs: _*) {
        checkAnswer2(
          lowerCaseData,
          upperCaseData,
          wrapForUnsafe(
            (node1, node2) => NestedLoopJoinNode(
              conf,
              node1,
              node2,
              buildSide,
              RightOuter,
              Some((lowerCaseData.col("n") === upperCaseData.col("N")).expr))
          ),
          lowerCaseData.join(upperCaseData, $"n" === $"N", "right").collect())

        checkAnswer2(
          lowerCaseData,
          upperCaseData,
          wrapForUnsafe(
            (node1, node2) => NestedLoopJoinNode(
              conf,
              node1,
              node2,
              buildSide,
              RightOuter,
              Some((lowerCaseData.col("n") === upperCaseData.col("N") &&
                lowerCaseData.col("n") > 1).expr))
          ),
          lowerCaseData.join(upperCaseData, $"n" === $"N" && $"n" > 1, "right").collect())

        checkAnswer2(
          lowerCaseData,
          upperCaseData,
          wrapForUnsafe(
            (node1, node2) => NestedLoopJoinNode(
              conf,
              node1,
              node2,
              buildSide,
              RightOuter,
              Some((lowerCaseData.col("n") === upperCaseData.col("N") &&
                upperCaseData.col("N") > 1).expr))
          ),
          lowerCaseData.join(upperCaseData, $"n" === $"N" && $"N" > 1, "right").collect())

        checkAnswer2(
          lowerCaseData,
          upperCaseData,
          wrapForUnsafe(
            (node1, node2) => NestedLoopJoinNode(
              conf,
              node1,
              node2,
              buildSide,
              RightOuter,
              Some((lowerCaseData.col("n") === upperCaseData.col("N") &&
                lowerCaseData.col("l") > upperCaseData.col("L")).expr))
          ),
          lowerCaseData.join(upperCaseData, $"n" === $"N" && $"l" > $"L", "right").collect())
      }
    }

    test(s"$suiteName: full outer join") {
      withSQLConf(confPairs: _*) {
        checkAnswer2(
          lowerCaseData,
          upperCaseData,
          wrapForUnsafe(
            (node1, node2) => NestedLoopJoinNode(
              conf,
              node1,
              node2,
              buildSide,
              FullOuter,
              Some((lowerCaseData.col("n") === upperCaseData.col("N")).expr))
          ),
          lowerCaseData.join(upperCaseData, $"n" === $"N", "full").collect())

        checkAnswer2(
          lowerCaseData,
          upperCaseData,
          wrapForUnsafe(
            (node1, node2) => NestedLoopJoinNode(
              conf,
              node1,
              node2,
              buildSide,
              FullOuter,
              Some((lowerCaseData.col("n") === upperCaseData.col("N") &&
                lowerCaseData.col("n") > 1).expr))
          ),
          lowerCaseData.join(upperCaseData, $"n" === $"N" && $"n" > 1, "full").collect())

        checkAnswer2(
          lowerCaseData,
          upperCaseData,
          wrapForUnsafe(
            (node1, node2) => NestedLoopJoinNode(
              conf,
              node1,
              node2,
              buildSide,
              FullOuter,
              Some((lowerCaseData.col("n") === upperCaseData.col("N") &&
                upperCaseData.col("N") > 1).expr))
          ),
          lowerCaseData.join(upperCaseData, $"n" === $"N" && $"N" > 1, "full").collect())

        checkAnswer2(
          lowerCaseData,
          upperCaseData,
          wrapForUnsafe(
            (node1, node2) => NestedLoopJoinNode(
              conf,
              node1,
              node2,
              buildSide,
              FullOuter,
              Some((lowerCaseData.col("n") === upperCaseData.col("N") &&
                lowerCaseData.col("l") > upperCaseData.col("L")).expr))
          ),
          lowerCaseData.join(upperCaseData, $"n" === $"N" && $"l" > $"L", "full").collect())
      }
    }
  }

  joinSuite(
    "general-build-left",
    BuildLeft,
    SQLConf.CODEGEN_ENABLED.key -> "false", SQLConf.UNSAFE_ENABLED.key -> "false")
  joinSuite(
    "general-build-right",
    BuildRight,
    SQLConf.CODEGEN_ENABLED.key -> "false", SQLConf.UNSAFE_ENABLED.key -> "false")
  joinSuite(
    "tungsten-build-left",
    BuildLeft,
    SQLConf.CODEGEN_ENABLED.key -> "true", SQLConf.UNSAFE_ENABLED.key -> "true")
  joinSuite(
    "tungsten-build-right",
    BuildRight,
    SQLConf.CODEGEN_ENABLED.key -> "true", SQLConf.UNSAFE_ENABLED.key -> "true")
}
