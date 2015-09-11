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

import scala.util.control.NonFatal

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.{DataFrame, Row, SQLConf}
import org.apache.spark.sql.test.{SharedSQLContext, SQLTestUtils}

class LocalNodeTest extends SparkFunSuite with SharedSQLContext {

  def conf: SQLConf = sqlContext.conf

  /**
   * Runs the LocalNode and makes sure the answer matches the expected result.
   * @param input the input data to be used.
   * @param nodeFunction a function which accepts the input LocalNode and uses it to instantiate
   *                     the local physical operator that's being tested.
   * @param expectedAnswer the expected result in a [[Seq]] of [[Row]]s.
   * @param sortAnswers if true, the answers will be sorted by their toString representations prior
   *                    to being compared.
   */
  protected def checkAnswer(
      input: DataFrame,
      nodeFunction: LocalNode => LocalNode,
      expectedAnswer: Seq[Row],
      sortAnswers: Boolean = true): Unit = {
    doCheckAnswer(
      input :: Nil,
      nodes => nodeFunction(nodes.head),
      expectedAnswer,
      sortAnswers)
  }

  /**
   * Runs the LocalNode and makes sure the answer matches the expected result.
   * @param left the left input data to be used.
   * @param right the right input data to be used.
   * @param nodeFunction a function which accepts the input LocalNode and uses it to instantiate
   *                     the local physical operator that's being tested.
   * @param expectedAnswer the expected result in a [[Seq]] of [[Row]]s.
   * @param sortAnswers if true, the answers will be sorted by their toString representations prior
   *                    to being compared.
   */
  protected def checkAnswer2(
      left: DataFrame,
      right: DataFrame,
      nodeFunction: (LocalNode, LocalNode) => LocalNode,
      expectedAnswer: Seq[Row],
      sortAnswers: Boolean = true): Unit = {
    doCheckAnswer(
      left :: right :: Nil,
      nodes => nodeFunction(nodes(0), nodes(1)),
      expectedAnswer,
      sortAnswers)
  }

  /**
   * Runs the `LocalNode`s and makes sure the answer matches the expected result.
   * @param input the input data to be used.
   * @param nodeFunction a function which accepts a sequence of input `LocalNode`s and uses them to
   *                     instantiate the local physical operator that's being tested.
   * @param expectedAnswer the expected result in a [[Seq]] of [[Row]]s.
   * @param sortAnswers if true, the answers will be sorted by their toString representations prior
   *                    to being compared.
   */
  protected def doCheckAnswer(
    input: Seq[DataFrame],
    nodeFunction: Seq[LocalNode] => LocalNode,
    expectedAnswer: Seq[Row],
    sortAnswers: Boolean = true): Unit = {
    LocalNodeTest.checkAnswer(
      input.map(dataFrameToSeqScanNode), nodeFunction, expectedAnswer, sortAnswers) match {
      case Some(errorMessage) => fail(errorMessage)
      case None =>
    }
  }

  protected def dataFrameToSeqScanNode(df: DataFrame): SeqScanNode = {
    new SeqScanNode(
      conf,
      df.queryExecution.sparkPlan.output,
      df.queryExecution.toRdd.map(_.copy()).collect())
  }

}

/**
 * Helper methods for writing tests of individual local physical operators.
 */
object LocalNodeTest {

  /**
   * Runs the `LocalNode`s and makes sure the answer matches the expected result.
   * @param input the input data to be used.
   * @param nodeFunction a function which accepts the input `LocalNode`s and uses them to
   *                     instantiate the local physical operator that's being tested.
   * @param expectedAnswer the expected result in a [[Seq]] of [[Row]]s.
   * @param sortAnswers if true, the answers will be sorted by their toString representations prior
   *                    to being compared.
   */
  def checkAnswer(
    input: Seq[SeqScanNode],
    nodeFunction: Seq[LocalNode] => LocalNode,
    expectedAnswer: Seq[Row],
    sortAnswers: Boolean): Option[String] = {

    val outputNode = nodeFunction(input)

    val outputResult: Seq[Row] = try {
      outputNode.collect()
    } catch {
      case NonFatal(e) =>
        val errorMessage =
          s"""
              | Exception thrown while executing local plan:
              | $outputNode
              | == Exception ==
              | $e
              | ${org.apache.spark.sql.catalyst.util.stackTraceToString(e)}
          """.stripMargin
        return Some(errorMessage)
    }

    SQLTestUtils.compareAnswers(outputResult, expectedAnswer, sortAnswers).map { errorMessage =>
      s"""
          | Results do not match for local plan:
          | $outputNode
          | $errorMessage
       """.stripMargin
    }
  }
}
