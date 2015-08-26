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
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.StructType

class LocalNodeTest extends SparkFunSuite {

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
    val output = df.queryExecution.sparkPlan.output
    val converter =
      CatalystTypeConverters.createToCatalystConverter(StructType.fromAttributes(output))
    new SeqScanNode(
      output,
      df.collect().map(r => converter(r).asInstanceOf[InternalRow]))
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

    compareAnswers(outputResult, expectedAnswer, sortAnswers).map { errorMessage =>
      s"""
          | Results do not match for local plan:
          | $outputNode
          | $errorMessage
       """.stripMargin
    }
  }

  private def compareAnswers(
      answer: Seq[Row],
      expectedAnswer: Seq[Row],
      sort: Boolean): Option[String] = {
    def prepareAnswer(answer: Seq[Row]): Seq[Row] = {
      // Converts data to types that we can do equality comparison using Scala collections.
      // For BigDecimal type, the Scala type has a better definition of equality test (similar to
      // Java's java.math.BigDecimal.compareTo).
      // For binary arrays, we convert it to Seq to avoid of calling java.util.Arrays.equals for
      // equality test.
      // This function is copied from Catalyst's QueryTest
      val converted: Seq[Row] = answer.map { s =>
        Row.fromSeq(s.toSeq.map {
          case d: java.math.BigDecimal => BigDecimal(d)
          case b: Array[Byte] => b.toSeq
          case o => o
        })
      }
      if (sort) {
        converted.sortBy(_.toString())
      } else {
        converted
      }
    }
    if (prepareAnswer(expectedAnswer) != prepareAnswer(answer)) {
      val errorMessage =
        s"""
           | == Results ==
           | ${sideBySide(
          s"== Expected Answer - ${expectedAnswer.size} ==" +:
            prepareAnswer(expectedAnswer).map(_.toString()),
          s"== Actual Answer - ${answer.size} ==" +:
            prepareAnswer(answer).map(_.toString())).mkString("\n")}
      """.stripMargin
      Some(errorMessage)
    } else {
      None
    }
  }

}
