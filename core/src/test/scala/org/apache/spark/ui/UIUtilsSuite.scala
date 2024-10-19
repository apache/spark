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

package org.apache.spark.ui

import scala.xml.{Node, Text}
import scala.xml.Utility.trim

import org.apache.spark.{ErrorMessageFormat, SparkException, SparkFunSuite, SparkThrowableHelper}

class UIUtilsSuite extends SparkFunSuite {
  import UIUtils._

  test("makeDescription(plainText = false)") {
    verify(
      """test <a href="/link"> text </a>""",
      <span class="description-input">test <a href="/link"> text </a></span>,
      "Correctly formatted text with only anchors and relative links should generate HTML",
      plainText = false
    )

    verify(
      """test <a href="/link" text </a>""",
      <span class="description-input">{"""test <a href="/link" text </a>"""}</span>,
      "Badly formatted text should make the description be treated as a string instead of HTML",
      plainText = false
    )

    verify(
      """test <a href="link"> text </a>""",
      <span class="description-input">{"""test <a href="link"> text </a>"""}</span>,
      "Non-relative links should make the description be treated as a string instead of HTML",
      plainText = false
    )

    verify(
      """test<a><img></img></a>""",
      <span class="description-input">{"""test<a><img></img></a>"""}</span>,
      "Non-anchor elements should make the description be treated as a string instead of HTML",
      plainText = false
    )

    verify(
      """test <a href="/link"> text </a>""",
      <span class="description-input">test <a href="base/link"> text </a></span>,
      baseUrl = "base",
      errorMsg = "Base URL should be prepended to html links",
      plainText = false
    )

    verify(
      """<a onclick="alert('oops');"></a>""",
      <span class="description-input">{"""<a onclick="alert('oops');"></a>"""}</span>,
      "Non href attributes should make the description be treated as a string instead of HTML",
      plainText = false
    )

    verify(
      """<a onmouseover="alert('oops');"></a>""",
      <span class="description-input">{"""<a onmouseover="alert('oops');"></a>"""}</span>,
      "Non href attributes should make the description be treated as a string instead of HTML",
      plainText = false
    )
  }

  test("makeDescription(plainText = true)") {
    verify(
      """test <a href="/link"> text </a>""",
      Text("test  text "),
      "Correctly formatted text with only anchors and relative links should generate a string " +
      "without any html tags",
      plainText = true
    )

    verify(
      """test <a href="/link"> text1 </a> <a href="/link"> text2 </a>""",
      Text("test  text1   text2 "),
      "Correctly formatted text with multiple anchors and relative links should generate a " +
      "string without any html tags",
      plainText = true
    )

    verify(
      """test <a href="/link"><span> text </span></a>""",
      Text("test  text "),
      "Correctly formatted text with nested anchors and relative links and/or spans should " +
      "generate a string without any html tags",
      plainText = true
    )

    verify(
      """test <a href="/link" text </a>""",
      Text("""test <a href="/link" text </a>"""),
      "Badly formatted text should make the description be as the same as the original text",
      plainText = true
    )

    verify(
      """test <a href="link"> text </a>""",
      Text("""test <a href="link"> text </a>"""),
      "Non-relative links should make the description be as the same as the original text",
      plainText = true
    )

    verify(
      """test<a><img></img></a>""",
      Text("""test<a><img></img></a>"""),
      "Non-anchor elements should make the description be as the same as the original text",
      plainText = true
    )
  }

  test("SPARK-11906: Progress bar should not overflow because of speculative tasks") {
    val generated = makeProgressBar(2, 3, 0, 0, Map.empty, 4).head.child.filter(_.label == "div")
    val expected = Seq(
      <div class="progress-bar progress-completed" style="width: 75.0%"></div>,
      <div class="progress-bar progress-started" style="width: 25.0%"></div>
    )
    assert(generated.sameElements(expected),
      s"\nRunning progress bar should round down\n\nExpected:\n$expected\nGenerated:\n$generated")
  }

  test("decodeURLParameter (SPARK-12708: Sorting task error in Stages Page when yarn mode.)") {
    val encoded1 = "%252F"
    val decoded1 = "/"

    assert(decoded1 === decodeURLParameter(encoded1))

    // verify that no affect to decoded URL.
    assert(decoded1 === decodeURLParameter(decoded1))
  }

  test("listingTable with tooltips") {

    def generateDataRowValue: String => Seq[Node] = row => <a>{row}</a>
    val header = Seq("Header1", "Header2")
    val data = Seq("Data1", "Data2")
    val tooltip = Seq(None, Some("tooltip"))

    val generated = listingTable(header, generateDataRowValue, data, tooltipHeaders = tooltip)

    val expected: Node =
      <table class="table table-bordered table-sm table-striped sortable">
        <thead>
          <th width="" class="">{header(0)}</th>
          <th width="" class="">
              <span data-toggle="tooltip" title="tooltip">
                {header(1)}
              </span>
          </th>
        </thead>
      <tbody>
        {data.map(generateDataRowValue)}
      </tbody>
    </table>

    assert(trim(generated(0)) == trim(expected))
  }

  test("listingTable without tooltips") {

    def generateDataRowValue: String => Seq[Node] = row => <a>{row}</a>
    val header = Seq("Header1", "Header2")
    val data = Seq("Data1", "Data2")

    val generated = listingTable(header, generateDataRowValue, data)

    val expected =
      <table class="table table-bordered table-sm table-striped sortable">
        <thead>
          <th width="" class="">{header(0)}</th>
          <th width="" class="">{header(1)}</th>
        </thead>
        <tbody>
          {data.map(generateDataRowValue)}
        </tbody>
      </table>

    assert(trim(generated(0)) == trim(expected))
  }

  private def verify(
      desc: String,
      expected: Node,
      errorMsg: String = "",
      baseUrl: String = "",
      plainText: Boolean): Unit = {
    val generated = makeDescription(desc, baseUrl, plainText)
    assert(generated.sameElements(expected),
      s"\n$errorMsg\n\nExpected:\n$expected\nGenerated:\n$generated")
  }

  // scalastyle:off line.size.limit
  test("SPARK-44367: Extract errorClass from errorMsg with errorMessageCell") {
    val e1 = "Job aborted due to stage failure: Task 0 in stage 1.0 failed 1 times, most recent failure: Lost task 0.0 in stage 1.0 (TID 1) (10.221.98.22 executor driver): org.apache.spark.SparkArithmeticException: [DIVIDE_BY_ZERO] Division by zero. Use `try_divide` to tolerate divisor being 0 and return NULL instead. If necessary set \"spark.sql.ansi.enabled\" to \"false\" to bypass this error.\n== SQL (line 1, position 8) ==\nselect a/b from src\n       ^^^\n\n\tat org.apache.spark.sql.errors.QueryExecutionErrors$.divideByZeroError(QueryExecutionErrors.scala:226)\n\tat org.apache.spark.sql.errors.QueryExecutionErrors.divideByZeroError(QueryExecutionErrors.scala)\n\tat org.apache.spark.sql.catalyst.expressions.GeneratedClass$GeneratedIteratorForCodegenStage1.processNext(generated.java:54)\n\tat org.apache.spark.sql.execution.BufferedRowIterator.hasNext(BufferedRowIterator.java:43)\n\tat org.apache.spark.sql.execution.WholeStageCodegenEvaluatorFactory$WholeStageCodegenPartitionEvaluator$$anon$1.hasNext(WholeStageCodegenEvaluatorFactory.scala:43)\n\tat org.apache.spark.sql.execution.SparkPlan.$anonfun$getByteArrayRdd$1(SparkPlan.scala:388)\n\tat org.apache.spark.rdd.RDD.$anonfun$mapPartitionsInternal$2(RDD.scala:890)\n\tat org.apache.spark.rdd.RDD.$anonfun$mapPartitionsInternal$2$adapted(RDD.scala:890)\n\tat org.apache.spark.rdd.MapPartitionsRDD.compute(MapPartitionsRDD.scala:52)\n\tat org.apache.spark.rdd.RDD.computeOrReadCheckpoint(RDD.scala:364)\n\tat org.apache.spark.rdd.RDD.iterator(RDD.scala:328)\n\tat org.apache.spark.scheduler.ResultTask.runTask(ResultTask.scala:93)\n\tat org.apache.spark.TaskContext.runTaskWithListeners(TaskContext.scala:161)\n\tat org.apache.spark.scheduler.Task.run(Task.scala:141)\n\tat org.apache.spark.executor.Executor$TaskRunner.$anonfun$run$4(Executor.scala:592)\n\tat org.apache.spark.util.Utils$.tryWithSafeFinally(Utils.scala:1474)\n\tat org.apache.spark.executor.Executor$TaskRunner.run(Executor.scala:595)\n\tat java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1149)\n\tat java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:624)\n\tat java.lang.Thread.run(Thread.java:750)\n\nDriver stacktrace:"
    val cell1 = UIUtils.errorMessageCell(e1)
    assert(cell1 === <td>{"DIVIDE_BY_ZERO"}{UIUtils.detailsUINode(isMultiline = true, e1)}</td>)

    val e2 = SparkException.internalError("test")
    val cell2 = UIUtils.errorMessageCell(e2.getMessage)
    assert(cell2 === <td>{"INTERNAL_ERROR"}{UIUtils.detailsUINode(isMultiline = true, e2.getMessage)}</td>)

    val e3 = new SparkException(
      errorClass = "CANNOT_CAST_DATATYPE",
      messageParameters = Map("sourceType" -> "long", "targetType" -> "int"), cause = null)
    val cell3 = UIUtils.errorMessageCell(SparkThrowableHelper.getMessage(e3, ErrorMessageFormat.PRETTY))
    assert(cell3 === <td>{"CANNOT_CAST_DATATYPE"}{UIUtils.detailsUINode(isMultiline = true, e3.getMessage)}</td>)

    val e4 = "java.lang.RuntimeException: random text"
    val cell4 = UIUtils.errorMessageCell(e4)
    assert(cell4 === <td>{"java.lang.RuntimeException"}{UIUtils.detailsUINode(isMultiline = true, e4)}</td>)
  }
  // scalastyle:on line.size.limit
}
