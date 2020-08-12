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

package org.apache.spark.sql

import java.io.File
import java.nio.charset.StandardCharsets

import scala.collection.mutable

import org.apache.commons.io.FileUtils

import org.apache.spark.sql.catalyst.expressions.AttributeSet
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.adaptive.DisableAdaptiveExecutionSuite
import org.apache.spark.sql.execution.exchange.{Exchange, ReusedExchangeExec}
import org.apache.spark.sql.internal.SQLConf

/**
 * Check that TPCDS SparkPlans don't change.
 * If there are plan differences, the error message looks like this:
 *   Plans did not match:
 *   last approved plan: /path/to/tpcds-plan-stability/approved-plans-xxx/q1/simplified.txt
 *   last explain: /path/to/tpcds-plan-stability/approved-plans-xxx/q1/explain.txt
 *   actual plan: /path/to/tmp/q1.actual.simplified.txt
 *   actual explain: /path/to/tmp/q1.actual.explain.txt
 *   [side by side plan diff]
 * The explain files are saved to help debug later, they are not checked. Only the simplified
 * plans are checked (by string comparison).
 *
 * Approving new plans:
 * IF the plan change is intended then re-running the test
 * with environ var SPARK_GENERATE_GOLDEN_FILES=1 will make the new plan canon.
 * This should be done only for the queries that need it, to avoid unnecessary diffs in the
 * other approved plans.
 * This can be done by running sbt test-only *PlanStability[WithStats]Suite* -- -z "q31"
 * The new plan files should be part of the PR and reviewed.
 */
trait PlanStabilitySuite extends TPCDSBase with DisableAdaptiveExecutionSuite {

  private val originalMaxToStringFields = conf.maxToStringFields

  override def beforeAll(): Unit = {
    conf.setConf(SQLConf.MAX_TO_STRING_FIELDS, Int.MaxValue)
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    conf.setConf(SQLConf.MAX_TO_STRING_FIELDS, originalMaxToStringFields)
  }

  private val regenerateGoldenFiles: Boolean = System.getenv("SPARK_GENERATE_GOLDEN_FILES") == "1"

  protected val baseResourcePath = {
    // use the same way as `SQLQueryTestSuite` to get the resource path
    java.nio.file.Paths.get("src", "test", "resources", "tpcds-plan-stability").toFile
  }

  def goldenFilePath: String

  private def getDirForTest(name: String): File = {
    new File(goldenFilePath, name)
  }

  private def isApproved(name: String, dir: File, actualSimplifiedPlan: String): Boolean = {
    val file = new File(dir, "simplified.txt")
    val approved = FileUtils.readFileToString(file, StandardCharsets.UTF_8)
    approved == actualSimplifiedPlan
  }

  /**
   * Serialize and save this SparkPlan.
   * The resulting file is used by [[checkWithApproved]] to check stability.
   *
   * @param plan    th SparkPlan
   * @param name    the name of the query
   * @param explain the full explain output; this is saved to help debug later as the simplified
   *                plan is not too useful for debugging
   */
  private def generateApprovedPlanFile(
      plan: SparkPlan,
      name: String,
      explain: String): Unit = {
    val dir = getDirForTest(name)
    val simplified = getSimplifiedPlan(plan)
    val foundMatch = dir.exists() && isApproved(name, dir, simplified)

    if (!foundMatch) {
      FileUtils.deleteDirectory(dir)
      assert(dir.mkdirs())

      val file = new File(dir, "simplified.txt")
      FileUtils.writeStringToFile(file, simplified, StandardCharsets.UTF_8)
      val fileOriginalPlan = new File(dir, "explain.txt")
      FileUtils.writeStringToFile(fileOriginalPlan, explain, StandardCharsets.UTF_8)
      logInfo(s"APPROVED: $file $fileOriginalPlan")
    }
  }

  private def checkWithApproved(plan: SparkPlan, name: String, explain: String): Unit = {
    val dir = getDirForTest(name)
    val tempDir = FileUtils.getTempDirectory
    val actualSimplified = getSimplifiedPlan(plan)
    val foundMatch = isApproved(name, dir, actualSimplified)

    if (!foundMatch) {
      // show diff with last approved
      val approvedSimplifiedFile = new File(dir, "simplified.txt")
      val approvedExplainFile = new File(dir, "explain.txt")

      val actualSimplifiedFile = new File(tempDir, s"$name.actual.simplified.txt")
      val actualExplainFile = new File(tempDir, s"$name.actual.explain.txt")

      val approvedSimplified = FileUtils.readFileToString(
        approvedSimplifiedFile, StandardCharsets.UTF_8)
      // write out for debugging
      FileUtils.writeStringToFile(actualSimplifiedFile, actualSimplified, StandardCharsets.UTF_8)
      FileUtils.writeStringToFile(actualExplainFile, explain, StandardCharsets.UTF_8)

      val header =
        s"""
           |Plans did not match:
           |last approved plan: ${approvedSimplifiedFile.getAbsolutePath}
           |last explain: ${approvedExplainFile.getAbsolutePath}
           |actual plan: ${actualSimplifiedFile.getAbsolutePath}
           |actual explain: ${actualExplainFile.getAbsolutePath}
        """.stripMargin
      val msg =
        s"""
           |$header
           |${sideBySide(approvedSimplified.linesIterator.toSeq,
          actualSimplified.linesIterator.toSeq).mkString("\n")}
         """.stripMargin
      fail(msg)
    }
  }

  /**
   * Get the simplified plan for a specific SparkPlan. In the simplified plan, the node only has
   * its name and all the sorted reference names(without ExprId) and its simplified children as
   * well. And we'll only identify the performance sensitive nodes, e.g. Exchange, Subquery, in the
   * simplified plan. Given such a identical but simplified plan, we'd expect to avoid frequent
   * plan changing and catch the possible meaningful regression.
   */
  private def getSimplifiedPlan(plan: SparkPlan): String = {
    val exchangeIdMap = new mutable.HashMap[Int, Int]()
    val subqueriesMap = new mutable.HashMap[Int, Int]()

    def getId(plan: SparkPlan): Int = plan match {
      case exchange: Exchange => exchangeIdMap.getOrElseUpdate(exchange.id, exchangeIdMap.size + 1)
      case ReusedExchangeExec(_, exchange) =>
        exchangeIdMap.getOrElseUpdate(exchange.id, exchangeIdMap.size + 1)
      case subquery: SubqueryExec =>
        subqueriesMap.getOrElseUpdate(subquery.id, subqueriesMap.size + 1)
      case subquery: SubqueryBroadcastExec =>
        subqueriesMap.getOrElseUpdate(subquery.id, subqueriesMap.size + 1)
      case ReusedSubqueryExec(subquery) =>
        subqueriesMap.getOrElseUpdate(subquery.id, subqueriesMap.size + 1)
      case _ => -1
    }

    /**
     * Some expression names have ExprId in them due to using things such as
     * "sum(sr_return_amt#14)", so we remove all of these using regex
     */
    def cleanUpReferences(references: AttributeSet): String = {
      val rgxId = "#\\d+".r
      rgxId.replaceAllIn(references.toSeq.map(_.name).sorted.mkString(","), "")
    }

    /**
     * Generate a simplified plan as a string
     * Example output:
     * TakeOrderedAndProject [c_customer_id]
     *   WholeStageCodegen
     *     Project [c_customer_id]
     */
    def getSimplifiedPlan(node: SparkPlan, depth: Int): String = {
      val padding = "  " * depth
      var thisNode = node.nodeName
      if (node.references.nonEmpty) {
        thisNode += s" [${cleanUpReferences(node.references)}]"
      }
      if (node.producedAttributes.nonEmpty) {
        thisNode += s" [${cleanUpReferences(node.producedAttributes)}]"
      }
      val id = getId(node)
      if (id > 0) {
        thisNode += s" #$id"
      }
      val childrenSimplified = node.children.map(getSimplifiedPlan(_, depth + 1))
      val subqueriesSimplified = node.subqueries.map(getSimplifiedPlan(_, depth + 1))
      s"$padding$thisNode\n${subqueriesSimplified.mkString("")}${childrenSimplified.mkString("")}"
    }

    getSimplifiedPlan(plan, 0)
  }

  private def normalizeIds(query: String): String = {
    val regex = "#\\d+L?".r
    val map = new mutable.HashMap[String, String]()
    regex.findAllMatchIn(query).map(_.toString)
      .foreach(map.getOrElseUpdate(_, (map.size + 1).toString))
    regex.replaceAllIn(query, regexMatch => s"#${map(regexMatch.toString)}")
  }

  /**
   * Test a TPC-DS query. Depending of the settings this test will either check if the plan matches
   * a golden file or it will create a new golden file.
   */
  protected def testQuery(tpcdsGroup: String, query: String, suffix: String = ""): Unit = {
    val queryString = resourceToString(s"$tpcdsGroup/$query.sql",
      classLoader = Thread.currentThread().getContextClassLoader)
    val qe = sql(queryString).queryExecution
    val plan = qe.executedPlan
    val explain = normalizeIds(qe.toString)

    if (regenerateGoldenFiles) {
      generateApprovedPlanFile(plan, query + suffix, explain)
    } else {
      checkWithApproved(plan, query + suffix, explain)
    }
  }

  // TODO: this is used to reduce generated files to ease the review. Revert it back after review.
  val targetQuires = Set("q47", "q49", "q57")
}

class TPCDSV1_4_PlanStabilitySuite extends PlanStabilitySuite {
  override val goldenFilePath: String =
    new File(baseResourcePath, s"approved-plans-v1_4").getAbsolutePath

  tpcdsQueries.filter(targetQuires.contains).foreach { q =>
    test(s"check simplified (tpcds-v1.4/$q)") {
      testQuery("tpcds", q)
    }
  }
}

class TPCDSV1_4_PlanStabilityWithStatsSuite extends PlanStabilitySuite {
  override def injectStats: Boolean = true

  override val goldenFilePath: String =
    new File(baseResourcePath, s"approved-plans-v1_4").getAbsolutePath

  tpcdsQueries.filter(targetQuires.contains).foreach { q =>
    test(s"check simplified sf100 (tpcds-v1.4/$q)") {
      testQuery("tpcds", q, ".sf100")
    }
  }
}

class TPCDSV2_7_PlanStabilitySuite extends PlanStabilitySuite {
  override val goldenFilePath: String =
    new File(baseResourcePath, s"approved-plans-v2_7").getAbsolutePath

  tpcdsQueriesV2_7_0.filter(targetQuires.contains).foreach { q =>
    test(s"check simplified (tpcds-v2.7.0/$q)") {
      testQuery("tpcds-v2.7.0", q)
    }
  }
}

class TPCDSV2_7_PlanStabilityWithStatsSuite extends PlanStabilitySuite {
  override def injectStats: Boolean = true

  override val goldenFilePath: String =
    new File(baseResourcePath, s"approved-plans-v2_7").getAbsolutePath

  tpcdsQueriesV2_7_0.filter(targetQuires.contains).foreach { q =>
    test(s"check simplified sf100 (tpcds-v2.7.0/$q)") {
      testQuery("tpcds-v2.7.0", q, ".sf100")
    }
  }
}

class TPCDSModifiedPlanStabilitySuite extends PlanStabilitySuite {
  override val goldenFilePath: String =
    new File(baseResourcePath, s"approved-plans-modified").getAbsolutePath

  modifiedTPCDSQueries.filter(targetQuires.contains).foreach { q =>
    test(s"check simplified (tpcds-modifiedQueries/$q)") {
      testQuery("tpcds-modifiedQueries", q)
    }
  }
}

class TPCDSModifiedPlanStabilityWithStatsSuite extends PlanStabilitySuite {
  override def injectStats: Boolean = true

  override val goldenFilePath: String =
    new File(baseResourcePath, s"approved-plans-modified").getAbsolutePath

  modifiedTPCDSQueries.filter(targetQuires.contains).foreach { q =>
    test(s"check simplified sf100 (tpcds-modifiedQueries/$q)") {
      testQuery("tpcds-modifiedQueries", q, ".sf100")
    }
  }
}
