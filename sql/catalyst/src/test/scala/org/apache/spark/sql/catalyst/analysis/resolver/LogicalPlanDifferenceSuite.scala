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

package org.apache.spark.sql.catalyst.analysis.resolver

import java.util.Locale

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
import org.apache.spark.sql.types.{IntegerType, StringType}

class LogicalPlanDifferenceSuite extends SparkFunSuite with SQLConfHelper {

  private val idAttr = AttributeReference("id", IntegerType)()
  private val nameAttr = AttributeReference("name", StringType)()
  private val ageAttr = AttributeReference("age", IntegerType)()

  private val GLOBAL_LIMIT = "GlobalLimit"
  private val LOCAL_LIMIT = "LocalLimit"
  private val LOCAL_RELATION = "LocalRelation"
  private val FILTER = "Filter"
  private val PROJECT = "Project"

  test("identical plans should return empty strings") {
    val plan1 = LocalRelation(idAttr, nameAttr)
    val plan2 = LocalRelation(idAttr, nameAttr)

    val (result1, result2) = LogicalPlanDifference(plan1, plan2, 2)

    assert(result1 == "")
    assert(result2 == "")
  }

  test("different plans with default context size (2 lines)") {
    // Create larger plans with multiple operations
    val plan1 = LocalRelation(idAttr, nameAttr, ageAttr)
      .where(idAttr > 10)
      .where(nameAttr === "Alice")
      .where(ageAttr < 50)
      .select(idAttr, nameAttr)
      .limit(100)

    val plan2 = LocalRelation(idAttr, nameAttr, ageAttr)
      .where(idAttr > 20) // Different condition - first mismatch
      .where(nameAttr === "Alice")
      .where(ageAttr < 50)
      .select(idAttr, nameAttr)
      .limit(100)

    val (result1, result2) = LogicalPlanDifference(plan1, plan2, 2)

    // Results should be truncated (shorter than full plans since mismatch is in the middle)
    assert(result1.length < plan1.toString.length)
    assert(result2.length < plan2.toString.length)

    // Should contain the different filter conditions
    assert(result1.contains("10"))
    assert(result2.contains("20"))

    // Should contain the mismatch line (Filter with id comparison)
    assert(result1.contains(FILTER))
    assert(result2.contains(FILTER))

    // Should contain the second filter (Alice) as context after the mismatch
    assert(result1.contains("Alice"))
    assert(result2.contains("Alice"))

    // Should contain context (age filter is also nearby)
    assert(result1.contains("age") || result1.contains("50"))
    assert(result2.contains("age") || result2.contains("50"))

    // Should NOT contain operations too far away (limit is at the top)
    val lines1 = result1.split("\n").filter(_.nonEmpty).length
    val lines2 = result2.split("\n").filter(_.nonEmpty).length
    assert(
      lines1 <= 7,
      s"Expected at most 7 lines (2 before + mismatch + 2 after + margins), got $lines1"
    )
    assert(lines2 <= 7, s"Expected at most 7 lines, got $lines2")
  }

  test("plans differing at first line") {
    // Larger plans where the top-level operation differs
    val plan1 = LocalRelation(idAttr, nameAttr, ageAttr)
      .where(idAttr > 5)
      .where(nameAttr === "Bob")
      .select(idAttr, nameAttr) // Different projection
      .limit(50)

    val plan2 = LocalRelation(idAttr, nameAttr, ageAttr)
      .where(idAttr > 5)
      .where(nameAttr === "Bob")
      .select(idAttr, ageAttr) // Different projection - includes age instead of name
      .limit(50)

    val (result1, result2) = LogicalPlanDifference(plan1, plan2, 2)

    // Should show the first line (GlobalLimit) and context after
    assert(result1.contains(GLOBAL_LIMIT))
    assert(result2.contains(GLOBAL_LIMIT))

    // Should show the mismatch in the Project
    assert(result1.contains(PROJECT))
    assert(result2.contains(PROJECT))
    assert(result1.contains("name"))
    assert(result2.contains("age"))

    // Should show some context after (first Filter)
    assert(result1.contains(FILTER))
    assert(result2.contains(FILTER))

    // Should NOT show the bottom LocalRelation (too far)
    assert(!result1.contains(LOCAL_RELATION))
    assert(!result2.contains(LOCAL_RELATION))
  }

  test("plans differing at last line") {
    // Large plans where only the bottom LocalRelation differs
    val plan1 = LocalRelation(idAttr, nameAttr) // Different attributes
      .where(idAttr > 5)
      .where(nameAttr === "Charlie")
      .where(idAttr < 100)
      .select(idAttr, nameAttr)
      .limit(10)

    val plan2 = LocalRelation(idAttr, ageAttr) // Different attributes - uses age instead of name
      .where(idAttr > 5)
      .where(nameAttr === "Charlie")
      .where(idAttr < 100)
      .select(idAttr, nameAttr)
      .limit(10)

    val (result1, result2) = LogicalPlanDifference(plan1, plan2, 2)

    // The plans are the same except LocalRelation has different attributes at the bottom
    // However, the FIRST line that differs will be much earlier in the tree
    // because the filter references different attributes

    // Should show some Filter operations (the first mismatch will be in one of them)
    assert(result1.contains(FILTER))
    assert(result2.contains(FILTER))

    // Both should have truncated output
    val lines1 = result1.split("\n").filter(_.nonEmpty).length
    val lines2 = result2.split("\n").filter(_.nonEmpty).length
    assert(lines1 >= 1 && lines1 <= 7, s"Expected 1-7 lines, got $lines1")
    assert(lines2 >= 1 && lines2 <= 7, s"Expected 1-7 lines, got $lines2")

    // At least one should reference the different attributes or values
    val combined1 = result1.toLowerCase(Locale.ROOT)
    val combined2 = result2.toLowerCase(Locale.ROOT)
    assert(combined1.contains("name") || combined1.contains("age") || combined1.contains("id"))
    assert(combined2.contains("name") || combined2.contains("age") || combined2.contains("id"))
  }

  test("custom context size - 0 lines (only mismatch line)") {
    // Larger plan with mismatch in the middle
    val plan1 = LocalRelation(idAttr, nameAttr, ageAttr)
      .where(idAttr > 10) // First mismatch
      .where(nameAttr === "David")
      .where(ageAttr < 60)
      .select(idAttr, nameAttr)
      .limit(25)

    val plan2 = LocalRelation(idAttr, nameAttr, ageAttr)
      .where(idAttr > 99) // Different value
      .where(nameAttr === "David")
      .where(ageAttr < 60)
      .select(idAttr, nameAttr)
      .limit(25)

    val (result1, result2) = LogicalPlanDifference(plan1, plan2, 0)

    // With 0 context lines, should only show the mismatched line
    val lines1 = result1.split("\n").filter(_.nonEmpty)
    val lines2 = result2.split("\n").filter(_.nonEmpty)

    // Should have exactly 1 line (the mismatch)
    assert(lines1.length == 1, s"Expected 1 line, got ${lines1.length}: ${lines1.mkString("; ")}")
    assert(lines2.length == 1, s"Expected 1 line, got ${lines2.length}: ${lines2.mkString("; ")}")

    // Should contain the filter with the different values
    assert(result1.contains(FILTER) && result1.contains("10"))
    assert(result2.contains(FILTER) && result2.contains("99"))

    // Should NOT contain other parts of the plan
    assert(!result1.contains("David"))
    assert(!result2.contains("David"))
    assert(!result1.contains(GLOBAL_LIMIT))
    assert(!result2.contains(GLOBAL_LIMIT))
  }

  test("custom context size - 5 lines") {
    // Very large plan to demonstrate larger context window
    val plan1 = LocalRelation(idAttr, nameAttr, ageAttr)
      .where(idAttr > 0)
      .where(nameAttr =!= "")
      .where(ageAttr > 0)
      .where(ageAttr < 120)
      .where(idAttr < 1000000)
      .where(nameAttr === "Eve") // Mismatch is here (7th line from top)
      .where(ageAttr > 18)
      .where(idAttr % 2 === 0)
      .select(idAttr, nameAttr, ageAttr)
      .limit(500)

    val plan2 = LocalRelation(idAttr, nameAttr, ageAttr)
      .where(idAttr > 0)
      .where(nameAttr =!= "")
      .where(ageAttr > 0)
      .where(ageAttr < 120)
      .where(idAttr < 1000000)
      .where(nameAttr === "Frank") // Different name - mismatch
      .where(ageAttr > 18)
      .where(idAttr % 2 === 0)
      .select(idAttr, nameAttr, ageAttr)
      .limit(500)

    val (result1, result2) = LogicalPlanDifference(plan1, plan2, 5)

    // With 5 context lines, should show 5 before, mismatch, and 5 after (11 total)
    val lines1 = result1.split("\n").filter(_.nonEmpty)
    val lines2 = result2.split("\n").filter(_.nonEmpty)

    assert(lines1.length >= 10, s"Expected at least 10 lines, got ${lines1.length}")
    assert(lines2.length >= 10, s"Expected at least 10 lines, got ${lines2.length}")

    // Should contain the mismatch
    assert(result1.contains("Eve"))
    assert(result2.contains("Frank"))

    // Should contain 5 lines of context before (includes filters with age and id checks)
    assert(result1.contains("120")) // age < 120 filter
    assert(result2.contains("120"))

    // Should contain 5 lines of context after (includes age > 18 and modulo filters)
    assert(result1.contains("18")) // age > 18 filter
    assert(result2.contains("18"))
    assert(result1.contains("% 2")) // modulo check
    assert(result2.contains("% 2"))

    // Should still be truncated (not showing LocalRelation at bottom or GlobalLimit at top)
    assert(!result1.contains(LOCAL_RELATION))
    assert(!result2.contains(LOCAL_RELATION))
  }

  test("plans with different number of lines") {
    // Plan 1 is short (3 operations)
    val plan1 = LocalRelation(idAttr)
      .where(idAttr > 100)
      .select(idAttr)

    // Plan 2 is much longer (8 operations)
    val plan2 = LocalRelation(idAttr, nameAttr, ageAttr)
      .where(idAttr > 0)
      .where(nameAttr === "George")
      .where(ageAttr > 21)
      .where(idAttr < 1000)
      .where(nameAttr =!= "")
      .select(idAttr, nameAttr, ageAttr)
      .limit(100)

    val (result1, result2) = LogicalPlanDifference(plan1, plan2, 2)

    // The mismatch is at the top (Project vs GlobalLimit), so both should show their top portions
    val lines1 = result1.split("\n").filter(_.nonEmpty)
    val lines2 = result2.split("\n").filter(_.nonEmpty)

    assert(lines1.length >= 1, s"Plan1 should have at least 1 line, got ${lines1.length}")
    assert(lines2.length >= 1, s"Plan2 should have at least 1 line, got ${lines2.length}")

    // Both should show their top-level operations
    assert(result1.contains(PROJECT))
    assert(result2.contains(GLOBAL_LIMIT) || result2.contains(LOCAL_LIMIT))

    // Should show some context
    assert(result1.contains(FILTER) && result1.contains("100"))
    assert(result2.contains(PROJECT) || result2.contains(FILTER))
  }

  test("plans where second plan is shorter") {
    // Plan 1 is longer (7 operations)
    val plan1 = LocalRelation(idAttr, nameAttr, ageAttr)
      .where(idAttr > 0)
      .where(nameAttr === "Helen")
      .where(ageAttr < 80)
      .where(idAttr < 500)
      .select(idAttr, nameAttr)
      .limit(50)

    // Plan 2 is shorter (3 operations)
    val plan2 = LocalRelation(idAttr)
      .where(idAttr > 5)
      .select(idAttr)

    val (result1, result2) = LogicalPlanDifference(plan1, plan2, 2)

    val lines1 = result1.split("\n").filter(_.nonEmpty)
    val lines2 = result2.split("\n").filter(_.nonEmpty)

    // Both should have content
    assert(lines1.length >= 1, s"Plan1 should have at least 1 line")
    assert(lines2.length >= 1, s"Plan2 should have at least 1 line")

    // Should show the top-level difference
    assert(result1.contains(GLOBAL_LIMIT) || result1.contains(LOCAL_LIMIT))
    assert(result2.contains(PROJECT))

    // Plan1 should show some of its filters as context
    assert(result1.contains(PROJECT) || result1.contains(FILTER))

    // Plan2 should show its simpler structure
    assert(result2.contains(FILTER) && result2.contains("5"))
  }

  test("complex nested plans") {
    // Create larger subqueries with multiple operations
    val subquery1 = LocalRelation(idAttr, ageAttr)
      .where(idAttr > 5)
      .where(ageAttr > 18)
      .select(idAttr, ageAttr)

    val subquery2 = LocalRelation(idAttr, ageAttr)
      .where(idAttr > 10) // Different threshold - mismatch
      .where(ageAttr > 18)
      .select(idAttr, ageAttr)

    val plan1 = LocalRelation(nameAttr)
      .where(nameAttr =!= "")
      .join(subquery1)
      .where(idAttr < 1000)
      .select(nameAttr, idAttr, ageAttr)
      .limit(200)

    val plan2 = LocalRelation(nameAttr)
      .where(nameAttr =!= "")
      .join(subquery2)
      .where(idAttr < 1000)
      .select(nameAttr, idAttr, ageAttr)
      .limit(200)

    val (result1, result2) = LogicalPlanDifference(plan1, plan2, 2)

    // Should show the mismatch which is in the subquery filter
    assert(result1.contains("5"))
    assert(result2.contains("10"))

    // Should show filter operations around the mismatch
    assert(result1.contains(FILTER))
    assert(result2.contains(FILTER))

    // Verify the outputs are different
    assert(result1 != result2, "Plans should produce different output strings")

    // Should be truncated (not showing all operations)
    val lines1 = result1.split("\n").filter(_.nonEmpty).length
    val lines2 = result2.split("\n").filter(_.nonEmpty).length
    assert(lines1 <= 10, s"Expected truncated output with at most 10 lines, got $lines1")
    assert(lines2 <= 10, s"Expected truncated output with at most 10 lines, got $lines2")
    assert(lines1 >= 1, "Should have at least the mismatch line")
    assert(lines2 >= 1, "Should have at least the mismatch line")
  }

  test("plans with special characters in string representation") {
    val plan1 = LocalRelation(nameAttr)
      .where(nameAttr === "test\nwith\nnewlines")

    val plan2 = LocalRelation(nameAttr)
      .where(nameAttr === "different\nvalue")

    val (result1, result2) = LogicalPlanDifference(plan1, plan2, 2)

    assert(result1.nonEmpty)
    assert(result2.nonEmpty)
  }

  test("empty plans") {
    val plan1 = LocalRelation(Seq.empty)
    val plan2 = LocalRelation(Seq.empty)

    val (result1, result2) = LogicalPlanDifference(plan1, plan2, 2)

    // Empty plans should be equal, so return empty strings
    assert(result1 == "")
    assert(result2 == "")
  }

  test("very large context size should not exceed plan length") {
    val plan1 = LocalRelation(idAttr)
      .where(idAttr > 10)
      .select(idAttr)

    val plan2 = LocalRelation(idAttr)
      .where(idAttr > 20)
      .select(idAttr)

    val (result1, result2) = LogicalPlanDifference(plan1, plan2, 1000)

    // With very large context, should show entire plans
    val lines1 = result1.split("\n").filter(_.nonEmpty).length
    val lines2 = result2.split("\n").filter(_.nonEmpty).length
    val planLines1 = plan1.toString.split("\n").filter(_.nonEmpty).length
    val planLines2 = plan2.toString.split("\n").filter(_.nonEmpty).length

    assert(lines1 <= planLines1)
    assert(lines2 <= planLines2)
  }

  test("mismatch at boundary with minimal context") {
    // Larger plan with mismatch in the middle
    val plan1 = LocalRelation(idAttr, nameAttr, ageAttr)
      .where(idAttr > 0)
      .where(nameAttr =!= "")
      .where(ageAttr > 10) // Mismatch here
      .where(idAttr < 10000)
      .select(idAttr, nameAttr)
      .limit(75)

    val plan2 = LocalRelation(idAttr, nameAttr, ageAttr)
      .where(idAttr > 0)
      .where(nameAttr =!= "")
      .where(ageAttr > 25) // Different value
      .where(idAttr < 10000)
      .select(idAttr, nameAttr)
      .limit(75)

    val (result1, result2) = LogicalPlanDifference(plan1, plan2, 1)

    // With 1 context line, should show 1 line before, the mismatch, and 1 line after (3 total)
    val lines1 = result1.split("\n").filter(_.nonEmpty)
    val lines2 = result2.split("\n").filter(_.nonEmpty)

    assert(
      lines1.length <= 3,
      s"Expected at most 3 lines, got ${lines1.length}: ${lines1.mkString("; ")}"
    )
    assert(
      lines2.length <= 3,
      s"Expected at most 3 lines, got ${lines2.length}: ${lines2.mkString("; ")}"
    )

    // Should contain the mismatch (age filter)
    assert(result1.contains(FILTER) && result1.contains("10"))
    assert(result2.contains(FILTER) && result2.contains("25"))

    // Should contain 1 line of context before (name filter)
    assert(result1.contains("name") || result1.contains("\"\""))
    assert(result2.contains("name") || result2.contains("\"\""))

    // Should contain 1 line of context after (id filter)
    assert(result1.contains("10000"))
    assert(result2.contains("10000"))

    // Should NOT contain operations too far away
    assert(!result1.contains(GLOBAL_LIMIT))
    assert(!result2.contains(GLOBAL_LIMIT))
    assert(!result1.contains(LOCAL_RELATION))
    assert(!result2.contains(LOCAL_RELATION))
  }
}
