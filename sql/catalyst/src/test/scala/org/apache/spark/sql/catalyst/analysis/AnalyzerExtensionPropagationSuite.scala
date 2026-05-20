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

package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.analysis.resolver.{ResolverExtension, TreeNodeResolver}
import org.apache.spark.sql.catalyst.catalog.{InMemoryCatalog, SessionCatalog}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.catalog.CatalogManager

/**
 * Verifies that [[Analyzer.withCatalogManager]] propagates all extension points.
 *
 * If this suite fails with an unexpected method count, a new extension point was added to
 * [[Analyzer.withCatalogManager]] without being verified here. Add the corresponding assertion
 * and update the expected count.
 *
 * If [[Analyzer]] gains a new extension point that is NOT yet in [[Analyzer.withCatalogManager]],
 * add it there first, then update this suite.
 */
class AnalyzerExtensionPropagationSuite extends SparkFunSuite {

  private val dummyRule: Rule[LogicalPlan] = new Rule[LogicalPlan] {
    override def apply(plan: LogicalPlan): LogicalPlan = plan
  }

  private val dummyCheck: LogicalPlan => Unit = (_: LogicalPlan) => ()

  private val dummyExtension: ResolverExtension = new ResolverExtension {
    override def resolveOperator(
        operator: LogicalPlan,
        resolver: TreeNodeResolver[LogicalPlan, LogicalPlan]): Option[LogicalPlan] = None
  }

  private def newCatalogManager(): CatalogManager =
    new CatalogManager(
      FakeV2SessionCatalog,
      new SessionCatalog(new InMemoryCatalog, EmptyFunctionRegistry))

  test("withCatalogManager propagates all extension points") {
    val analyzer = new Analyzer(newCatalogManager()) {
      override val hintResolutionRules: Seq[Rule[LogicalPlan]] = Seq(dummyRule)
      override val extendedResolutionRules: Seq[Rule[LogicalPlan]] = Seq(dummyRule)
      override val postHocResolutionRules: Seq[Rule[LogicalPlan]] = Seq(dummyRule)
      override val extendedCheckRules: Seq[LogicalPlan => Unit] = Seq(dummyCheck)
      override val singlePassResolverExtensions: Seq[ResolverExtension] = Seq(dummyExtension)
      override val singlePassMetadataResolverExtensions: Seq[ResolverExtension] =
        Seq(dummyExtension)
      override val singlePassPostHocResolutionRules: Seq[Rule[LogicalPlan]] = Seq(dummyRule)
      override val singlePassExtendedResolutionChecks: Seq[LogicalPlan => Unit] = Seq(dummyCheck)
    }

    val clone = analyzer.withCatalogManager(newCatalogManager())

    assert(clone.hintResolutionRules eq analyzer.hintResolutionRules)
    assert(clone.extendedResolutionRules eq analyzer.extendedResolutionRules)
    assert(clone.postHocResolutionRules eq analyzer.postHocResolutionRules)
    assert(clone.extendedCheckRules eq analyzer.extendedCheckRules)
    assert(clone.singlePassResolverExtensions eq analyzer.singlePassResolverExtensions)
    assert(clone.singlePassMetadataResolverExtensions eq
      analyzer.singlePassMetadataResolverExtensions)
    assert(clone.singlePassPostHocResolutionRules eq analyzer.singlePassPostHocResolutionRules)
    assert(clone.singlePassExtendedResolutionChecks eq analyzer.singlePassExtendedResolutionChecks)

    // Verify the clone's anonymous class overrides exactly the expected extension points.
    // If this assertion fails, withCatalogManager was updated but this test was not.
    // Add the corresponding assert above and update the expected set.
    val overriddenMethods = clone.getClass.getDeclaredMethods
      .filterNot(m => m.isSynthetic || m.isBridge || m.getName.contains("$"))
      .map(_.getName)
      .toSet

    val expectedExtensions = Set(
      "hintResolutionRules",
      "extendedResolutionRules",
      "postHocResolutionRules",
      "extendedCheckRules",
      "singlePassResolverExtensions",
      "singlePassMetadataResolverExtensions",
      "singlePassPostHocResolutionRules",
      "singlePassExtendedResolutionChecks"
    )

    assert(overriddenMethods == expectedExtensions,
      s"withCatalogManager does not copy the expected set of extension points. " +
      s"Missing from withCatalogManager: ${expectedExtensions -- overriddenMethods}. " +
      s"Unexpected overrides: ${overriddenMethods -- expectedExtensions}.")
  }
}
