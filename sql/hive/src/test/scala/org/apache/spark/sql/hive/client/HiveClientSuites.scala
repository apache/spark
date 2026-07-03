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

package org.apache.spark.sql.hive.client

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.scalatest.{Args, CompositeStatus, DynaTags, FailedStatus, Filter, Status, Suite}

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.catalog.CatalogDatabase
import org.apache.spark.sql.catalyst.util.quietly
import org.apache.spark.sql.hive.HiveUtils
import org.apache.spark.tags.{ExtendedHiveTest, SlowHiveTest}

/**
 * A simple set of tests that call the methods of a [[HiveClient]], loading different version
 * of hive from maven central.  These tests are simple in that they are mostly just testing to make
 * sure that reflective calls are not throwing NoSuchMethod error, but the actually functionality
 * is not fully tested.
 */
@SlowHiveTest
@ExtendedHiveTest
class HiveClientSuites extends SparkFunSuite with HiveClientVersions {

  override protected val enableAutoThreadAudit = false

  import HiveClientBuilder.buildClient

  test("success sanity check") {
    val badClient = buildClient(HiveUtils.builtinHiveVersion, new Configuration())
    val db = CatalogDatabase("default", "desc", new URI("loc"), Map())
    badClient.createDatabase(db, ignoreIfExists = true)
  }

  test("hadoop configuration preserved") {
    val hadoopConf = new Configuration()
    hadoopConf.set("test", "success")
    val client = buildClient(HiveUtils.builtinHiveVersion, hadoopConf)
    assert("success" === client.getConf("test", null))
  }

  test("override useless and side-effect hive configurations") {
    Seq("spark", "tez").foreach { hiveExecEngine =>
      val hadoopConf = new Configuration()
      // These hive flags should be reset by spark
      hadoopConf.setBoolean("hive.cbo.enable", true)
      hadoopConf.setBoolean("hive.session.history.enabled", true)
      hadoopConf.set("hive.execution.engine", hiveExecEngine)
      val client = buildClient(HiveUtils.builtinHiveVersion, hadoopConf)
      assert(!client.getConf("hive.cbo.enable", "true").toBoolean)
      assert(!client.getConf("hive.session.history.enabled", "true").toBoolean)
      assert(client.getConf("hive.execution.engine", hiveExecEngine) === "mr")
    }
  }

  private def getNestedMessages(e: Throwable): String = {
    var causes = ""
    var lastException = e
    while (lastException != null) {
      causes += lastException.toString + "\n"
      lastException = lastException.getCause
    }
    causes
  }

  // Its actually pretty easy to mess things up and have all of your tests "pass" by accidentally
  // connecting to an auto-populated, in-process metastore.  Let's make sure we are getting the
  // versions right by forcing a known compatibility failure.
  // TODO: currently only works on mysql where we manually create the schema...
  ignore("failure sanity check") {
    val e = intercept[Throwable] {
      val badClient = quietly { buildClient("13", new Configuration()) }
    }
    assert(getNestedMessages(e) contains "Unknown column 'A0.OWNER_NAME' in 'field list'")
  }

  // Lazily initialize nested suites to avoid re-creating them on every call
  private lazy val versionedSuites: IndexedSeq[HiveClientSuite] = {
    versions.map(new HiveClientSuite(_))
  }

  override def nestedSuites: IndexedSeq[Suite] = versionedSuites

  // Include nested suite test names in this suite's testNames so that SBT's
  // `-z "keyword"` filtering can find them. Without this, the ScalaTest SBT framework
  // would skip the entire suite when no parent-level test matches the keyword.
  override def testNames: Set[String] = {
    super.testNames ++ versionedSuites.flatMap(_.testNames)
  }

  // Override runTest to delegate to the appropriate nested suite when the test name
  // belongs to a nested suite rather than this suite directly.
  override protected def runTest(testName: String, args: Args): Status = {
    if (super.testNames.contains(testName)) {
      // This is one of our own tests (success sanity check, etc.)
      super.runTest(testName, args)
    } else {
      // This test belongs to a nested suite. We must not call suite.run() directly
      // here because the SBT reporter expects SuiteStarting to have been fired first.
      // Instead, we use the nested suite's runTest directly, but we need to ensure
      // the reporter knows about the nested suite context.
      // The safest approach: call the nested suite's full lifecycle via runNestedSuites.
      versionedSuites.find(_.testNames.contains(testName)) match {
        case Some(suite) =>
          val nestedFilter = Filter(
            tagsToInclude = None,
            args.filter.tagsToExclude,
            excludeNestedSuites = false
          )
          // Run the nested suite with testName specified so only that test executes
          val nestedArgs = args.copy(filter = nestedFilter)
          // Use the full suite lifecycle (SuiteStarting/Completed) by wrapping
          val report = args.reporter
          val tracker = args.tracker
          val suiteClassName = suite.getClass.getName
          val suiteStartTime = System.currentTimeMillis
          import org.scalatest.events._

          report(SuiteStarting(tracker.nextOrdinal(), suite.suiteName, suite.suiteId,
            Some(suiteClassName), None, Some(TopOfClass(suiteClassName)),
            suite.rerunner))
          try {
            val status = suite.run(Some(testName), nestedArgs)
            val duration = System.currentTimeMillis - suiteStartTime
            status.unreportedException match {
              case Some(ue) =>
                report(SuiteAborted(tracker.nextOrdinal(), ue.getMessage, suite.suiteName,
                  suite.suiteId, Some(suiteClassName), Some(ue), Some(duration),
                  None, Some(SeeStackDepthException), suite.rerunner))
                FailedStatus
              case None =>
                report(SuiteCompleted(tracker.nextOrdinal(), suite.suiteName, suite.suiteId,
                  Some(suiteClassName), Some(duration), None,
                  Some(TopOfClass(suiteClassName)), suite.rerunner))
                status
            }
          } catch {
            case e: RuntimeException =>
              val duration = System.currentTimeMillis - suiteStartTime
              report(SuiteAborted(tracker.nextOrdinal(), e.getMessage, suite.suiteName,
                suite.suiteId, Some(suiteClassName), Some(e), Some(duration),
                None, Some(SeeStackDepthException), suite.rerunner))
              FailedStatus
          }
        case None =>
          throw new IllegalArgumentException(s"Test not found: $testName")
      }
    }
  }

  // Override run to handle both filtered and full test runs correctly:
  // - When testName is Some (from -z/-t filter via dynaTags): runTests calls our
  //   runTest override which delegates to the appropriate nested suite.
  // - When testName is None (full run or `test` task): run only our own tests via
  //   runTests, then run all nested suites via runNestedSuites. We must avoid calling
  //   runTests with testName=None naively because our testNames override includes
  //   nested suite tests, which would cause double execution.
  override def run(testName: Option[String], args: Args): Status = {
    testName match {
      case Some(_) =>
        // Specific test requested - runTests will filter and call our runTest override
        runTests(testName, args)
      case None =>
        // Full run - run our own tests only (filter against super.testNames)
        val ownTests = super.testNames
        val statusBuffer = new scala.collection.mutable.ListBuffer[Status]()
        for ((tn, ignoreTest) <- args.filter(ownTests, tags, suiteId)) {
          if (!args.stopper.stopRequested) {
            if (!ignoreTest) {
              statusBuffer += super.runTest(tn, args)
            }
          }
        }
        val testsStatus = new CompositeStatus(Set.empty ++ statusBuffer)

        // Then run all nested suites with proper filtering.
        // When -z filtering is active, the SBT framework sets
        // tagsToInclude=Some(Set("Selected")) with dynaTags keyed by THIS suite's
        // suiteId. Since our testNames override includes nested suite test names,
        // the matching tests are in our dynaTags. We redistribute these tags to
        // each nested suite by their own suiteId.
        val selectedTag = "org.scalatest.Selected"
        val isSelectedFiltering = args.filter.tagsToInclude.exists(_.contains(selectedTag))

        val nestedSuitesStatus = if (isSelectedFiltering) {
          val myTestTags = args.filter.dynaTags.testTags.getOrElse(suiteId, Map.empty)
          val nestedSuitesArray = nestedSuites.toArray
          val nestedStatusBuffer = new scala.collection.mutable.ListBuffer[Status]()
          val report = args.reporter
          val tracker = args.tracker

          for (nestedSuite <- nestedSuitesArray) {
            if (!args.stopper.stopRequested) {
              // Find which tests in this nested suite were "Selected" in our dynaTags
              val nestedTestNames = nestedSuite.testNames
              val selectedForNested = myTestTags.filter {
                case (testName, tagSet) =>
                  tagSet.contains(selectedTag) && nestedTestNames.contains(testName)
              }
              if (selectedForNested.nonEmpty) {
                val nestedFilter = Filter(
                  tagsToInclude = Some(Set(selectedTag)),
                  args.filter.tagsToExclude,
                  excludeNestedSuites = false,
                  dynaTags = DynaTags(Map.empty, Map(nestedSuite.suiteId -> selectedForNested))
                )
                val nestedArgs = args.copy(filter = nestedFilter)
                // Replicate runNestedSuites lifecycle: SuiteStarting -> run -> SuiteCompleted
                val suiteClassName = nestedSuite.getClass.getName
                val suiteStartTime = System.currentTimeMillis
                import org.scalatest.events._
                report(SuiteStarting(tracker.nextOrdinal(), nestedSuite.suiteName,
                  nestedSuite.suiteId, Some(suiteClassName), None,
                  Some(TopOfClass(suiteClassName)), nestedSuite.rerunner))
                try {
                  val status = nestedSuite.run(None, nestedArgs)
                  val duration = System.currentTimeMillis - suiteStartTime
                  status.unreportedException match {
                    case Some(ue) =>
                      report(SuiteAborted(tracker.nextOrdinal(), ue.getMessage,
                        nestedSuite.suiteName, nestedSuite.suiteId, Some(suiteClassName),
                        Some(ue), Some(duration), None, Some(SeeStackDepthException),
                        nestedSuite.rerunner))
                      nestedStatusBuffer += FailedStatus
                    case None =>
                      report(SuiteCompleted(tracker.nextOrdinal(), nestedSuite.suiteName,
                        nestedSuite.suiteId, Some(suiteClassName), Some(duration), None,
                        Some(TopOfClass(suiteClassName)), nestedSuite.rerunner))
                      nestedStatusBuffer += status
                  }
                } catch {
                  case e: RuntimeException =>
                    val eMessage = e.getMessage
                    val rawString = if (eMessage != null && eMessage.nonEmpty) eMessage
                      else e.getClass.getName
                    val duration = System.currentTimeMillis - suiteStartTime
                    report(SuiteAborted(tracker.nextOrdinal(), rawString,
                      nestedSuite.suiteName, nestedSuite.suiteId, Some(suiteClassName),
                      Some(e), Some(duration), None, Some(SeeStackDepthException),
                      nestedSuite.rerunner))
                    nestedStatusBuffer += FailedStatus
                }
              }
            }
          }
          new CompositeStatus(Set.empty ++ nestedStatusBuffer)
        } else {
          // No -z/-t filter, run all nested suites normally via runNestedSuites
          // which handles SuiteStarting/SuiteCompleted event reporting
          val nestedFilter = Filter(
            tagsToInclude = None,
            args.filter.tagsToExclude,
            excludeNestedSuites = false
          )
          val nestedArgs = args.copy(filter = nestedFilter)
          runNestedSuites(nestedArgs)
        }
        new CompositeStatus(Set(testsStatus, nestedSuitesStatus))
    }
  }
}
