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

import scala.util.control.NonFatal

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

  // Override run to handle both filtered and full test runs correctly.
  //
  // The ScalaTest SBT framework always invokes a suite's run with testName == None and
  // encodes any `-z` / `-t` selection as a "Selected" tag plus per-test dynaTags keyed by
  // the suite's suiteId (see the workaround for
  // https://github.com/scalatest/scalatest/issues/2375). Because our testNames override
  // exposes the nested suites' test names, those selections land in *this* suite's dynaTags
  // rather than the nested suites'. This override redistributes them:
  //   - run our own tests via super.runTest (filtered against super.testNames only), then
  //   - for each nested suite, re-key the matching dynaTags to that nested suite's suiteId
  //     and run it once.
  // Own tests and nested-suite tests are dispatched separately so a full run never
  // double-executes (our testNames intentionally includes the nested names).
  //
  // NOTE: the nested HiveClientSuite tests are order-dependent (they share a single
  // `client`/`versionSpark` built in beforeAll, and e.g. table/partition/function tests
  // rely on state created by earlier tests). Selecting a single order-dependent test with
  // `-z` will run it in isolation and may fail because its prerequisites were filtered out.
  // Independent tests (e.g. "create client") can be selected safely. Making every test
  // self-contained is out of scope for this change.
  override def run(testName: Option[String], args: Args): Status = {
    testName match {
      case Some(_) =>
        // Not exercised by the SBT framework (it uses testName == None + Selected tag, see
        // above); delegate to the standard implementation as a fallback for other runners.
        super.run(testName, args)
      case None =>
        // Run our own tests only (filter against super.testNames, not the nested names).
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

        // Then run the nested suites. When a `-z`/`-t` selection is active the SBT framework
        // sets tagsToInclude = Some(Set("Selected")) with dynaTags keyed by THIS suite's
        // suiteId; we redistribute those tags to each nested suite by its own suiteId.
        val selectedTag = "org.scalatest.Selected"
        val isSelectedFiltering = args.filter.tagsToInclude.exists(_.contains(selectedTag))

        val nestedSuitesStatus = if (isSelectedFiltering) {
          val myTestTags = args.filter.dynaTags.testTags.getOrElse(suiteId, Map.empty)
          val nestedStatusBuffer = new scala.collection.mutable.ListBuffer[Status]()

          for (nestedSuite <- nestedSuites) {
            if (!args.stopper.stopRequested) {
              // Find which tests in this nested suite were "Selected" in our dynaTags.
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
                nestedStatusBuffer +=
                  runNestedSuiteWithEvents(nestedSuite, args.copy(filter = nestedFilter), args)
              }
            }
          }
          new CompositeStatus(Set.empty ++ nestedStatusBuffer)
        } else {
          // No `-z`/`-t` selection: run all nested suites normally via runNestedSuites,
          // which handles SuiteStarting/SuiteCompleted event reporting itself.
          val nestedFilter = Filter(
            tagsToInclude = None,
            args.filter.tagsToExclude,
            excludeNestedSuites = false
          )
          runNestedSuites(args.copy(filter = nestedFilter))
        }
        new CompositeStatus(Set(testsStatus, nestedSuitesStatus))
    }
  }

  // Run a single nested suite while emitting the SuiteStarting/SuiteCompleted/SuiteAborted
  // events that runNestedSuites would normally emit. We cannot use runNestedSuites directly
  // here because we need to pass a per-suite Filter (with re-keyed dynaTags) to each nested
  // suite. `runArgs` carries the reporter/tracker; `nestedArgs` carries the per-suite filter.
  private def runNestedSuiteWithEvents(
      nestedSuite: Suite, nestedArgs: Args, runArgs: Args): Status = {
    import org.scalatest.events._
    val report = runArgs.reporter
    val tracker = runArgs.tracker
    val suiteClassName = nestedSuite.getClass.getName
    val suiteStartTime = System.currentTimeMillis
    report(SuiteStarting(tracker.nextOrdinal(), nestedSuite.suiteName, nestedSuite.suiteId,
      Some(suiteClassName), None, Some(TopOfClass(suiteClassName)), nestedSuite.rerunner))
    try {
      val status = nestedSuite.run(None, nestedArgs)
      val duration = System.currentTimeMillis - suiteStartTime
      status.unreportedException match {
        case Some(ue) =>
          report(SuiteAborted(tracker.nextOrdinal(), ue.getMessage, nestedSuite.suiteName,
            nestedSuite.suiteId, Some(suiteClassName), Some(ue), Some(duration),
            None, Some(SeeStackDepthException), nestedSuite.rerunner))
          FailedStatus
        case None =>
          report(SuiteCompleted(tracker.nextOrdinal(), nestedSuite.suiteName,
            nestedSuite.suiteId, Some(suiteClassName), Some(duration), None,
            Some(TopOfClass(suiteClassName)), nestedSuite.rerunner))
          status
      }
    } catch {
      case NonFatal(e) =>
        val eMessage = e.getMessage
        val rawString = if (eMessage != null && eMessage.nonEmpty) eMessage
          else e.getClass.getName
        val duration = System.currentTimeMillis - suiteStartTime
        report(SuiteAborted(tracker.nextOrdinal(), rawString, nestedSuite.suiteName,
          nestedSuite.suiteId, Some(suiteClassName), Some(e), Some(duration),
          None, Some(SeeStackDepthException), nestedSuite.rerunner))
        FailedStatus
    }
  }
}
