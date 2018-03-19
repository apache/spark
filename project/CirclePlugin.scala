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

import scala.annotation.tailrec

import com.fasterxml.jackson.core.JsonToken
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import sbt.Keys._
import sbt._
import sbt.plugins.JvmPlugin
import scalaz.Dequeue

//noinspection ScalaStyle
object CirclePlugin extends AutoPlugin {
  lazy val Circle = config("circle").extend(Test).hide

  case class ProjectTests(project: ProjectRef, tests: Seq[TestDefinition])
  case class ProjectTest(project: ProjectRef, test: TestDefinition)

  val circleTestsByProject = taskKey[Option[Seq[ProjectTests]]]("The tests that should be run under this circle node, if circle is set up")
  val copyTestReportsToCircle: TaskKey[Boolean] = taskKey("Copy the test reports to circle. Expects CIRCLE_TEST_REPORTS to be defined")

  override def projectConfigurations: Seq[Configuration] = List(Circle)

  override def requires: Plugins = JvmPlugin

  override def trigger: PluginTrigger = allRequirements

  private[this] lazy val testsByProject = Def.task {
    // Defaults.detectTests is basically the value of Keys.definedTests, but since we're
    // overriding the latter depending on the value of this task, we can't depend on it
    ProjectTests(thisProjectRef.value, Defaults.detectTests.value)
  }

  private[this] lazy val mapper = new ObjectMapper() with ScalaObjectMapper
  mapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
  mapper.registerModule(DefaultScalaModule)
  case class TestResult(classname: String, result: String, run_time: Double, source: String)

  case class TestKey(source: String, classname: String)

  // Through this magical command we established that the average class run_time is 7.737
  // jq <~/results.json '.tests | map(select(.result != "skipped")) | group_by(.classname) | map(map(.run_time) | add) | (add / length)'
  private[this] val AVERAGE_TEST_CLASS_RUN_TIME = 7.737d

  override def globalSettings: Seq[Def.Setting[_]] = List(
    circleTestsByProject := {
      val log = streams.value.log

      if (sys.env contains "CIRCLE_NODE_INDEX") {
        val index = sys.env("CIRCLE_NODE_INDEX").toInt
        val totalNodes = sys.env("CIRCLE_NODE_TOTAL").toInt
        val byProject: Seq[ProjectTests] = testsByProject.all(ScopeFilter(inAnyProject, inConfigurations(Test))).value

        val testsByKey = byProject.iterator
            .flatMap(pt => pt.tests.map(ProjectTest(pt.project, _)))
            .toStream
            .groupBy(pt => TestKey(pt.project.project, pt.test.name))

        log.info(s"Discovered tests in these projects: ${byProject.map(_.project.project)}")

        import collection.JavaConverters._
        // Get timings and sum them up by TestKey = (source, classname)
        val testResultsFile = sys.env.get("CIRCLE_INTERNAL_TASK_DATA")
            .map(taskData => file(taskData) / "circle-test-results/results.json")
            .filter(file => file.exists())
        val testTimings = try {
          testResultsFile.fold {
            log.warn("Couldn't find circle test results file, using naive test packing")
          } {
            file => log.info(s"Using circle test results to determine test packing: $file")
          }
          testResultsFile
              .map(file => {
                val parser = mapper.getFactory.createParser(file)
                require(
                  parser.nextToken() == JsonToken.START_OBJECT,
                  s"Unexpected first token: ${parser.currentToken()}")
                require(parser.nextToken() == JsonToken.FIELD_NAME)
                require(parser.getText == "tests")
                require(parser.nextToken() == JsonToken.START_ARRAY)
                parser.nextToken()
                mapper.readValues[TestResult](parser)
              })
              .map(_.asScala)
              .getOrElse(Iterator())
              .toStream
              .filter(_.result != "skipped")  // don't count timings of tests that didn't run
              .groupBy(result => TestKey(result.source, result.classname))
              .mapValues(_.foldLeft(0.0d)(_ + _.run_time))
        } catch {
          case e: Exception =>
            log.warn(f"Couldn't read test results file: $testResultsFile")
            log.trace(e)
            Map.empty[TestKey, Nothing]
        }

        val allTestsTimings = testsByKey.keys
            .iterator
            .map(key => key -> testTimings.getOrElse(key, AVERAGE_TEST_CLASS_RUN_TIME))
            .toMap

        val testsWithoutTimings = testsByKey.keySet diff testTimings.keySet
        log.info(s"${testsWithoutTimings.size} / ${testsByKey.size} test classes didn't have timings in "
            + "the previous test results file")
        if (testTimings.nonEmpty) {
          val displayMax = 10
          testsWithoutTimings.groupBy(_.source)
              .foreach { case (proj, tests) =>
                if (tests.nonEmpty) {
                  val remaining = tests.size - displayMax
                  val text = (tests.iterator.map(_.classname)
                      .take(displayMax)
                      .mkString(", ")
                      + { if (remaining > 0) s", ... ($remaining more)" else "" })
                  log.info(s"Un-timed tests for project $proj: $text")
                }
              }
        }

        val totalTestTime = allTestsTimings.valuesIterator.sum
        log.info(s"Estimated total test time to be $totalTestTime")
        val timePerNode = totalTestTime / totalNodes

        // Now, do bin packing. Sort first by runTime, then by key, to get a stable sort.
        implicit val testKeyOrdering = Ordering.by((tk: TestKey) => (tk.source, tk.classname))
        val tests = Dequeue[(TestKey, Double)](
            allTestsTimings.toIndexedSeq.sortBy { case (key, runTime) => (runTime, key) } : _*)

        @tailrec
        def process(tests: Dequeue[(TestKey, Double)],
                    soFar: Double = 0d,
                    takeLeft: Boolean = true,
                    acc: List[TestKey] = Nil,
                    groups: List[List[TestKey]] = Nil): List[List[TestKey]] = {

          if (groups.size == totalNodes || tests.isEmpty) {
            // Short circuit the logic if we've just completed the last group
            // (or finished all the tests early somehow)
            require(acc.isEmpty)
            return if (tests.isEmpty) {
              groups
            } else {
              // Fit all remaining tests in the last issued bucket.
              val lastGroup +: restGroups = groups
              val toFit = tests.toStream.map(_._1).force
              log.info(s"Fitting remaining tests into first bucket (which already has " +
                  s"${lastGroup.size} tests): $toFit")
              (toFit ++: lastGroup) :: restGroups
            }
          }

          case class TestCandidate[A](test: A, rest: Dequeue[A], fromLeft: Boolean)
          /** Converts a uncons/unsnoc'd value to a [[TestCandidate]]. */
          def toCandidate[A](fromLeft: Boolean): ((A, Dequeue[A])) => TestCandidate[A] = {
            case (test, rest) => TestCandidate(test, rest, fromLeft)
          }

          val candidates = ((
              if (takeLeft)
                Stream(tests.uncons.map(toCandidate(true)))
              else
                Stream.empty)
              #::: tests.unsnoc.map(toCandidate(false))
              #:: Stream.empty)
              .flatMap(_.toOption)

          candidates
              .collectFirst {
                case x@TestCandidate((_, runTime), _, _) if soFar + runTime <= timePerNode => x
              } match {
                case None =>
                  process(tests, 0d, takeLeft = true, Nil, acc :: groups)
                case Some(TestCandidate((key, runTime), rest, fromLeft)) =>
                  process(rest, soFar + runTime, fromLeft, key :: acc, groups)
            }
        }

        val buckets = process(tests)
        val rootTarget = (target in LocalRootProject).value
        val bucketsFile = rootTarget / "tests-by-bucket.json"
        log.info(s"Saving test distribution into $totalNodes buckets to: $bucketsFile")
        mapper.writeValue(bucketsFile, buckets)
        val timingsPerBucket = buckets.map(_.iterator.map(allTestsTimings.apply).sum)
        log.info(s"Estimated test timings per bucket: $timingsPerBucket")

        val bucket = buckets.lift.apply(index).getOrElse(Nil)

        val groupedByProject = bucket.flatMap(testsByKey.apply)
            .groupBy(_.project)
            .mapValues(_.map(_.test))
            .iterator
            .map { case (proj, tests) => ProjectTests(proj, tests) }
            .toIndexedSeq

        Some(groupedByProject)
      } else {
        None
      }
    }
  )

  override def projectSettings: Seq[Def.Setting[_]] = inConfig(Circle)(Defaults.testSettings ++ List(
    // Copy over important changes of the += kind from TestSettings.settings into the Circle config
    envVars := (envVars in Test).value,
    javaOptions := (javaOptions in Test).value,
    testOptions := (testOptions in Test).value,
    resourceGenerators := (resourceGenerators in Test).value,
    // NOTE: this is because of dependencies like:
    //   org.apache.spark:spark-tags:2.3.0-SNAPSHOT:test->test
    // That somehow don't get resolved properly in the 'circle' ivy configuration even though it extends test
    // To test, copare:
    // > show unsafe/test:fullClasspath
    // > show unsafe/circle:fullClasspath
    fullClasspath := (fullClasspath in Test).value,

    copyTestReportsToCircle := {
      val log = streams.value.log
      val reportsDir = target.value / "test-reports"
      val circleReports = sys.env.get("CIRCLE_TEST_REPORTS")
      val projectName = thisProjectRef.value.project
      val `project had tests for this circle node` = definedTests.value.nonEmpty

      circleReports.map { circle =>
        if (!reportsDir.exists()) {
          if (`project had tests for this circle node`) {
            sys.error(s"Found no test reports from $projectName to circle, " +
              "though there were tests defined for this node.")
          } else {
            // There were no tests for this node, do nothing.
            false
          }
        } else {
          IO.copyDirectory(reportsDir, file(circle) / projectName)
          log.info(s"Copied test reports from $projectName to circle.")
          true
        }
      }.getOrElse(sys.error(s"Expected CIRCLE_TEST_REPORTS to be defined."))
    },

    definedTests := {
      val testsByProject = (circleTestsByProject in Global).value
                           .getOrElse(sys.error("We are not running in circle."))
      val thisProj = thisProjectRef.value
      val log = streams.value.log

      testsByProject.collectFirst {
        case ProjectTests(`thisProj`, tests) => tests
      }.getOrElse {
        log.info(s"Didn't find any tests for $thisProj in the global circleTestsByProject. Skipping")
        List()
      }
    },

    test := (test, copyTestReportsToCircle) { (test, copy) =>
      test.doFinally(copy.map(_ => ()))
    }.value
  ))
}
