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

import java.io.PrintWriter

import scala.collection.mutable

import sbt._
import sbt.Keys._
import sbt.plugins.JvmPlugin

//noinspection ScalaStyle
object CirclePlugin extends AutoPlugin {
  lazy val Circle = config("circle").extend(Test).hide

  case class ProjectTests(project: ProjectRef, tests: Seq[TestDefinition])

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

  private[CirclePlugin] case class ProjectAndTest(project: ProjectRef, test: TestDefinition)

  override def globalSettings: Seq[Def.Setting[_]] = List(
    circleTestsByProject := {
      if (sys.env contains "CIRCLE_NODE_INDEX") {
        val byProject: Seq[ProjectTests] = testsByProject.all(ScopeFilter(inAnyProject, inConfigurations(Test))).value

        val allTestsByName = byProject
          .flatMap(pt => pt.tests.iterator.map(ProjectAndTest(pt.project, _)))
          .groupBy(_.test.name)

        // Don't want to use SBT's process class
        import sys.process._
        val lines = new mutable.ArrayBuffer[String]
        val processIO = new ProcessIO(out => {
          val pw = new PrintWriter(out)
          try {
            // Might these to be sorted stably, so sort by class name
            allTestsByName.keys.toSeq.sorted.foreach(pw.println)
          } finally {
            pw.close()
          }
        }, BasicIO.processFully(lines += _), BasicIO.toStdErr)
        val builder = Process(List("circleci", "tests", "split", "--split-by=timings",
          "--timings-type=classname"))
        val exitCode = builder.run(processIO).exitValue()
        require(exitCode == 0, s"circleci process failed: $builder")

        // Read out all the class names that it generated
        val out = lines.iterator
          .flatMap(className => allTestsByName.getOrElse(
            className, sys.error(s"Could not find class name in allTestsByName: $className")))
          .toSeq
          .groupBy(_.project)
          .iterator
          .map { case (project, projectAndTests) => ProjectTests(project, projectAndTests.map(_.test)) }
          .toSeq

        Some(out)
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
    // To test, compare:
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
