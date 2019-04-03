package org.apache.spark.cypher.tck

import java.io.File

import org.apache.spark.SparkFunSuite
import org.apache.spark.cypher.construction.ScanGraphFactory
import org.apache.spark.cypher.{SharedCypherContext, SparkCypherSession}
import org.opencypher.okapi.tck.test.Tags.{BlackList, WhiteList}
import org.opencypher.okapi.tck.test.{ScenariosFor, TCKGraph}
import org.opencypher.okapi.testing.propertygraph.CypherTestGraphFactory
import org.opencypher.tools.tck.api.CypherTCK
import org.scalatest.Tag
import org.scalatest.prop.TableDrivenPropertyChecks._

import scala.io.Source
import scala.util.{Failure, Success, Try}

class SparkCypherTckSuite extends SparkFunSuite with SharedCypherContext {

  private val tckSparkCypherTag = Tag("TckSparkCypher")

  private val graphFactory: CypherTestGraphFactory[SparkCypherSession] = ScanGraphFactory

  private val failingBlacklist = getClass.getResource("/tck/failing_blacklist").getFile
  private val temporalBlacklist = getClass.getResource("/tck/temporal_blacklist").getFile
  private val wontFixBlacklist = getClass.getResource("/tck/wont_fix_blacklist").getFile
  private val failureReportingBlacklist = getClass.getResource("/tck/failure_reporting_blacklist").getFile
  private val scenarios = ScenariosFor(failingBlacklist, temporalBlacklist, wontFixBlacklist, failureReportingBlacklist)

  forAll(scenarios.whiteList) { scenario =>
    test(s"[${WhiteList.name}] $scenario", WhiteList, tckSparkCypherTag, Tag(graphFactory.name)) {
      scenario(TCKGraph(graphFactory, internalCypherSession.graphs.empty)(internalCypherSession)).execute()
    }
  }

  forAll(scenarios.blackList) { scenario =>
    test(s"[${graphFactory.name}, ${BlackList.name}] $scenario", BlackList, tckSparkCypherTag) {
      val tckGraph = TCKGraph(graphFactory, internalCypherSession.graphs.empty)(internalCypherSession)

      Try(scenario(tckGraph).execute()) match {
        case Success(_) =>
          throw new RuntimeException(s"A blacklisted scenario passed: $scenario")
        case Failure(_) =>
      }
    }
  }

  test("compute TCK coverage") {
    def withSource[T](s: Source)(f: Source => T) = try { f(s) } finally { s.close() }

    val failingScenarios = withSource(Source.fromFile(failingBlacklist))(_.getLines().size)
    val failingTemporalScenarios = withSource(Source.fromFile(temporalBlacklist))(_.getLines().size)
    val failureReportingScenarios = withSource(Source.fromFile(failureReportingBlacklist))(_.getLines().size)

    val white = scenarios.whiteList.groupBy(_.featureName).mapValues(_.size)
    val black = scenarios.blackList.groupBy(_.featureName).mapValues(_.size)

    val allFeatures = white.keySet ++ black.keySet
    val perFeatureCoverage = allFeatures.foldLeft(Map.empty[String, Float]) {
      case (acc, feature) =>
        val w = white.getOrElse(feature, 0).toFloat
        val b = black.getOrElse(feature, 0).toFloat
        val percentage = (w / (w + b)) * 100
        acc.updated(feature, percentage)
    }

    val allScenarios = scenarios.blacklist.size + scenarios.whiteList.size.toFloat
    val readOnlyScenarios = scenarios.whiteList.size + failingScenarios + failureReportingScenarios.toFloat + failingTemporalScenarios
    val smallReadOnlyScenarios = scenarios.whiteList.size + failingScenarios.toFloat

    val overallCoverage = scenarios.whiteList.size / allScenarios
    val readOnlyCoverage = scenarios.whiteList.size / readOnlyScenarios
    val smallReadOnlyCoverage = scenarios.whiteList.size / smallReadOnlyScenarios

    val featureCoverageReport =
      perFeatureCoverage.map { case (feature, coverage) => s" $feature: $coverage%" }.mkString("\n")

    val report =
      s"""|TCK Coverage
          |------------
          |
          | Complete: ${overallCoverage * 100}%
          | Read Only: ${readOnlyCoverage * 100}%
          | Read Only (without Failure case Scenarios and temporal): ${smallReadOnlyCoverage * 100}%
          |
          |Feature Coverage
          |----------------
          |
          |$featureCoverageReport
    """.stripMargin

    println(report)

  }

  ignore("run custom scenario") {
    val file = new File(getClass.getResource("CustomTest.feature").toURI)

    CypherTCK
      .parseFilesystemFeature(file)
      .scenarios
      .foreach(scenario => scenario(TCKGraph(graphFactory, internalCypherSession.graphs.empty)(internalCypherSession)).execute())
  }

  ignore("run single scenario") {
    scenarios.get("Should add or subtract duration to or from date")
      .foreach(scenario => scenario(TCKGraph(graphFactory, internalCypherSession.graphs.empty)(internalCypherSession)).execute())
  }
}
