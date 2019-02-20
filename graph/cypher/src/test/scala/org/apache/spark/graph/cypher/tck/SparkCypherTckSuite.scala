package org.apache.spark.graph.cypher.tck

import java.io.File

import org.apache.spark.SparkFunSuite
import org.apache.spark.graph.cypher.{SharedCypherContext, SparkCypherSession}
import org.apache.spark.graph.cypher.construction.ScanGraphFactory
import org.opencypher.okapi.tck.test.Tags.{BlackList, WhiteList}
import org.opencypher.okapi.tck.test.{ScenariosFor, TCKGraph}
import org.opencypher.okapi.testing.propertygraph.CypherTestGraphFactory
import org.opencypher.tools.tck.api.CypherTCK
import org.scalatest.Tag
import org.scalatest.prop.TableDrivenPropertyChecks._

import scala.io.Source
import scala.util.{Failure, Success, Try}

class SparkCypherTckSuite extends SparkFunSuite with SharedCypherContext {

  object TckCapsTag extends Tag("TckSparkCypher")

  private val graphFactory: CypherTestGraphFactory[SparkCypherSession] = ScanGraphFactory

  private val failingBlacklist = getClass.getResource("/tck/failing_blacklist").getFile
  private val temporalBlacklist = getClass.getResource("/tck/temporal_blacklist").getFile
  private val wontFixBlacklistFile = getClass.getResource("/tck/wont_fix_blacklist").getFile
  private val failureReportingBlacklistFile = getClass.getResource("/tck/failure_reporting_blacklist").getFile
  private val scenarios = ScenariosFor(failingBlacklist, temporalBlacklist, wontFixBlacklistFile, failureReportingBlacklistFile)

  forAll(scenarios.whiteList) { scenario =>
    test(s"[${graphFactory.name}, ${WhiteList.name}] $scenario", WhiteList, TckCapsTag, Tag(graphFactory.name)) {
      scenario(TCKGraph(graphFactory, cypherEngine.graphs.empty)).execute()
    }
  }

  forAll(scenarios.blackList) { scenario =>
    test(s"[${graphFactory.name}, ${BlackList.name}] $scenario", BlackList, TckCapsTag) {
      val tckGraph = TCKGraph(graphFactory, cypherEngine.graphs.empty)

      Try(scenario(tckGraph).execute()) match {
        case Success(_) =>
          throw new RuntimeException(s"A blacklisted scenario passed: $scenario")
        case Failure(_) =>
      }
    }
  }

  test("compute TCK coverage") {
    val failingScenarios = Source.fromFile(failingBlacklist).getLines().size
    val failingTemporalScenarios = Source.fromFile(temporalBlacklist).getLines().size
    val failureReportingScenarios = Source.fromFile(failureReportingBlacklistFile).getLines().size

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
      .foreach(scenario => scenario(TCKGraph(graphFactory, cypherEngine.graphs.empty)).execute())
  }

  ignore("run single scenario") {
    scenarios.get("Should add or subtract duration to or from date")
      .foreach(scenario => scenario(TCKGraph(graphFactory, cypherEngine.graphs.empty)).execute())
  }
}

