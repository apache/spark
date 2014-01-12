package catalyst
package execution

import java.io._
import org.scalatest.{BeforeAndAfterAll, FunSuite, GivenWhenThen}

import frontend.hive.{ExplainCommand, Command}
import util._

/**
 * Allows the creations of tests that execute the same query against both hive
 * and catalyst, comparing the results.
 *
 * The "golden" results from Hive are cached in an retrieved both from the classpath and
 * [[answerCache]] to speed up testing.
 *
 * TODO(marmbrus): Document system properties.
 */
abstract class HiveComparisonTest extends FunSuite with BeforeAndAfterAll with GivenWhenThen with Logging {
  protected val targetDir = new File("target")

  /** The local directory with cached golden answer will be stored. */
  protected val answerCache = new File(targetDir, "comparison-test-cache")
  if (!answerCache.exists)
    answerCache.mkdir()

  /** The [[ClassLoader]] that contains test dependencies.  Used to look for golden answers. */
  protected val testClassLoader = this.getClass.getClassLoader

  /** A file where all the test cases that pass are written. Can be used to update the whiteList. */
  val passedFile = new File(targetDir, s"$suiteName.passed")
  protected val passedList = new PrintWriter(passedFile)

  override def afterAll() {
    passedList.close()
  }

  /**
   * When `-Dshark.hive.failFast` is set the first test to fail will cause all subsequent tests to
   * also fail.
   */
  val failFast = System.getProperty("shark.hive.failFast") != null
  private var testFailed = false

  /**
   * Delete any cache files that result in test failures.  Used when the test harness has been
   * updated thus requiring new golden answers to be computed for some tests.
   */
  val recomputeCache = System.getProperty("shark.hive.recomputeCache") != null

  protected val cacheDigest = java.security.MessageDigest.getInstance("MD5")
  protected def getMd5(str: String): String = {
    val digest = java.security.MessageDigest.getInstance("MD5")
    digest.update(str.getBytes)
    new java.math.BigInteger(1, digest.digest).toString(16)
  }

  protected def prepareAnswer(sharkQuery: TestShark.type#SharkSqlQuery, answer: Seq[String]): Seq[String] = {
    val orderedAnswer = sharkQuery.parsed match {
      // Clean out non-deterministic time schema info.
      case _: Command => answer.filterNot(nonDeterministicLine)
      case _ =>
        val isOrdered = sharkQuery.executedPlan.collect { case s: Sort => s}.nonEmpty
        // If the query results aren't sorted, then sort them to ensure deterministic answers.
        if (!isOrdered) answer.sorted else answer
    }
    orderedAnswer.map(cleanPaths).map(clearTimes)
  }

  protected def nonDeterministicLine(line: String) =
    Seq("CreateTime","transient_lastDdlTime", "grantTime").map(line contains _).reduceLeft(_||_)

  protected def clearTimes(line: String) =
    line.replaceAll("\"lastUpdateTime\":\\d+", "<UPDATETIME>")

  /**
   * Removes non-deterministic paths from `str` so cached answers will still pass.
   */
  protected def cleanPaths(str: String): String = {
    str.replaceAll("file:\\/.*\\/", "<PATH>")
  }

  val installHooksCommand = "(?i)SET.*hooks".r
  def createQueryTest(testCaseName: String, sql: String) = {
    test(testCaseName) {
      if(failFast && testFailed) sys.error("Failing fast due to previous failure")
      testFailed = true
      logger.error(
       s"""
          |=============================
          |HIVE TEST: $testCaseName
          |=============================
          """.stripMargin)
      val queryList = sql.split("(?<=[^\\\\]);").map(_.trim).filterNot(q => q == "").toSeq

      try {
        TestShark.reset()

        if (queryList.filter(installHooksCommand.findAllMatchIn(_).nonEmpty).nonEmpty)
          sys.error("hive exec hooks not supported for tests.")

        val hiveCacheFiles = queryList.zipWithIndex.map {
          case (queryString, i)  =>
            val cachedAnswerName = s"$testCaseName-$i-${getMd5(queryString)}"
            new File(answerCache, cachedAnswerName)
        }

        val hiveCachedResults = hiveCacheFiles.flatMap { cachedAnswerFile =>
          logger.debug(s"Looking for cached answer file $cachedAnswerFile.")
          if (cachedAnswerFile.exists) {
            Some(fileToString(cachedAnswerFile))
          } else if (getClass.getClassLoader.getResourceAsStream(cachedAnswerFile.toString) != null) {
            Some(resourceToString(cachedAnswerFile.toString, classLoader = testClassLoader))
          } else {
            logger.debug(s"File $cachedAnswerFile not found")
            None
          }
        }.map {
          case "" => Nil
          case other => other.split("\n").toSeq
        }

        val hiveResults: Seq[Seq[String]] =
          if (hiveCachedResults.size == queryList.size) {
            logger.warn(s"Using answer cache for test: $testCaseName")
            hiveCachedResults
          } else {
            val sharkQueries = queryList.map(new TestShark.SharkSqlQuery(_))
            // Make sure we can at least parse everything before doing hive execution.
            sharkQueries.foreach(_.parsed)
            val computedResults = (queryList.zipWithIndex, sharkQueries,hiveCacheFiles).zipped.map {
              case ((queryString, i), sharkQuery, cachedAnswerFile)=>
                logger.warn(s"Running query ${i+1}/${queryList.size} with hive.")
                info(s"HIVE: $queryString")
                // Analyze the query with catalyst to ensure test tables are loaded.
                val answer = sharkQuery.analyzed match {
                  case _: ExplainCommand => Nil // No need to execute EXPLAIN queries as we don't check the output.
                  case _ => TestShark.runSqlHive(queryString)
                }

                stringToFile(cachedAnswerFile, answer.mkString("\n"))

                answer
            }.toSeq
            TestShark.reset()

            computedResults
          }

        testFailed = false

        // Run w/ catalyst
        val catalystResults = queryList.zip(hiveResults).map { case (queryString, hive) =>
          info(queryString)
          val query = new TestShark.SharkSqlQuery(queryString)
          try { (query, prepareAnswer(query, query.stringResult())) } catch {
            case e: Exception =>
              val out = new java.io.ByteArrayOutputStream
              val writer = new PrintWriter(out)
              e.printStackTrace(writer)
              writer.flush()
              fail(
                s"""
                  |Failed to execute query using catalyst:
                  |Error: ${e.getMessage}
                  |${new String(out.toByteArray)}
                  |$query
                  |== HIVE - ${hive.size} row(s) ==
                  |${hive.mkString("\n")}
                """.stripMargin)
          }
        }.toSeq

        (queryList, hiveResults, catalystResults).zipped.foreach {
          case (query, hive, (sharkQuery, catalyst)) =>
            // Check that the results match unless its an EXPLAIN query.
            val preparedHive = prepareAnswer(sharkQuery,hive)

            if ((!sharkQuery.parsed.isInstanceOf[ExplainCommand]) && preparedHive != catalyst) {

              val hivePrintOut = s"== HIVE - ${hive.size} row(s) ==" +: preparedHive
              val catalystPrintOut = s"== CATALYST - ${catalyst.size} row(s) ==" +: catalyst

              val resultComparison = sideBySide(hivePrintOut, catalystPrintOut).mkString("\n")

              if(recomputeCache) {
                logger.warn(s"Clearing cache files for failed test $testCaseName")
                hiveCacheFiles.foreach(_.delete())
              }

              fail(
                s"""
                  |Results do not match for query:
                  |$sharkQuery\n${sharkQuery.analyzed.output.map(_.name).mkString("\t")}
                  |$resultComparison
                """.stripMargin)
            }
        }

        passedList.println(s""""$testCaseName",""")
      } catch {
        case tf: org.scalatest.exceptions.TestFailedException => throw tf
        case originalException: Exception =>
          if (System.getProperty("shark.hive.canarytest") != null) {
            // When we encounter an error we check to see if the environment is still okay by running a simple query.
            // If this fails then we halt testing since something must have gone seriously wrong.
            try {
              new TestShark.SharkSqlQuery("SELECT key FROM src").stringResult()
              TestShark.runSqlHive("SELECT key FROM src")
            } catch {
              case e: Exception =>
                logger.error(s"FATAL ERROR: Canary query threw $e This implies that the testing environment has likely been corrupted.")
                // The testing setup traps exits so wait here for a long time so the developer can see when things started
                // to go wrong.
                Thread.sleep(1000000)
            }
          }

          // If the canary query didn't fail then the environment is still okay, so just throw the original exception.
          throw originalException
      }
    }
  }
}