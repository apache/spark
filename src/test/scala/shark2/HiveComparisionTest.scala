package catalyst
package shark2

import shark.{SharkContext, SharkEnv}

import java.io._
import org.scalatest.{BeforeAndAfterAll, FunSuite, GivenWhenThen}

import catalyst.frontend.hive.{ExplainCommand, Command}
import util._

/**
 * Allows the creations of tests that execute the same query against both hive
 * and catalyst, comparing the results.
 *
 * The "golden" results from Hive are cached in [[answerCache]] to speed up testing.
 */
abstract class HiveComaparisionTest extends FunSuite with BeforeAndAfterAll with GivenWhenThen {
  val testShark = TestShark

  protected val targetDir = new File("target")
  protected val answerCache = new File(targetDir, "comparison-test-cache")
  if(!answerCache.exists)
    answerCache.mkdir()

  val passedFile = new File(targetDir, s"$suiteName.passed")
  val passedList = new PrintWriter(passedFile)

  override def afterAll() {
    passedList.close()
  }

  protected val cacheDigest = java.security.MessageDigest.getInstance("MD5")
  protected def getMd5(str: String): String = {
    val digest = java.security.MessageDigest.getInstance("MD5")
    digest.update(str.getBytes)
    new java.math.BigInteger(1, digest.digest).toString(16)
  }

  protected def prepareAnswer(sharkQuery: TestShark.type#SharkSqlQuery, answer: Seq[String]): Seq[String] = {
    val orderedAnswer = sharkQuery.parsed match {
      case _: Command => answer  // Don't attempt to modify the result of Commands since they are run by hive.
      case _ =>
        val isOrdered = !sharkQuery.executedPlan.collect { case s: Sort => s}.isEmpty
        // If the query results aren't sorted, then sort them to ensure deterministic answers.
        if(!isOrdered) answer.sorted else answer
    }
    orderedAnswer.map(cleanPaths)
  }

  /**
   * Removes non-deterministic paths from [[str]] so cached answers will still pass.
   */
  protected def cleanPaths(str: String): String = {
    str.replaceAll("file:\\/.*\\/", "<PATH>")
  }


  def createQueryTest(testCaseName: String, sql: String) = {
    test(testCaseName) {
      println(
       s"""=============================
          |===HIVE TEST: $testCaseName===
          |=============================""".stripMargin)
      val queryList = sql.split("(?<=[^\\\\]);").map(_.trim).filterNot(q => q == "").toSeq

      try {
        testShark.reset()

        val hiveResults: Seq[Seq[String]] = queryList.zipWithIndex.map {
          case (queryString, i)  =>
            val cachedAnswerName = s"$testCaseName-$i-${getMd5(queryString)}"
            val cachedAnswerFile = new File(answerCache, cachedAnswerName)

            if(cachedAnswerFile.exists) {
              println(s"Using cached answer for: $queryString")
              val cachedAnswer = fileToString(cachedAnswerFile)
              if(cachedAnswer == "")
                Nil
              else
                cachedAnswer.split("\n").toSeq
            } else {
              // Analyze the query with catalyst to ensure test tables are loaded.
              val sharkQuery = (new testShark.SharkSqlQuery(queryString))
              val answer = sharkQuery.analyzed match {
                case _: ExplainCommand => Nil // No need to execute EXPLAIN queries as we don't check the output.
                case _ => testShark.runSqlHive(queryString)
              }

              stringToFile(cachedAnswerFile, answer.mkString("\n"))

              answer
            }
        }.toSeq

        testShark.reset()

        // Run w/ catalyst
        val catalystResults = queryList.zip(hiveResults).map { case (queryString, hive) =>
          info(queryString)
          val query = new testShark.SharkSqlQuery(queryString)
          try { (query, prepareAnswer(query, query.stringResult())) } catch {
            case e: Exception =>
              fail(
                s"""
                  |Failed to execute query using catalyst:
                  |$query
                  |== HIVE - ${hive.size} row(s) ==
                  |${hive.mkString("\n")}
                """.stripMargin)
          }
        }.toSeq


        testShark.reset()

        (queryList, hiveResults, catalystResults).zipped.foreach {
          case (query, hive, (sharkQuery, catalyst)) =>
            // Check that the results match unless its an EXPLAIN query.
            if((!sharkQuery.parsed.isInstanceOf[ExplainCommand]) && prepareAnswer(sharkQuery,hive) != catalyst) {
              fail(
                s"""
                  |Results do not match for query:
                  |$sharkQuery\n${sharkQuery.analyzed.output.mkString("\t")}
                  |== HIVE - ${hive.size} row(s) ==
                  |${hive.mkString("\n")}
                  |== CATALYST - ${catalyst.size} row(s) ==
                  |${catalyst.mkString("\n")}
                """.stripMargin)
            }
        }

        passedList.println(testCaseName)
      } catch {
        case tf: org.scalatest.exceptions.TestFailedException => throw tf
        case originalException: Exception =>
          // When we encounter an error we check to see if the environment is still okay by running a simple query.
          // If this fails then we halt testing since something must have gone seriously wrong.
          try {
            new testShark.SharkSqlQuery("SELECT key FROM src").stringResult()
            testShark.runSqlHive("SELECT key FROM src")
          } catch {
            case e: Exception =>
              println(s"FATAL ERROR: Canary query threw $e This implies that the testing environment has likely been corrupted.")
              // The testing setup traps exits so wait here for a long time so the developer can see when things started
              // to go wrong.
              Thread.sleep(1000000)
              System.exit(1)
          }

          // If the canary query didn't fail then the environment is still okay, so just throw the original exception.
          throw originalException
      }
    }
  }
}