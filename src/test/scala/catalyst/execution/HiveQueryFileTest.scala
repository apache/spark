package catalyst
package execution

import java.io._

import util._

/**
 * A framework for running the query tests that are listed as a set of text files.
 *
 * TestSuites that derive from this class must provide a map of testCaseName -> testCaseFiles that should be included.
 * Additionally, there is support for whitelisting and blacklisting tests as development progresses.
 */
abstract class HiveQueryFileTest extends HiveComaparisionTest {
  /** A list of tests deemed out of scope and thus completely disregarded */
  def blackList: Seq[String] = Nil

  /**
   * The set of tests that are believed to be working in catalyst. Tests not in whiteList
   * blacklist are implicitly marked as ignored.
   */
  def whiteList: Seq[String] = ".*" :: Nil

  def testCases: Seq[(String, File)]

  val runAll = !(System.getProperty("shark.hive.alltests") == null)

  // Allow the whiteList to be overridden by a system property
  val realWhiteList = Option(System.getProperty("shark.hive.whitelist")).map(_.split(",").toSeq).getOrElse(whiteList)

  // Go through all the test cases and add them to scala test.
  testCases.foreach {
    case (testCaseName, testCaseFile) =>
      if(blackList.map(_.r.pattern.matcher(testCaseName).matches()).reduceLeft(_||_)) {
        logger.warn(s"Blacklisted test skipped $testCaseName")
      } else if(realWhiteList.map(_.r.pattern.matcher(testCaseName).matches()).reduceLeft(_||_) || runAll) {
        // Build a test case and submit it to scala test framework...
        val queriesString = fileToString(testCaseFile)
        createQueryTest(testCaseName, queriesString)
      } else {
        ignore(testCaseName) {}
      }
  }
}