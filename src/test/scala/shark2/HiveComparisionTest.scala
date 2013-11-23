package catalyst
package shark2

import shark.{SharkContext, SharkEnv}

import java.io._
import org.scalatest.{BeforeAndAfterAll, FunSuite, GivenWhenThen}

/**
 * Allows the creations of tests that execute the same query against both hive
 * and catalyst, comparing the results.
 */
abstract class HiveComaparisionTest extends FunSuite with BeforeAndAfterAll with GivenWhenThen {
  val testShark = TestShark

  def createQueryTest(testCaseName: String, sql: String) {
    test(testCaseName) {
      val queryList = sql.split("(?<=[^\\\\]);").map(_.trim).filterNot(_ == "").toSeq

      testShark.reset()

      // Run w/ catalyst
      val catalystResults = queryList.map { queryString =>
        info(queryString)
        val query = new testShark.SharkSqlQuery(queryString)
        (query, query.execute().map(_.collect().map(_.mkString("\t")).toSeq).getOrElse(Nil))
      }.toSeq

      testShark.reset()

      val hiveResults = queryList.map { queryString =>
        // Analyze the query with catalyst to ensure test tables are loaded.
        (new testShark.SharkSqlQuery(queryString)).analyzed

        val result = testShark.runSqlHive(queryString).toSeq

        // We don't yet match DESCRIBE output... In the end this command will probably be passed back to hive.
        if(queryString startsWith "DESCRIBE") Nil else result
      }.toSeq

      testShark.reset()

      (queryList, hiveResults, catalystResults).zipped.foreach {
        case (query, hive, (sharkQuery, catalyst)) =>
          assert(hive === catalyst, s"Results do not match for query: $sharkQuery")
      }
    }
  }
}