package catalyst
package shark2

import catalyst.frontend.hive.Command
import shark.{SharkContext, SharkEnv}

import java.io._
import org.scalatest.{BeforeAndAfterAll, FunSuite, GivenWhenThen}

/**
 * Allows the creations of tests that execute the same query against both hive
 * and catalyst, comparing the results.
 */
abstract class HiveComaparisionTest extends FunSuite with BeforeAndAfterAll with GivenWhenThen {
  val testShark = TestShark

  protected def prepareAnswer(query: String, answer: Seq[String]): Seq[String] = {
    if(answer.isEmpty) return Nil // Don't attempt to plan NativeCommands
    val isOrdered = !(new testShark.SharkSqlQuery(query)).executedPlan.collect { case s: Sort => s}.isEmpty
    // If the query results aren't sorted, then sort them to ensure deterministic answers.
    if(!isOrdered) answer.sorted else answer
  }

  def createQueryTest(testCaseName: String, sql: String) {
    test(testCaseName) {
      val queryList = sql.split("(?<=[^\\\\]);").map(_.trim).filterNot(q => q == "" || (q startsWith "EXPLAIN")).toSeq

      testShark.reset()

      // Run w/ catalyst
      val catalystResults = queryList.map { queryString =>
        info(queryString)
        val query = new testShark.SharkSqlQuery(queryString)
        val result: Seq[Seq[Any]] = query.execute().map(_.collect.toSeq).getOrElse(Nil)
        val asString = result.map(_.map {
            case null => "NULL"
            case other => other
          }).map(_.mkString("\t")).toSeq

        (query, prepareAnswer(queryString, asString))
      }.toSeq

      testShark.reset()

      val hiveResults = queryList.map { queryString =>
        // Analyze the query with catalyst to ensure test tables are loaded.
        val sharkQuery = (new testShark.SharkSqlQuery(queryString))
        sharkQuery.analyzed

        val result = testShark.runSqlHive(queryString).toSeq

        sharkQuery.parsed match {
          case _: Command => Nil // We don't check output for commands that are passed back to hive.
          case _ => prepareAnswer(queryString, result)
        }
      }.toSeq

      testShark.reset()

      (queryList, hiveResults, catalystResults).zipped.foreach {
        case (query, hive, (sharkQuery, catalyst)) =>
          if(hive != catalyst) {
            fail(
              s"""
                |Results do not match for query:
                |$sharkQuery\n${sharkQuery.analyzed.output.mkString("\t")}\n
                |==HIVE - ${hive.size} rows==
                |${hive.mkString("\n")}\n
                |==CATALYST - ${catalyst.size} rows==
                |${catalyst.mkString("\n")}
              """.stripMargin)
          }
      }
    }
  }
}