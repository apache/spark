package catalyst
package shark2

import shark.{SharkContext, SharkEnv}

import java.io._
import org.scalatest.{BeforeAndAfterAll, FunSuite, GivenWhenThen}

abstract class HiveComaparisionTest extends FunSuite with BeforeAndAfterAll with GivenWhenThen {
  val testShark = new TestShark

  def createQueryTest(testCaseName: String, sql: String) {
    test(testCaseName) {
      val queryList = sql.split("(?<=[^\\\\]);").map(_.trim).filterNot(_ == "").toSeq

      cleanup()
      val hiveResults: Seq[Seq[String]] = queryList.map { queryString =>
        val result = testShark.runSql(queryString).toSeq

        if(queryString startsWith "DESCRIBE") Nil else result
      }.toSeq

      cleanup()

      // Run w/ catalyst
      val catalystResults: Seq[Seq[String]] = queryList.map { queryString =>
        info(queryString)
        val query = new testShark.SharkSqlQuery(queryString)
        query.execute().map(_.collect().map(_.mkString("\t")).toSeq).getOrElse(Nil)
      }.toSeq

      (queryList, hiveResults, catalystResults).zipped.foreach {
        case (query, hive, catalyst) =>
          assert(hive === catalyst)
      }
    }
  }

  // Depending on QTestUtil is hard since its not published...
  def cleanup() = {
    testShark.runSql("DROP TABLE IF EXISTS src")
    testShark.runSql("DROP TABLE IF EXISTS tmp_select")

    testShark.runSql("CREATE TABLE src (key INT, value STRING)")
    testShark.runSql("""LOAD DATA LOCAL INPATH '/Users/marmbrus/workspace/hive/data/files/kv1.txt' INTO TABLE src""")
  }
}