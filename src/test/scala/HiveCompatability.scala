package catalyst

import shark.SharkContext
import shark.SharkEnv

import java.io._
import org.scalatest.{BeforeAndAfterAll, FunSuite, GivenWhenThen}

import frontend.Hive
import util.TestShark

class HiveCompatability extends FunSuite with BeforeAndAfterAll with GivenWhenThen {
  /** A list of tests currently deemed out of scope and thus completely ignored */
  val blackList = Seq(
    "set_processor_namespaces" // Unclear if we ever want to handle set commands in catalyst.
  )

  /** The set of tests that are believed to be working in catalyst. Tests not in white */
  val whiteList = Seq(
    "tablename_with_select"
  )

  override def beforeAll() {
    // By clearing the port we force Spark to pick a new one.  This allows us to rerun tests
    // without restarting the JVM.
    System.clearProperty("spark.driver.port")
    System.clearProperty("spark.hostPort")
  }

  val testShark = new TestShark

  // TODO: bundle in jar files... get from classpath
  val hiveQueryDir = new File("/Users/marmbrus/workspace/hive/ql/src/test/queries/clientpositive")
  val testCases = hiveQueryDir.listFiles

  // Go through all the test cases and add them to scala test.
  testCases.foreach { testCase =>
    val testCaseName = testCase.getName.stripSuffix(".q")
    if(blackList contains testCaseName) {
      // Do nothing
    } else if(whiteList contains testCaseName) {
      // Build a test case and submit it to scala test framework...
      test(testCaseName) {
        val queriesString = fileToString(testCase)
        val queryList = queriesString.split("(?<=[^\\\\]);").map(_.trim).filterNot(_ == "").toSeq

        cleanup()
        val hiveResults: Seq[Seq[String]] = queryList.map { queryString =>
          val result = testShark.runSql(queryString).toSeq

          if(queryString startsWith "DESCRIBE") Nil else result
        }.toSeq

        cleanup()

        // Run w/ catalyst
        val catalystResults: Seq[Seq[String]] = queryList.map { queryString =>
          info(queryString)
          val query = new testShark.SharkQuery  (queryString)
          query.execute().map(_.collect().map(_.mkString("\t")).toSeq).getOrElse(Nil)
        }.toSeq

        (queryList, hiveResults, catalystResults).zipped.foreach {
          case (query, hive, catalyst) =>
            assert(hive === catalyst)
        }
      }
    } else {
      ignore(testCaseName) {}
    }
  }

  // Depending on QTestUtil is hard since its not published...
  def cleanup() = {
    testShark.runSql("DROP TABLE IF EXISTS src")
    testShark.runSql("DROP TABLE IF EXISTS tmp_select")

    testShark.runSql("CREATE TABLE src (key INT, value STRING)")
    testShark.runSql("""LOAD DATA LOCAL INPATH '/Users/marmbrus/workspace/hive/data/files/kv1.txt' INTO TABLE src""")
  }

  protected def fileToString(file: File, encoding: String = "UTF-8") = {
    val inStream = new FileInputStream(file)
    val outStream = new ByteArrayOutputStream
    try {
      var reading = true
      while ( reading ) {
        inStream.read() match {
          case -1 => reading = false
          case c => outStream.write(c)
        }
      }
      outStream.flush()
    }
    finally {
      inStream.close()
    }
    new String(outStream.toByteArray(), encoding)
  }
}
