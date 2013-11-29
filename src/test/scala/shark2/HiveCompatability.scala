package catalyst
package shark2

import java.io._

/**
 * A framework for running the query tests that are included in hive distribution.
 */
class HiveCompatability extends HiveComaparisionTest {
  /** A list of tests deemed out of scope and thus completely disregarded */
  val blackList = Seq(
    "set_processor_namespaces" // Unclear if we ever want to handle set commands in catalyst.
  )

  /**
   * The set of tests that are believed to be working in catalyst. Tests not in whiteList
   * blacklist are implicitly marked as ignored.
   */
  val whiteList = Seq(
    "tablename_with_select",
    "literal_string",
    "literal_ints",
    "literal_double",
    "union16",
    "quote2",
    "count"
  )

  // TODO: bundle in jar files... get from classpath
  val hiveQueryDir = new File(testShark.hiveDevHome, "ql/src/test/queries/clientpositive")
  val testCases = hiveQueryDir.listFiles

  // Go through all the test cases and add them to scala test.
  testCases.foreach { testCase =>
    val testCaseName = testCase.getName.stripSuffix(".q")
    if(blackList contains testCaseName) {
      // Do nothing
    } else if(whiteList contains testCaseName) {
      // Build a test case and submit it to scala test framework...
      val queriesString = fileToString(testCase)
      createQueryTest(testCaseName, queriesString)
    } else {
      ignore(testCaseName) {}
    }
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
