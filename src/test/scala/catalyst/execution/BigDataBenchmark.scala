package catalyst
package execution

import java.io.File

/**
 * A set of test cases based on the big-data-benchmark.
 * https://amplab.cs.berkeley.edu/benchmark/
 */
class BigDataBenchmarkTests extends HiveComaparisionTest {
  import TestShark._

  val testDataDirectory = new File("target/big-data-benchmark-testdata")

  val testTables = Seq(
    TestTable(
      "rankings",
      s"""
        |CREATE EXTERNAL TABLE rankings (
        |  pageURL STRING,
        |  pageRank INT,
        |  avgDuration INT)
        |  ROW FORMAT DELIMITED FIELDS TERMINATED BY ","
        |  STORED AS TEXTFILE LOCATION "${new File(testDataDirectory, "rankings").getCanonicalPath}"
      """.stripMargin.cmd),
    TestTable(
      "scratch",
      s"""
        |CREATE EXTERNAL TABLE scratch (
        |  pageURL STRING,
        |  pageRank INT,
        |  avgDuration INT)
        |  ROW FORMAT DELIMITED FIELDS TERMINATED BY ","
        |  STORED AS TEXTFILE LOCATION "${new File(testDataDirectory, "scratch").getCanonicalPath}"
      """.stripMargin.cmd),
    TestTable(
      "uservisits",
      s"""
        |CREATE EXTERNAL TABLE uservisits (
        |  sourceIP STRING,
        |  destURL STRING,
        |  visitDate STRING,
        |  adRevenue DOUBLE,
        |  userAgent STRING,
        |  countryCode STRING,
        |  languageCode STRING,
        |  searchWord STRING,
        |  duration INT)
        |  ROW FORMAT DELIMITED FIELDS TERMINATED BY ","
        |  STORED AS TEXTFILE LOCATION "${new File(testDataDirectory, "uservisits").getCanonicalPath}"
      """.stripMargin.cmd),
    TestTable(
      "documents",
      s"""
        |CREATE EXTERNAL TABLE documents (line STRING)
        |STORED AS TEXTFILE
        |LOCATION "${new File(testDataDirectory, "crawl").getCanonicalPath}"
      """.stripMargin.cmd))

  testTables.foreach(registerTestTable)

  if(!testDataDirectory.exists()) {
    // TODO: Auto download the files on demand.
    ignore("No data files found for BigDataBenchmark tests.") {}
  } else {
    createQueryTest("query1",
      "SELECT pageURL, pageRank FROM rankings WHERE pageRank > 1")

    createQueryTest("query2",
      "SELECT SUBSTR(sourceIP, 1, 10), SUM(adRevenue) FROM uservisits GROUP BY SUBSTR(sourceIP, 1, 10)")

    createQueryTest("query3",
      """
        |SELECT sourceIP,
        |       sum(adRevenue) as totalRevenue,
        |       avg(pageRank) as pageRank
        |FROM
        |  rankings R JOIN
        |  (SELECT sourceIP, destURL, adRevenue
        |   FROM uservisits UV
        |   WHERE UV.visitDate > "1980-01-01"
        |   AND UV.visitDate < "1980-04-01")
        |   NUV ON (R.pageURL = NUV.destURL)
        |GROUP BY sourceIP
        |ORDER BY totalRevenue DESC
        |LIMIT 1
      """.stripMargin)

    createQueryTest("query4",
      """
        |DROP TABLE IF EXISTS url_counts_partial;
        |CREATE TABLE url_counts_partial AS
        |  SELECT TRANSFORM (line)
        |  USING 'python target/url_count.py' as (sourcePage,
        |    destPage, count) from documents;
        |DROP TABLE IF EXISTS url_counts_total;
        |CREATE TABLE url_counts_total AS
        |  SELECT SUM(count) AS totalCount, destpage
        |  FROM url_counts_partial GROUP BY destpage
        |-- The following queries run, but generate different results in HIVE likely because the UDF is not deterministic
        |-- given different input splits.
        |-- SELECT CAST(SUM(count) AS INT) FROM url_counts_partial
        |-- SELECT COUNT(*) FROM url_counts_partial
        |-- SELECT * FROM url_counts_partial
        |-- SELECT * FROM url_counts_total
      """.stripMargin)
  }
}