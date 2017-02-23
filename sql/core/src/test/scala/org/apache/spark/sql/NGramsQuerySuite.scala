package org.apache.spark.sql

import org.apache.spark.sql.test.SharedSQLContext

class NGramsQuerySuite extends QueryTest with SharedSQLContext {

  import testImplicits._

  private val table = "ngrams_test"

  val abc = "abc"
  val bcd = "bcd"

  val pattern1 = Array[String](abc, abc, bcd, abc, bcd)
  val pattern2 = Array[String](bcd, abc, abc, abc, abc, bcd)

  test("nGrams") {
    withTempView(table) {
      List[String](pattern1.mkString(" "), pattern2.mkString(" ")).toDF("col").createOrReplaceTempView(table)
      checkAnswer(
        spark.sql(
          s"""
             |SELECT
             |  ngrams(array('abc', 'abc', 'bcd', 'abc', 'bcd'), 2, 3)
             |FROM $table
           """.stripMargin),
        Row(250D, 500D, 750D, 1D, 1000D, 1D, 1000D)
      )
    }
  }
}
