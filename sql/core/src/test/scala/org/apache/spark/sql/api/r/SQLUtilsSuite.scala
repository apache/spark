package org.apache.spark.sql.api.r

import org.apache.spark.sql.test.SharedSQLContext
import org.scalatest.{Matchers, FlatSpec}

class SQLUtilsSuite extends SharedSQLContext {

  import testImplicits._

  test("dfToCols should collect and transpose a data frame") {
    val df = Seq(
      (1, 2, 3),
      (4, 5, 6)
    ).toDF
    assert(SQLUtils.dfToCols(df) === Array(
      Array(1, 4),
      Array(2, 5),
      Array(3, 6)
    ))
  }

}
