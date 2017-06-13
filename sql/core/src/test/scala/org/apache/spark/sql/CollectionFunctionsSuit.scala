package org.apache.spark.sql

import org.apache.spark.sql.test.SharedSQLContext

import org.apache.spark.sql.functions._

class CollectionFunctionsSuit extends QueryTest with SharedSQLContext {

  import testImplicits._

  test("functions map_keys") {
    val df = Seq(("1", 1), ("2", 2), ("3", 3)).toDF("col0", "col1")
    val mapDF = df.select(map($"col0", $"col1").as("column0"))
    checkAnswer(
      mapDF.select(map_keys($"column0")),
      Seq(Row(Array("1")), Row(Array("2")), Row(Array("3"))))
  }

  test("function map_values") {
    val df = Seq(("1", 1), ("2", 2), ("3", 3)).toDF("col0", "col1")
    val mapDF = df.select(map($"col0", $"col1").as("column0"))
    checkAnswer(
      mapDF.select(map_values($"column0")),
      Seq(Row(Array(1)), Row(Array(2)), Row(Array(3))))
  }
}
