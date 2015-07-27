package org.apache.spark.sql

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._


class DataFrameTungstenSuite extends SparkFunSuite {

  private lazy val ctx = org.apache.spark.sql.test.TestSQLContext
  import ctx.implicits._

  test("test simple types") {
    ctx.setConf("spark.sql.unsafe.enabled", "true")
    val df = ctx.sparkContext.parallelize(Seq((1, 2))).toDF("a", "b")
    assert(df.select(struct("a", "b")).first().getStruct(0) === Row(1, 2))
  }

  test("test struct type") {
    val struct = Row(1, 2L, 3.0F, 3.0)
    val data = ctx.sparkContext.parallelize(Seq(Row(1, struct)))

    val schema = new StructType()
      .add("a", IntegerType)
      .add("b",
        new StructType()
          .add("b1", IntegerType)
          .add("b2", LongType)
          .add("b3", FloatType)
          .add("b4", DoubleType))

    val df = ctx.createDataFrame(data, schema)
    assert(df.select("b").first() === Row(struct))
  }

  test("test nested struct type") {
    val innerStruct = Row(1, "abcd")
    val outerStruct = Row(1, 2L, 3.0F, 3.0, innerStruct, "efg")
    val data = ctx.sparkContext.parallelize(Seq(Row(1, outerStruct)))

    val schema = new StructType()
      .add("a", IntegerType)
      .add("b",
        new StructType()
          .add("b1", IntegerType)
          .add("b2", LongType)
          .add("b3", FloatType)
          .add("b4", DoubleType)
          .add("b5", new StructType()
            .add("b5a", IntegerType)
            .add("b5b", StringType))
          .add("b6", StringType))

    val df = ctx.createDataFrame(data, schema)
    assert(df.select("b").first() === Row(outerStruct))
  }
}
