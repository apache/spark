/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql.connect

import java.lang.{Long => JLong}
import java.util.{Arrays, Iterator => JIterator}
import java.util.concurrent.atomic.AtomicLong

import scala.jdk.CollectionConverters._

import org.apache.spark.api.java.function._
import org.apache.spark.sql.{AnalysisException, Encoder, Encoders, Row}
import org.apache.spark.sql.api.java.UDF2
import org.apache.spark.sql.catalyst.encoders.AgnosticEncoders.{PrimitiveIntEncoder, PrimitiveLongEncoder, StringEncoder}
import org.apache.spark.sql.connect.test.{QueryTest, RemoteSparkSession}
import org.apache.spark.sql.expressions.{Aggregator, MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.functions.{call_function, col, count, lit, struct, udaf, udf}
import org.apache.spark.sql.types.{DataType, IntegerType, LongType, StringType, StructField, StructType}

/**
 * All tests in this class requires client UDF defined in this test class synced with the server.
 */
class UserDefinedFunctionE2ETestSuite extends QueryTest with RemoteSparkSession {
  test("Dataset typed filter") {
    val rows = spark.range(10).filter(n => n % 2 == 0).collectAsList()
    assert(rows == Arrays.asList[Long](0, 2, 4, 6, 8))
  }

  test("Dataset typed filter - java") {
    val rows = spark
      .range(10)
      .filter(new FilterFunction[JLong] {
        override def call(value: JLong): Boolean = value % 2 == 0
      })
      .collectAsList()
    assert(rows == Arrays.asList[Long](0, 2, 4, 6, 8))
  }

  test("Dataset typed map") {
    val rows = spark.range(10).map(n => n / 2)(PrimitiveLongEncoder).collectAsList()
    assert(rows == Arrays.asList[Long](0, 0, 1, 1, 2, 2, 3, 3, 4, 4))
  }

  test("filter with condition") {
    // This should go via `def filter(condition: Column)` rather than
    // `def filter(func: T => Boolean)`
    def func(i: Long): Boolean = i < 5
    val under5 = udf(func _)
    val longs = spark.range(10).filter(under5(col("id") * 2)).collectAsList()
    assert(longs == Arrays.asList[Long](0, 1, 2))
  }

  test("filter with col(*)") {
    // This should go via `def filter(condition: Column)` but it is executed as
    // `def filter(func: T => Boolean)`. This is fine as the result is the same.
    def func(i: Long): Boolean = i < 5
    val under5 = udf(func _)
    val longs = spark.range(10).filter(under5(col("*"))).collectAsList()
    assert(longs == Arrays.asList[Long](0, 1, 2, 3, 4))
  }

  test("Dataset typed map - java") {
    val rows = spark
      .range(10)
      .map(
        new MapFunction[JLong, Long] {
          def call(value: JLong): Long = value / 2
        },
        PrimitiveLongEncoder)
      .collectAsList()
    assert(rows == Arrays.asList[Long](0, 0, 1, 1, 2, 2, 3, 3, 4, 4))
  }

  test("Dataset typed flat map") {
    val session: SparkSession = spark
    import session.implicits._
    val rows = spark
      .range(5)
      .flatMap(n => Iterator(42, 42))
      .collectAsList()
    assert(rows.size() == 10)
    rows.forEach(x => assert(x == 42))
  }

  test("(deprecated) Dataset explode") {
    val session: SparkSession = spark
    import session.implicits._
    val result1 = spark
      .range(3)
      .filter(col("id") =!= 1L)
      .explode(col("id") + 41, col("id") + 10) { case Row(x: Long, y: Long) =>
        Iterator((x, x - 1), (y, y + 1))
      }
      .as[(Long, Long, Long)]
      .collect()
      .toSeq
    assert(result1 === Seq((0L, 41L, 40L), (0L, 10L, 11L), (2L, 43L, 42L), (2L, 12L, 13L)))

    val result2 = Seq((1, "a b c"), (2, "a b"), (3, "a"))
      .toDF("number", "letters")
      .explode(Symbol("letters")) { case Row(letters: String) =>
        letters.split(' ').map(Tuple1.apply).toSeq
      }
      .as[(Int, String, String)]
      .collect()
      .toSeq
    assert(
      result2 === Seq(
        (1, "a b c", "a"),
        (1, "a b c", "b"),
        (1, "a b c", "c"),
        (2, "a b", "a"),
        (2, "a b", "b"),
        (3, "a", "a")))

    val result3 = Seq("a b c", "d e")
      .toDF("words")
      .explode("words", "word") { word: String =>
        word.split(' ').toSeq
      }
      .select(col("word"))
      .as[String]
      .collect()
      .toSeq
    assert(result3 === Seq("a", "b", "c", "d", "e"))

    val result4 = Seq("a b c", "d e")
      .toDF("words")
      .explode("words", "word") { word: String =>
        word.split(' ').map(s => s -> s.head.toInt).toSeq
      }
      .select(col("word"), col("words"))
      .as[((String, Int), String)]
      .collect()
      .toSeq
    assert(
      result4 === Seq(
        (("a", 97), "a b c"),
        (("b", 98), "a b c"),
        (("c", 99), "a b c"),
        (("d", 100), "d e"),
        (("e", 101), "d e")))
  }

  test("Dataset typed flat map - java") {
    val rows = spark
      .range(5)
      .flatMap(
        new FlatMapFunction[JLong, Int] {
          def call(value: JLong): JIterator[Int] = Arrays.asList(42, 42).iterator()
        },
        PrimitiveIntEncoder)
      .collectAsList()
    assert(rows.size() == 10)
    rows.forEach(x => assert(x == 42))
  }

  test("Dataset typed map partition") {
    val session: SparkSession = spark
    import session.implicits._
    val df = spark.range(0, 100, 1, 50).repartition(4)
    val result =
      df.mapPartitions(iter => Iterator.single(iter.length)).collect()
    assert(result.sorted.toSeq === Seq(23, 25, 25, 27))
  }

  test("Dataset typed map partition - java") {
    val df = spark.range(0, 100, 1, 50).repartition(4)
    val result = df
      .mapPartitions(
        new MapPartitionsFunction[JLong, Int] {
          override def call(input: JIterator[JLong]): JIterator[Int] = {
            Arrays.asList(input.asScala.length).iterator()
          }
        },
        PrimitiveIntEncoder)
      .collect()
    assert(result.sorted.toSeq === Seq(23, 25, 25, 27))
  }

  test("Dataset foreach") {
    val func: JLong => Unit = _ => {
      throw new RuntimeException("Hello foreach")
    }
    val exception = intercept[Exception] {
      spark.range(2).foreach(func)
    }
    assert(exception.getMessage.contains("Hello foreach"))
  }

  test("Dataset foreach - java") {
    val exception = intercept[Exception] {
      spark
        .range(2)
        .foreach(new ForeachFunction[JLong] {
          override def call(t: JLong): Unit = {
            throw new RuntimeException("Hello foreach")
          }
        })
    }
    assert(exception.getMessage.contains("Hello foreach"))
  }

  test("Dataset foreachPartition") {
    val func: Iterator[JLong] => Unit = f => {
      val sum = new AtomicLong()
      f.foreach(v => sum.addAndGet(v))
      throw new Exception("Success, processed records: " + sum.get())
    }
    val exception = intercept[Exception] {
      spark.range(10).repartition(1).foreachPartition(func)
    }
    assert(exception.getMessage.contains("Success, processed records: 45"))
  }

  test("Dataset foreachPartition - java") {
    val sum = new AtomicLong()
    val exception = intercept[Exception] {
      spark
        .range(11)
        .repartition(1)
        .foreachPartition(new ForeachPartitionFunction[JLong] {
          override def call(t: JIterator[JLong]): Unit = {
            t.asScala.foreach(v => sum.addAndGet(v))
            throw new Exception("Success, processed records: " + sum.get())
          }
        })
    }
    assert(exception.getMessage.contains("Success, processed records: 55"))
  }

  test("Dataset foreach: change not visible to client") {
    val sum = new AtomicLong()
    val func: Iterator[JLong] => Unit = f => {
      f.foreach(v => sum.addAndGet(v))
    }
    spark.range(10).repartition(1).foreachPartition(func)
    assert(sum.get() == 0) // The value is not 45
  }

  test("Dataset reduce without null partition inputs") {
    val session: SparkSession = spark
    import session.implicits._
    assert(spark.range(0, 10, 1, 5).map(_ + 1).reduce(_ + _) == 55)
  }

  test("Dataset reduce with null partition inputs") {
    val session: SparkSession = spark
    import session.implicits._
    assert(spark.range(0, 10, 1, 16).map(_ + 1).reduce(_ + _) == 55)
  }

  test("Dataset reduce with null partition inputs - java to scala long type") {
    val session: SparkSession = spark
    import session.implicits._
    assert(spark.range(0, 5, 1, 10).as[Long].reduce(_ + _) == 10)
  }

  test("Dataset reduce with null partition inputs - java") {
    val session: SparkSession = spark
    import session.implicits._
    assert(
      spark
        .range(0, 10, 1, 16)
        .map(_ + 1)
        .reduce(new ReduceFunction[Long] {
          override def call(v1: Long, v2: Long): Long = v1 + v2
        }) == 55)
  }

  test("udf with row input encoder") {
    val session: SparkSession = spark
    import session.implicits._
    val df = Seq((1, 2, 3)).toDF("a", "b", "c")
    val f = udf((row: Row) => row.schema.fieldNames)
    import org.apache.spark.util.ArrayImplicits._
    checkDataset(
      df.select(f(struct((df.columns map col).toImmutableArraySeq: _*))),
      Row(Seq("a", "b", "c")))
  }

  test("Filter with row input encoder") {
    val session: SparkSession = spark
    import session.implicits._
    val df = Seq(("a", 10), ("a", 20), ("b", 1), ("b", 2), ("c", 1)).toDF("c1", "c2")

    checkDataset(df.filter(r => r.getInt(1) > 5), Row("a", 10), Row("a", 20))
  }

  test("SPARK-50693: Filter with row input encoder on unresolved plan") {
    val session: SparkSession = spark
    import session.implicits._
    val df = Seq(("a", 10), ("a", 20), ("b", 1), ("b", 2), ("c", 1)).toDF("c1", "c2")

    checkDataset(df.select("*").filter(r => r.getInt(1) > 5), Row("a", 10), Row("a", 20))
  }

  test("mapPartitions with row input encoder") {
    val session: SparkSession = spark
    import session.implicits._
    val df = Seq(("a", 10), ("a", 20), ("b", 1), ("b", 2), ("c", 1)).toDF("c1", "c2")

    checkDataset(
      df.mapPartitions(it => it.map(r => r.getAs[String]("c1"))),
      "a",
      "a",
      "b",
      "b",
      "c")
  }

  // TODO re-enable this after we hooked SqlApiConf into the session confs.
  ignore("(deprecated) scala UDF with dataType") {
    val session: SparkSession = spark
    import session.implicits._
    val fn = udf(((i: Long) => (i + 1).toInt), IntegerType)
    checkDataset(session.range(2).select(fn($"id")).as[Int], 1, 2)
  }

  test("(deprecated) scala UDF with dataType should fail") {
    intercept[AnalysisException] {
      udf(((i: Long) => (i + 1).toInt), IntegerType)
    }
  }

  test("java UDF") {
    val session: SparkSession = spark
    import session.implicits._
    val fn = udf(
      new UDF2[Long, Long, Int] {
        override def call(t1: Long, t2: Long): Int = (t1 + t2 + 1).toInt
      },
      IntegerType)
    checkDataset(session.range(2).select(fn($"id", $"id" + 2)).as[Int], 3, 5)
  }

  test("nullified SparkSession/Dataset/KeyValueGroupedDataset in UDF") {
    val session: SparkSession = spark
    import session.implicits._
    val df = session.range(0, 10, 1, 1)
    val kvgds = df.groupByKey(_ / 2)
    val f = udf { (i: Long) =>
      assert(session == null)
      assert(df == null)
      assert(kvgds == null)
      i + 1
    }
    val result = df.select(f($"id")).as[Long].head()
    assert(result == 1L)
  }

  test("UDAF custom Aggregator - primitive types") {
    val session: SparkSession = spark
    import session.implicits._
    val agg = new Aggregator[Long, Long, Long] {
      override def zero: Long = 0L
      override def reduce(b: Long, a: Long): Long = b + a
      override def merge(b1: Long, b2: Long): Long = b1 + b2
      override def finish(reduction: Long): Long = reduction
      override def bufferEncoder: Encoder[Long] = Encoders.scalaLong
      override def outputEncoder: Encoder[Long] = Encoders.scalaLong
    }
    spark.udf.register("agg", udaf(agg))
    val result = spark.range(10).selectExpr("agg(id)").as[Long].head()
    assert(result == 45)
  }

  test("UDAF custom Aggregator - case class as input types") {
    val session: SparkSession = spark
    import session.implicits._
    val agg = new CompleteUdafTestInputAggregator()
    spark.udf.register("agg", udaf(agg))
    val result = spark
      .range(10)
      .withColumn("extra", col("id") * 2)
      .as[UdafTestInput]
      .selectExpr("agg(id, extra)")
      .as[Long]
      .head()
    assert(result == 135) // 45 + 90
  }

  test("UDAF custom Aggregator - toColumn") {
    val encoder = Encoders.product[UdafTestInput]
    val aggCol = new CompleteUdafTestInputAggregator().toColumn
    val ds = spark.range(10).withColumn("extra", col("id") * 2).as(encoder)
    assert(ds.select(aggCol).head() == 135) // 45 + 90
  }

  test("SPARK-50789: UDAF custom Aggregator - toColumn on unresolved plan") {
    val encoder = Encoders.product[UdafTestInput]
    val aggCol = new CompleteUdafTestInputAggregator().toColumn
    val ds = spark.range(10).withColumn("extra", col("id") * 2).select("*").as(encoder)
    assert(ds.select(aggCol).head() == 135) // 45 + 90
  }

  test("UDAF custom Aggregator - multiple extends - toColumn") {
    val encoder = Encoders.product[UdafTestInput]
    val aggCol = new CompleteGrandChildUdafTestInputAggregator().toColumn
    val ds = spark.range(10).withColumn("extra", col("id") * 2).as(encoder)
    assert(ds.select(aggCol).head() == 540) // (45 + 90) * 4
  }

  test("SPARK-50789: UDAF custom Aggregator - multiple extends - toColumn on unresolved plan") {
    val encoder = Encoders.product[UdafTestInput]
    val aggCol = new CompleteGrandChildUdafTestInputAggregator().toColumn
    val ds = spark.range(10).withColumn("extra", col("id") * 2).select("*").as(encoder)
    assert(ds.select(aggCol).head() == 540) // (45 + 90) * 4
  }

  test("UDAF custom Aggregator - with rows - toColumn") {
    val ds = spark.range(10).withColumn("extra", col("id") * 2)
    assert(ds.select(RowAggregator.toColumn).head() == 405)
    assert(ds.agg(RowAggregator.toColumn).head().getLong(0) == 405)
  }

  test("SPARK-50789: UDAF custom Aggregator - with rows - toColumn on unresolved plan") {
    val ds = spark.range(10).withColumn("extra", col("id") * 2).select("*")
    assert(ds.select(RowAggregator.toColumn).head() == 405)
    assert(ds.agg(RowAggregator.toColumn).head().getLong(0) == 405)
  }

  test("registerJavaUdf") {
    spark.udf.registerJava("sconcat", classOf[StringConcat].getName, StringType)
    val ds = spark
      .range(2)
      .select(call_function("sconcat", col("id").cast("string"), (col("id") + 1).cast("string")))
      .as(StringEncoder)
    checkDataset(ds, "01", "12")
  }

  test("inline UserDefinedAggregateFunction") {
    val summer0 = new LongSummer(0)
    val summer1 = new LongSummer(1)
    val summer3 = new LongSummer(3)
    val ds = spark
      .range(10)
      .select(
        count(lit(1)),
        summer0(),
        summer1(col("id")),
        summer3(col("id"), col("id") + 1, col("id") + 2))
    checkDataset(ds, Row(10L, 10L, Row(45L, 10L), Row(45L, 55L, 65L, 10L)))
  }

  test("inline UserDefinedAggregateFunction distinct") {
    val summer1 = new LongSummer(1)
    val ds =
      spark.range(10).union(spark.range(10)).select(count(lit(1)), summer1.distinct(col("id")))
    checkDataset(ds, Row(20L, Row(45L, 10L)))
  }

  test("register UserDefinedAggregateFunction") {
    spark.udf.register("s0", new LongSummer(0))
    spark.udf.register("s2", new LongSummer(2))
    spark.udf.register("s4", new LongSummer(4))
    val ds = spark
      .range(10)
      .select(
        count(lit(1)),
        call_function("s0"),
        call_function("s2", col("id"), col("id") + 1),
        call_function("s4", col("id"), col("id") + 1, col("id") + 2, col("id") + 3))
    checkDataset(ds, Row(10L, 10L, Row(45L, 55L, 10L), Row(45L, 55L, 65L, 75L, 10L)))
  }
}

case class UdafTestInput(id: Long, extra: Long)

// An Aggregator that takes [[UdafTestInput]] as input.
final class CompleteUdafTestInputAggregator
    extends Aggregator[UdafTestInput, (Long, Long), Long] {
  override def zero: (Long, Long) = (0L, 0L)
  override def reduce(b: (Long, Long), a: UdafTestInput): (Long, Long) =
    (b._1 + a.id, b._2 + a.extra)
  override def merge(b1: (Long, Long), b2: (Long, Long)): (Long, Long) =
    (b1._1 + b2._1, b1._2 + b2._2)
  override def finish(reduction: (Long, Long)): Long = reduction._1 + reduction._2
  override def bufferEncoder: Encoder[(Long, Long)] =
    Encoders.tuple(Encoders.scalaLong, Encoders.scalaLong)
  override def outputEncoder: Encoder[Long] = Encoders.scalaLong
}

// Same as [[CompleteUdafTestInputAggregator]] but the input type is not defined.
abstract class IncompleteUdafTestInputAggregator[T] extends Aggregator[T, (Long, Long), Long] {
  override def zero: (Long, Long) = (0L, 0L)
  override def reduce(b: (Long, Long), a: T): (Long, Long) // Incomplete!
  override def merge(b1: (Long, Long), b2: (Long, Long)): (Long, Long) =
    (b1._1 + b2._1, b1._2 + b2._2)
  override def finish(reduction: (Long, Long)): Long = reduction._1 + reduction._2
  override def bufferEncoder: Encoder[(Long, Long)] =
    Encoders.tuple(Encoders.scalaLong, Encoders.scalaLong)
  override def outputEncoder: Encoder[Long] = Encoders.scalaLong
}

// A layer over [[IncompleteUdafTestInputAggregator]] but the input type is still not defined.
abstract class IncompleteChildUdafTestInputAggregator[T]
    extends IncompleteUdafTestInputAggregator[T] {
  override def finish(reduction: (Long, Long)): Long = (reduction._1 + reduction._2) * 2
}

// Another layer that finally defines the input type.
final class CompleteGrandChildUdafTestInputAggregator
    extends IncompleteChildUdafTestInputAggregator[UdafTestInput] {
  override def reduce(b: (Long, Long), a: UdafTestInput): (Long, Long) =
    (b._1 + a.id, b._2 + a.extra)
  override def finish(reduction: (Long, Long)): Long = (reduction._1 + reduction._2) * 4
}

object RowAggregator extends Aggregator[Row, (Long, Long), Long] {
  override def zero: (Long, Long) = (0, 0)
  override def reduce(b: (Long, Long), a: Row): (Long, Long) = {
    (b._1 + a.getLong(0), b._2 + a.getLong(1))
  }
  override def merge(b1: (Long, Long), b2: (Long, Long)): (Long, Long) = {
    (b1._1 + b2._1, b1._2 + b2._2)
  }
  override def finish(r: (Long, Long)): Long = (r._1 + r._2) * 3
  override def bufferEncoder: Encoder[(Long, Long)] =
    Encoders.tuple(Encoders.scalaLong, Encoders.scalaLong)
  override def outputEncoder: Encoder[Long] = Encoders.scalaLong
}

class StringConcat extends UDF2[String, String, String] {
  override def call(t1: String, t2: String): String = t1 + t2
}

class LongSummer(size: Int) extends UserDefinedAggregateFunction {
  assert(size >= 0)

  override def inputSchema: StructType = {
    StructType(Array.tabulate(size)(i => StructField(s"val_$i", LongType)))
  }

  override def bufferSchema: StructType = inputSchema.add("counter", LongType)

  override def dataType: DataType = {
    if (size == 0) {
      LongType
    } else {
      bufferSchema
    }
  }

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    var i = 0
    while (i < size + 1) {
      buffer.update(i, 0L)
      i += 1
    }
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    var i = 0
    while (i < size) {
      buffer.update(i, buffer.getLong(i) + input.getLong(i))
      i += 1
    }
    buffer.update(size, buffer.getLong(size) + 1)
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    var i = 0
    while (i < size + 1) {
      buffer1.update(i, buffer1.getLong(i) + buffer2.getLong(i))
      i += 1
    }
  }

  override def evaluate(buffer: Row): Any = {
    if (size == 0) {
      buffer.getLong(0)
    } else {
      buffer
    }
  }
}
