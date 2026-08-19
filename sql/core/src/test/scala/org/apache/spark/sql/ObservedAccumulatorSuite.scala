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

package org.apache.spark.sql

import org.apache.spark.sql.execution.streaming.runtime.MemoryStream
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, transform, udf}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.DoubleType

/** A plain object holding an accumulator, to exercise closure detection reaching object fields. */
class AccHolder(val acc: ObservedAccumulator) extends Serializable

class ObservedAccumulatorSuite extends QueryTest with SharedSparkSession {
  import testImplicits._

  // `value` blocks internally (drains the listener bus) so no explicit wait is needed here.

  /** A plain udf that references `acc` -- the closure detection must pick it up automatically. */
  private def parseUdf(acc: ObservedAccumulator): UserDefinedFunction = udf { (s: String) =>
    try {
      java.lang.Double.valueOf(s)
    } catch {
      case _: NumberFormatException =>
        acc.add()
        null.asInstanceOf[java.lang.Double]
    }
  }

  test("a plain UDF that references the accumulator records it; output stays scalar") {
    val acc = spark.accumulator("bad")
    val df = Seq("1", "x", "2", "y", "z").toDF("raw")
    val out = df.withColumn("v", parseUdf(acc)(col("raw")))

    // The analyzer rule rewrote the struct output into the plain value column.
    assert(out.schema("v").dataType == DoubleType)
    val rows = out.select("v").collect()
    val nonNull = rows.flatMap(r => if (r.isNullAt(0)) None else Some(r.getDouble(0))).sorted.toSeq
    assert(rows.length == 5)
    assert(nonNull == Seq(1.0d, 2.0d))
    assert(rows.count(_.isNullAt(0)) == 3)

    assert(acc.value == 3L)
  }

  test("UDF is evaluated exactly once per row") {
    val acc = spark.accumulator("bad2")
    val calls = spark.sparkContext.longAccumulator("calls")
    val parse = udf { (s: String) =>
      calls.add(1)
      try {
        java.lang.Double.valueOf(s)
      } catch {
        case _: NumberFormatException =>
          acc.add()
          null.asInstanceOf[java.lang.Double]
      }
    }
    val df = Seq("1", "x", "2", "y", "z").toDF("raw")
    // A single action (collect); checkAnswer would run the query twice (checkToRDD).
    df.withColumn("v", parse(col("raw"))).select("v").collect()
    assert(calls.value == 5L, "UDF must be evaluated exactly once per row")
    assert(acc.value == 3L)
  }

  test("value is cumulative across queries") {
    val acc = spark.accumulator("bad3")
    val parse = parseUdf(acc)
    val df = Seq("1", "x", "2", "y", "z").toDF("raw")
    df.withColumn("v", parse(col("raw"))).collect()
    df.withColumn("v", parse(col("raw"))).collect()
    assert(acc.value == 6L)
  }

  test("same accumulator referenced by two UDFs in one projection counts both") {
    // Two distinct UDFs both add to the same accumulator in a single select, so every input row
    // contributes twice. Regression: the two deltas must be combined into one `__oa_metric_shared`
    // column; emitting a duplicate-named metric per UDF made the harvest listener read only the
    // first and silently drop the other (result 3 instead of 6).
    val acc = spark.accumulator("shared")
    val f = udf { (s: String) => acc.add(); s }
    val g = udf { (s: String) => acc.add(); s }
    val df = Seq("a", "b", "c").toDF("raw")
    df.select(f(col("raw")).as("f"), g(col("raw")).as("g")).collect()
    assert(acc.value == 6L) // 3 rows * 2 UDFs
  }

  test("supports double-valued accumulators") {
    val acc = spark.accumulator("weight")
    val f = udf { (s: String) =>
      val v = s.toDouble
      acc.add(v * 0.5) // add(Double)
      v
    }
    val df = Seq("1", "2", "3").toDF("raw")
    df.withColumn("v", f(col("raw"))).collect()
    assert(acc.doubleValue == (1.0 + 2.0 + 3.0) * 0.5)
    assert(acc.value == 3L) // Long view rounds
  }

  test("works inside foreachBatch (streaming), matching classic accumulators") {
    implicit val ctx = spark.sqlContext
    val acc = spark.accumulator("bad_fb")
    val parse = parseUdf(acc)

    val input = MemoryStream[String]
    val query = input.toDF().toDF("raw").writeStream
      .foreachBatch { (batch: DataFrame, _: Long) =>
        batch.withColumn("v", parse(col("raw"))).collect()
        ()
      }
      .start()
    try {
      input.addData("1", "x", "2") // 1 bad
      query.processAllAvailable()
      input.addData("y", "3", "z") // 2 bad
      query.processAllAvailable()
    } finally {
      query.stop()
    }

    assert(acc.value == 3L)
  }

  test("supports typed custom-merge accumulators (parity with AccumulatorV2)") {
    // Arbitrary (non-numeric) type + associative merge: each task folds a partial, the partials
    // are gathered through the plan and folded on the driver.
    val acc = spark.accumulator[Set[String]]("keys_scala", Set.empty[String], _ ++ _)
    val f = udf { (s: String) =>
      acc.add(Set(s))
      s
    }
    val df = Seq("a", "b", "a").toDF("v")
    df.withColumn("x", f(col("v"))).collect()
    assert(acc.value == Set("a", "b"))
  }

  test("detection finds an accumulator held indirectly (collection / object field)") {
    // The closure walk must find an accumulator however it is captured, not just as a direct field
    // -- a miss would silently harvest nothing. Mirrors the PySpark nested-shape coverage.
    val nested = spark.accumulator("nested_scala")
    val box = Seq(Seq(nested)) // nested collection captured by the closure
    val f1 = udf { (s: String) =>
      box.head.head.add()
      s
    }
    Seq("a", "b").toDF("raw").withColumn("v", f1(col("raw"))).collect()
    assert(nested.value == 2L)

    val attr = spark.accumulator("attr_scala")
    val holder = new AccHolder(attr) // accumulator as a field of a captured plain object
    val f2 = udf { (s: String) =>
      holder.acc.add()
      s
    }
    Seq("a", "b", "c").toDF("raw").withColumn("v", f2(col("raw"))).collect()
    assert(attr.value == 3L)
  }

  test("observed accumulator matches a classic SparkContext accumulator across cases") {
    // Cross-validate against the classic accumulator (the reference): in a single execution with no
    // task retries both must agree. The observed one additionally survives retries (exactly-once).
    def check(name: String, data: Seq[String], contrib: String => Long): Unit = {
      val classic = spark.sparkContext.longAccumulator(s"ref_$name")
      val observed = spark.accumulator(s"obs_$name")
      val f = udf { (s: String) =>
        val v = contrib(s)
        classic.add(v)
        observed.add(v)
        s
      }
      data.toDF("raw").withColumn("v", f(col("raw"))).collect()
      assert(observed.value == classic.value, name)
    }
    check("count", Seq("a", "b", "c"), _ => 1L)
    check("sum_len", Seq("a", "bb", "ccc"), _.length.toLong)
    check("weighted", Seq("1", "2", "3"), _.toLong * 2)
  }

  test("multiple accumulators (numeric + typed) in one Scala UDF") {
    // A single Scala UDF may reference several accumulators; all are harvested, mixing a numeric
    // (summed Double delta) and a typed custom-merge (collect_list of serialized partials).
    val total = spark.accumulator("m_total")
    val keys = spark.accumulator[Set[String]]("m_keys", Set.empty[String], _ ++ _)
    val f = udf { (s: String) =>
      total.add()
      keys.add(Set(s.substring(0, 1)))
      s
    }
    Seq("apple", "banana", "avocado").toDF("w").withColumn("x", f(col("w"))).collect()
    assert(total.value == 3L)
    assert(keys.value == Set("a", "b"))
  }

  test("works under both codegen and interpreted evaluation") {
    // The struct wrapper has a doGenCode; exercise it (CODEGEN_ONLY, which fails if codegen is
    // invalid) and the interpreted eval (NO_CODEGEN) and confirm both harvest correctly.
    Seq(("true", "CODEGEN_ONLY"), ("false", "NO_CODEGEN")).foreach { case (wholeStage, factory) =>
      withSQLConf(
        SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> wholeStage,
        SQLConf.CODEGEN_FACTORY_MODE.key -> factory) {
        val acc = spark.accumulator(s"cg_$factory")
        val df = Seq("1", "x", "2", "y", "z").toDF("raw")
        df.withColumn("v", parseUdf(acc)(col("raw"))).collect()
        assert(acc.value == 3L, s"wholeStage=$wholeStage factoryMode=$factory")
      }
    }
  }

  test("accumulator UDF in a filter condition is observed") {
    val bad = spark.accumulator("filter_bad")
    val valid = udf { (s: String) =>
      try {
        java.lang.Double.valueOf(s)
        true
      } catch {
        case _: NumberFormatException =>
          bad.add()
          false
      }
    }
    val kept = Seq("1", "x", "2", "y", "z").toDF("raw").filter(valid(col("raw"))).collect()
    assert(kept.length == 2) // "1", "2"
    assert(bad.value == 3L) // x, y, z
  }

  test("accumulator UDF composed in a larger projected expression is observed") {
    val acc = spark.accumulator("composed")
    val f = udf { (x: Long) => acc.add(x); x }
    val out = spark.range(1, 4).select((f(col("id")) + 1L).alias("y")).collect()
    assert(out.map(_.getLong(0)).toSeq == Seq(2L, 3L, 4L))
    assert(acc.value == (1 + 2 + 3))
  }

  test("accumulator UDF in an unobservable position fails fast") {
    val acc = spark.accumulator("unsupported")
    val f = udf { (s: String) => acc.add(); s }
    // Grouping by an accumulator UDF -> Aggregate, which the rule cannot observe -> loud error
    // instead of a silent zero.
    val e = intercept[Exception] {
      Seq("a", "b").toDF("raw").groupBy(f(col("raw"))).count().collect()
    }
    assert(
      (e.getMessage != null && e.getMessage.contains("cannot be observed")) ||
        (e.getCause != null && e.getCause.getMessage != null &&
          e.getCause.getMessage.contains("cannot be observed")),
      s"unexpected error: $e")
  }

  test("same-named accumulators in different sessions do not collide") {
    // The classic registry is keyed by session, not by bare name, so two sessions that each create
    // an accumulator named "shared" write to disjoint slots (regression for the global-name bug).
    val other = spark.newSession()
    val accA = spark.accumulator("shared_iso")
    val accB = other.accumulator("shared_iso")
    val fA = udf { (s: String) => accA.add(); s }
    val fB = udf { (s: String) => accB.add(); s }

    spark.range(0, 3).selectExpr("cast(id as string) as raw")
      .withColumn("v", fA(col("raw"))).collect()
    other.range(0, 2).selectExpr("cast(id as string) as raw")
      .withColumn("v", fB(col("raw"))).collect()

    assert(accA.value == 3L) // only session A's rows; would be 5 if the slot were shared
    SparkSession.setActiveSession(other)
    try {
      assert(accB.value == 2L)
    } finally {
      SparkSession.setActiveSession(spark)
    }
  }

  test("Scala accumulator UDF inside a higher-order-function lambda fails fast") {
    val acc = spark.accumulator("hof_scala")
    val dbl = udf { (x: Int) => acc.add(); x * 2 }
    val df = Seq(Seq(1, 2, 3)).toDF("arr")
    val e = intercept[Exception] {
      df.select(transform(col("arr"), x => dbl(x)).alias("out")).collect()
    }
    val msg = if (e.getMessage != null) e.getMessage
      else if (e.getCause != null) e.getCause.getMessage else ""
    assert(msg != null && (msg.contains("cannot be observed") || msg.contains("higher-order")),
      s"unexpected error: $e")
  }

  test("detection survives closure serialization (as Spark Connect ships the UDF)") {
    // Spark Connect serializes the Scala UDF closure on the client and deserializes it on the
    // server; detection must still find the captured accumulator in the deserialized closure.
    val acc = spark.accumulator("ser_acc")
    val fn: String => java.lang.Double = { (s: String) =>
      try {
        java.lang.Double.valueOf(s)
      } catch {
        case _: NumberFormatException =>
          acc.add()
          null.asInstanceOf[java.lang.Double]
      }
    }
    val bos = new java.io.ByteArrayOutputStream()
    val oos = new java.io.ObjectOutputStream(bos)
    oos.writeObject(fn)
    oos.close()
    val ois = new java.io.ObjectInputStream(new java.io.ByteArrayInputStream(bos.toByteArray))
    val fn2 = ois.readObject().asInstanceOf[String => java.lang.Double]
    ois.close()

    val parse = udf(fn2)
    Seq("1", "x", "2", "y", "z").toDF("raw").withColumn("v", parse(col("raw"))).collect()
    assert(acc.value == 3L)
  }

  test("cross-session use is rejected") {
    // An accumulator is harvested by its creating session; reading it while a different session is
    // active would silently return the zero value, so it must raise instead.
    val other = spark.newSession() // a different session; `spark` stays active
    val numeric = other.accumulator("xsession_num")
    val typed = other.accumulator[Set[String]]("xsession_typed", Set.empty[String], _ ++ _)
    intercept[IllegalStateException](numeric.value)
    intercept[IllegalStateException](typed.value)
  }
}
