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

package org.apache.spark.ml.feature

import org.apache.spark.{SparkException, SparkRuntimeException}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.attribute.NumericAttribute
import org.apache.spark.ml.param.ParamsSuite
import org.apache.spark.ml.util.{DefaultReadWriteTest, MLTest}
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

class FrequencyEncoderSuite extends MLTest with DefaultReadWriteTest {

  import testImplicits._

  @transient var data: Seq[Row] = _
  @transient var schema: StructType = _
  @transient var expected_proportions: Array[Map[Double, Double]] = _
  @transient var expected_counts: Array[Map[Double, Double]] = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    // Nine rows. input1 gives every category the same count, which is what makes the
    // equal-frequency case observable; input2 is uneven; input3 has a mix of common and
    // singleton categories.
    //
    // Expectations are written as count/9.0 because that is the arithmetic fit performs, which
    // keeps the fixture readable against the implementation. For these operands it makes no
    // numerical difference: 1.0/3.0 and 3.0/9.0 are the same Double.
    // scalastyle:off
    data = Seq(
      Row(0.toShort, 3, 5.0, 3.0/9.0, 5.0/9.0, 3.0/9.0, 3.0, 5.0, 3.0),
      Row(1.toShort, 4, 5.0, 3.0/9.0, 4.0/9.0, 3.0/9.0, 3.0, 4.0, 3.0),
      Row(2.toShort, 3, 5.0, 3.0/9.0, 5.0/9.0, 3.0/9.0, 3.0, 5.0, 3.0),
      Row(0.toShort, 4, 6.0, 3.0/9.0, 4.0/9.0, 3.0/9.0, 3.0, 4.0, 3.0),
      Row(1.toShort, 3, 6.0, 3.0/9.0, 5.0/9.0, 3.0/9.0, 3.0, 5.0, 3.0),
      Row(2.toShort, 4, 6.0, 3.0/9.0, 4.0/9.0, 3.0/9.0, 3.0, 4.0, 3.0),
      Row(0.toShort, 3, 7.0, 3.0/9.0, 5.0/9.0, 1.0/9.0, 3.0, 5.0, 1.0),
      Row(1.toShort, 4, 8.0, 3.0/9.0, 4.0/9.0, 1.0/9.0, 3.0, 4.0, 1.0),
      Row(2.toShort, 3, 9.0, 3.0/9.0, 5.0/9.0, 1.0/9.0, 3.0, 5.0, 1.0))
    // scalastyle:on

    schema = StructType(Array(
      StructField("input1", ShortType, nullable = true),
      StructField("input2", IntegerType, nullable = true),
      StructField("input3", DoubleType, nullable = true),
      StructField("expected1", DoubleType),
      StructField("expected2", DoubleType),
      StructField("expected3", DoubleType),
      StructField("count1", DoubleType),
      StructField("count2", DoubleType),
      StructField("count3", DoubleType)))

    expected_proportions = Array(
      Map(0.0 -> 3.0/9.0, 1.0 -> 3.0/9.0, 2.0 -> 3.0/9.0),
      Map(3.0 -> 5.0/9.0, 4.0 -> 4.0/9.0),
      Map(5.0 -> 3.0/9.0, 6.0 -> 3.0/9.0, 7.0 -> 1.0/9.0, 8.0 -> 1.0/9.0, 9.0 -> 1.0/9.0))

    expected_counts = Array(
      Map(0.0 -> 3.0, 1.0 -> 3.0, 2.0 -> 3.0),
      Map(3.0 -> 5.0, 4.0 -> 4.0),
      Map(5.0 -> 3.0, 6.0 -> 3.0, 7.0 -> 1.0, 8.0 -> 1.0, 9.0 -> 1.0))
  }

  private def multiColumnEncoder: FrequencyEncoder = new FrequencyEncoder()
    .setInputCols(Array("input1", "input2", "input3"))
    .setOutputCols(Array("output1", "output2", "output3"))

  test("params") {
    ParamsSuite.checkParams(new FrequencyEncoder)
  }

  test("model estimated size") {
    val df = spark.createDataFrame(sc.parallelize(data), schema)
    val model = multiColumnEncoder.fit(df)
    val maxSize = 1024 * 6
    assert(model.estimatedSize < maxSize,
      s"Estimation (${model.estimatedSize}) should be less than $maxSize")
  }

  test("FrequencyEncoder - proportions") {

    val df = spark.createDataFrame(sc.parallelize(data), schema)

    val model = multiColumnEncoder.fit(df)

    model.encodings.zip(expected_proportions).foreach {
      case (actual, expected) => assert(actual.equals(expected))
    }

    // Every proportion for a feature sums to one, because each row contributes exactly one
    // category per feature.
    model.encodings.foreach { mapping =>
      assert(mapping.values.sum === 1.0)
    }

    testTransformer[(Double, Double, Double, Double, Double, Double)](
      df.select("input1", "input2", "input3", "expected1", "expected2", "expected3"),
      model,
      "output1", "expected1",
      "output2", "expected2",
      "output3", "expected3") {
      case Row(output1: Double, expected1: Double,
      output2: Double, expected2: Double,
      output3: Double, expected3: Double) =>
        assert(output1 === expected1)
        assert(output2 === expected2)
        assert(output3 === expected3)
    }
  }

  test("FrequencyEncoder - raw counts") {

    val df = spark.createDataFrame(sc.parallelize(data), schema)

    val model = multiColumnEncoder.setNormalize(false).fit(df)

    model.encodings.zip(expected_counts).foreach {
      case (actual, expected) => assert(actual.equals(expected))
    }

    testTransformer[(Double, Double, Double, Double, Double, Double)](
      df.select("input1", "input2", "input3", "count1", "count2", "count3"),
      model,
      "output1", "count1",
      "output2", "count2",
      "output3", "count3") {
      case Row(output1: Double, expected1: Double,
      output2: Double, expected2: Double,
      output3: Double, expected3: Double) =>
        assert(output1 === expected1)
        assert(output2 === expected2)
        assert(output3 === expected3)
    }
  }

  test("FrequencyEncoder - equally common categories share an encoding") {

    val df = spark.createDataFrame(sc.parallelize(data), schema)

    val model = multiColumnEncoder.fit(df)

    // input1's three categories each occur three times, so all three collapse onto one value.
    // This is inherent to the technique: the encoding carries how common a category is and
    // nothing that distinguishes one equally common category from another.
    val input1 = model.encodings(0)
    assert(input1.keySet === Set(0.0, 1.0, 2.0))
    assert(input1.values.toSet.size === 1)
  }

  test("FrequencyEncoder - unseen value - keep") {

    val df = spark.createDataFrame(sc.parallelize(data), schema)

    val model = multiColumnEncoder
      .setHandleInvalid(FrequencyEncoder.KEEP_INVALID)
      .fit(df)

    // Category 10.0 never appears in training, so its observed frequency is zero.
    val data_unseen = Row(0.toShort, 3, 10.0, 3.0/9.0, 5.0/9.0, 0.0, 3.0, 5.0, 0.0)

    val df_unseen = spark.createDataFrame(sc.parallelize(data :+ data_unseen), schema)

    testTransformer[(Double, Double, Double, Double, Double, Double)](
      df_unseen.select("input1", "input2", "input3", "expected1", "expected2", "expected3"),
      model,
      "output1", "expected1",
      "output2", "expected2",
      "output3", "expected3") {
      case Row(output1: Double, expected1: Double,
      output2: Double, expected2: Double,
      output3: Double, expected3: Double) =>
        assert(output1 === expected1)
        assert(output2 === expected2)
        assert(output3 === expected3)
    }
  }

  test("FrequencyEncoder - unseen value - error") {

    val df = spark.createDataFrame(sc.parallelize(data), schema)

    val model = multiColumnEncoder
      .setHandleInvalid(FrequencyEncoder.ERROR_INVALID)
      .fit(df)

    val data_unseen = Row(0.toShort, 3, 10.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0)
    val df_unseen = spark.createDataFrame(sc.parallelize(data :+ data_unseen), schema)

    val ex = intercept[SparkRuntimeException] {
      model.transform(df_unseen).select("output3").collect()
    }
    assert(ex.getMessage.contains("Unseen value"))
  }

  test("FrequencyEncoder - seen null category") {

    val data_null = data :+ Row(null, 3, 5.0, 1.0/10.0, 6.0/10.0, 4.0/10.0, 1.0, 6.0, 4.0)
    val df = spark.createDataFrame(sc.parallelize(data_null), schema)

    // A null is a category in its own right: it occurs once in ten rows.
    val model = multiColumnEncoder.fit(df)

    assert(model.encodings(0).get(FrequencyEncoder.NULL_CATEGORY).contains(1.0/10.0))

    // Collected directly rather than through testTransformer: its typed encoder cannot
    // deserialize a null into a non-nullable Double, and a null input is the point here.
    model.transform(df).select("input1", "output1").collect().foreach { row =>
      val expected = if (row.isNullAt(0)) 1.0/10.0 else 3.0/10.0
      assert(row.getDouble(1) === expected)
    }
  }

  test("FrequencyEncoder - unseen null category - keep") {

    val df = spark.createDataFrame(sc.parallelize(data), schema)

    // Fitted without nulls, so null has no learned frequency and falls to the unseen branch.
    val model = multiColumnEncoder
      .setHandleInvalid(FrequencyEncoder.KEEP_INVALID)
      .fit(df)

    assert(!model.encodings(0).contains(FrequencyEncoder.NULL_CATEGORY))

    val df_null = spark.createDataFrame(
      sc.parallelize(data :+ Row(null, 3, 5.0, 0.0, 5.0/9.0, 3.0/9.0, 0.0, 5.0, 3.0)), schema)

    // As above, collected directly because the row under test carries a null input.
    model.transform(df_null).select("input1", "output1").collect().foreach { row =>
      val expected = if (row.isNullAt(0)) 0.0 else 3.0/9.0
      assert(row.getDouble(1) === expected)
    }
  }

  test("FrequencyEncoder - missing feature") {

    val df = spark.createDataFrame(sc.parallelize(data), schema)

    val encoder = new FrequencyEncoder()
      .setInputCols(Array("input1", "absent"))
      .setOutputCols(Array("output1", "output2"))

    val ex = intercept[SparkException] { encoder.fit(df) }
    assert(ex.getMessage.contains("No column named absent found on dataset"))
  }

  test("FrequencyEncoder - wrong data type") {

    val df = spark.createDataFrame(sc.parallelize(data), schema)
      .withColumn("stringy", $"input1".cast(StringType))

    val encoder = new FrequencyEncoder()
      .setInputCol("stringy")
      .setOutputCol("output")

    val ex = intercept[SparkException] { encoder.fit(df) }
    assert(ex.getMessage.contains("Data type for column stringy"))
  }

  test("FrequencyEncoder - non-indexed categories") {

    val df = spark.createDataFrame(sc.parallelize(data), schema)
      .withColumn("fractional", $"input3" + 0.5)

    val encoder = new FrequencyEncoder()
      .setInputCol("fractional")
      .setOutputCol("output")

    val ex = intercept[SparkRuntimeException] { encoder.fit(df) }
    assert(ex.getMessage.contains("MUST be non-negative integers"))
  }

  test("FrequencyEncoder - the default output name is the one the getter reports") {

    // Deriving a friendlier default from the input name broke the getter contract: getOutputCol
    // still returned HasOutputCol's default while transform produced `input_encoded`, so a
    // caller feeding the getter's answer to the next stage named a column that did not exist.
    val df = spark.createDataFrame(sc.parallelize(data), schema)

    val model = new FrequencyEncoder().setInputCol("input1").fit(df)
    val produced = model.transform(df).columns.toSet

    assert(produced.contains(model.getOutputCol),
      s"transform produced $produced, but getOutputCol says ${model.getOutputCol}")
  }

  test("FrequencyEncoder - multiple inputs need explicit output names") {

    val df = spark.createDataFrame(sc.parallelize(data), schema)

    val ex = intercept[IllegalArgumentException] {
      new FrequencyEncoder().setInputCols(Array("input1", "input2")).fit(df)
    }
    assert(ex.getMessage.contains("must be the same as the number of"))
  }

  test("FrequencyEncoder - composes in a Pipeline") {

    // The defect this guards: the estimator's transformSchema returned the input schema
    // unchanged, and Pipeline.fit folds transformSchema across its stages, so VectorAssembler
    // was handed a schema with no `freq` in it and failed with FIELD_NOT_FOUND before any
    // fitting happened. Direct estimator and model calls could never surface that.
    val df = spark.createDataFrame(sc.parallelize(data), schema)

    val pipeline = new Pipeline().setStages(Array(
      new FrequencyEncoder().setInputCol("input3").setOutputCol("freq"),
      new VectorAssembler().setInputCols(Array("freq")).setOutputCol("features")))

    val out = pipeline.fit(df).transform(df)

    assert(out.columns.contains("freq"))
    assert(out.columns.contains("features"))
    assert(out.count() === data.length)
  }

  test("FrequencyEncoder - an output name that collides with an existing column is rejected") {

    // transformSchema used to append a duplicate field while withColumns replaced the existing
    // column, so the declared schema and the delivered frame disagreed. Rejecting the collision
    // at fit time gives validation, schema inference and execution one answer.
    val df = spark.createDataFrame(sc.parallelize(data), schema)

    val ex = intercept[IllegalArgumentException] {
      new FrequencyEncoder().setInputCol("input3").setOutputCol("input1").fit(df)
    }
    assert(ex.getMessage.contains("already exists"))
  }

  test("FrequencyEncoder - wrong number of features") {

    val df = spark.createDataFrame(sc.parallelize(data), schema)

    val model = multiColumnEncoder.fit(df)

    // Three encodings were fitted; asking the model for two outputs must not silently encode
    // the wrong columns.
    val ex = intercept[SparkException] {
      model
        .setInputCols(Array("input1", "input2"))
        .setOutputCols(Array("output1", "output2"))
        .transform(df)
    }
    assert(ex.getMessage.contains("does not match the number of"))
  }

  test("FrequencyEncoder - a feature with many distinct categories") {

    // This encoder exists for high cardinality features, so the case it is built for is
    // exercised rather than assumed: fit collects one entry per category to the driver and
    // transform ships that map into the plan as a literal, and neither of those is free.
    //
    // The frequencies are deliberately unequal. An earlier version of this test gave every
    // category the same count, which meant a transform that ignored the fitted map entirely and
    // returned that one constant would still have satisfied it. Here the expected value is
    // derived independently, from a grouped count joined back on, so the assertion is about the
    // mapping rather than about a single number.
    val counts = (0 until 10000).flatMap(i => Seq.fill(1 + (i % 3))(Row(i.toDouble)))
    val wideSchema = StructType(Array(StructField("cat", DoubleType, nullable = true)))
    val df = spark.createDataFrame(sc.parallelize(counts), wideSchema)

    val model = new FrequencyEncoder().setInputCol("cat").setOutputCol("freq").fit(df)

    assert(model.encodings.head.size === 10000)

    val total = df.count().toDouble
    val oracle = df.groupBy("cat").count()
      .select(col("cat"), (col("count") / lit(total)).alias("want"))
    val joined = model.transform(df).join(oracle, "cat")

    assert(joined.filter(col("freq") =!= col("want")).count() === 0,
      "a category was encoded with a frequency that is not its own")
    // Three distinct counts went in, so no constant could have passed the check above.
    assert(joined.select("freq").distinct().count() === 3)
  }

  test("FrequencyEncoder - ids above the supported range are rejected, not merged") {

    // Categories are carried as Double map keys, exact only to 2^53. 9007199254740992 and
    // 9007199254740993 both become the same double, so accepting them would merge two distinct
    // categories and report one count of 3 where the truth is 1 and 2. Rejecting on range
    // before anything narrows is what makes that unreachable.
    val df = spark.sql(
      "SELECT * FROM VALUES (9007199254740992L), (9007199254740993L), " +
        "(9007199254740993L) AS t(cat)")

    val encoder = new FrequencyEncoder().setInputCol("cat").setOutputCol("freq")

    val ex = intercept[SparkRuntimeException] { encoder.fit(df) }
    assert(ex.getMessage.contains("only supports up to"))
    assert(ex.getMessage.contains(Int.MaxValue.toString))
  }

  test("FrequencyEncoder - a fractional decimal is rejected rather than truncated") {

    // decimal(38,18) 1.000000000000000001 becomes exactly 1.0 once cast to double, so an
    // integrality check performed after that cast would accept it as category 1 and merge it
    // with genuine 1s. The check runs on the source column for this reason.
    val df = spark.sql(
      "SELECT CAST('1.000000000000000001' AS DECIMAL(38,18)) AS cat " +
        "UNION ALL SELECT CAST('1' AS DECIMAL(38,18))")

    val encoder = new FrequencyEncoder().setInputCol("cat").setOutputCol("freq")

    val ex = intercept[SparkRuntimeException] { encoder.fit(df) }
    assert(ex.getMessage.contains("MUST be non-negative integers"))
  }

  test("FrequencyEncoder - an out of range value at transform time is unseen, not aliased") {

    // The failure this guards: a value that no longer fits must not round down onto a category
    // the model did learn and borrow its frequency. With keep it is unseen, so zero.
    val df = spark.createDataFrame(sc.parallelize(data), schema)
    val model = multiColumnEncoder
      .setHandleInvalid(FrequencyEncoder.KEEP_INVALID)
      .fit(df)

    val far = spark.sql(
      "SELECT CAST(0 AS SHORT) AS input1, 3 AS input2, 9007199254740993D AS input3, " +
        "0D AS expected1, 0D AS expected2, 0D AS expected3, 0D AS count1, 0D AS count2, " +
        "0D AS count3")

    val row = model.transform(far).select("output3").head()
    assert(row.getDouble(0) === 0.0, "an out of range value must not inherit a learned frequency")
  }

  test("FrequencyEncoder - a known category does not raise under handleInvalid error") {

    // transform looks the category up once and coalesces the miss handler in, rather than
    // testing the lookup for null and reading it again. Under handleInvalid=error that miss
    // handler is a raise_error, so this asserts the property the shape depends on: coalesce
    // evaluates its second argument only when the first is null. A known category must come
    // back with its own encoding and must not trip the error.
    val df = spark.createDataFrame(sc.parallelize(data), schema)

    val model = multiColumnEncoder
      .setHandleInvalid(FrequencyEncoder.ERROR_INVALID)
      .fit(df)

    val rows = model.transform(df).select("output1", "output2", "output3").collect()
    assert(rows.length === data.length)
    rows.foreach { row =>
      assert(row.getDouble(0) === 3.0/9.0)
      assert(row.getDouble(1) === 5.0/9.0 || row.getDouble(1) === 4.0/9.0)
    }
  }

  test("FrequencyEncoder - a feature that trained on nothing but nulls") {

    // Every value is null, so the only category learned is the null one and the non-null mapping
    // is empty. Nulls take their learned encoding of 1.0; anything else is unseen.
    val nullSchema = StructType(Array(StructField("cat", DoubleType, nullable = true)))
    val allNull = spark.createDataFrame(
      sc.parallelize(Seq(Row(null), Row(null), Row(null))), nullSchema)

    val model = new FrequencyEncoder()
      .setInputCol("cat").setOutputCol("freq")
      .setHandleInvalid(FrequencyEncoder.KEEP_INVALID)
      .fit(allNull)

    assert(model.encodings.head === Map(FrequencyEncoder.NULL_CATEGORY -> 1.0))

    val mixed = spark.createDataFrame(
      sc.parallelize(Seq(Row(null), Row(7.0))), nullSchema)
    val got = model.transform(mixed).select("cat", "freq").collect()
      .map(r => (if (r.isNullAt(0)) None else Some(r.getDouble(0))) -> r.getDouble(1)).toMap
    assert(got(None) === 1.0, "a learned null keeps its own encoding")
    assert(got(Some(7.0)) === 0.0, "a value never seen is unseen, not the null encoding")
  }

  test("FrequencyEncoder - a fit over zero rows learns nothing") {

    // An empty mapping is the other way into the short-circuit branch: nothing was learned, so
    // every value is unseen, including null, which has no learned encoding to fall back on.
    val nullSchema = StructType(Array(StructField("cat", DoubleType, nullable = true)))
    val empty = spark.createDataFrame(sc.emptyRDD[Row], nullSchema)

    val keep = new FrequencyEncoder()
      .setInputCol("cat").setOutputCol("freq")
      .setHandleInvalid(FrequencyEncoder.KEEP_INVALID)
      .fit(empty)
    assert(keep.encodings.head.isEmpty)

    val probe = spark.createDataFrame(sc.parallelize(Seq(Row(1.0), Row(null))), nullSchema)
    assert(keep.transform(probe).select("freq").collect().forall(_.getDouble(0) === 0.0))

    val strict = new FrequencyEncoder()
      .setInputCol("cat").setOutputCol("freq")
      .setHandleInvalid(FrequencyEncoder.ERROR_INVALID)
      .fit(empty)
    intercept[SparkRuntimeException] {
      strict.transform(probe).select("freq").collect()
    }
  }

  test("FrequencyEncoder - output type, metadata and nullability") {

    val df = spark.createDataFrame(sc.parallelize(data), schema)

    for (mode <- Seq(FrequencyEncoder.KEEP_INVALID, FrequencyEncoder.ERROR_INVALID)) {
      val model = multiColumnEncoder.setHandleInvalid(mode).fit(df)

      val declared = model.transformSchema(df.schema)("output1")
      val delivered = model.transform(df).schema("output1")

      for (field <- Seq(declared, delivered)) {
        assert(field.dataType === DoubleType)
        assert(NumericAttribute.fromStructField(field).name === Some("output1"))
      }
      assert(!declared.nullable, "schema inference promises a value for every row")

      // Under keep the single-lookup shape is provably non-null: coalesce falls back to a
      // literal. Under error the fallback is raise_error, which Catalyst types as nullable, so
      // the delivered field still reports nullable even though it can only ever throw or
      // produce a double. Either way no null is ever emitted, which is what matters.
      if (mode == FrequencyEncoder.KEEP_INVALID) {
        assert(!delivered.nullable, "keep mode coalesces to a literal, so nothing can be null")
      }
      assert(model.transform(df).filter(col("output1").isNull).count() === 0)
    }
  }

  test("FrequencyEncoder - encoding is unchanged with codegen disabled") {

    // The single-lookup shape is a Catalyst expression change, so it has an interpreted path as
    // well as a generated one. Both must produce the same values.
    val df = spark.createDataFrame(sc.parallelize(data), schema)
    val model = multiColumnEncoder
      .setHandleInvalid(FrequencyEncoder.KEEP_INVALID)
      .fit(df)

    def encode(): Seq[Double] = model.transform(df)
      .select("output1", "output2", "output3").collect()
      .flatMap(r => Seq(r.getDouble(0), r.getDouble(1), r.getDouble(2))).toSeq

    val generated = encode()
    val interpreted = withSQLConf(
      SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false",
      SQLConf.CODEGEN_FACTORY_MODE.key -> "NO_CODEGEN") {
      encode()
    }
    assert(generated === interpreted)
    assert(generated.nonEmpty)
  }

  test("FrequencyEncoder - R/W single-column") {
    val encoder = new FrequencyEncoder()
      .setInputCol("input1")
      .setOutputCol("output1")
      .setHandleInvalid(FrequencyEncoder.KEEP_INVALID)
    testDefaultReadWrite(encoder)

    val df = spark.createDataFrame(sc.parallelize(data), schema)
    val model = encoder.fit(df)
    testDefaultReadWrite(model)
  }

  test("FrequencyEncoder - R/W multi-column") {
    val encoder = multiColumnEncoder.setNormalize(false)
    testDefaultReadWrite(encoder)

    val df = spark.createDataFrame(sc.parallelize(data), schema)
    val model = encoder.fit(df)

    // The encodings are positional, matched to inputCols by order, so a round trip must
    // preserve that order and not just the set of maps.
    val reloaded = testDefaultReadWrite(model)
    reloaded.encodings.zip(expected_counts).foreach {
      case (actual, expected) => assert(actual.equals(expected))
    }
  }
}
