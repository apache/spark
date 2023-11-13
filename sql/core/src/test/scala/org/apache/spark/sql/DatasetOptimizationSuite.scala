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

import org.apache.spark.metrics.source.CodegenMetrics
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.{CreateNamedStruct, Expression}
import org.apache.spark.sql.catalyst.plans.logical.SerializeFromObject
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.StructType

class DatasetOptimizationSuite extends QueryTest with SharedSparkSession {
  import testImplicits._

  test("SPARK-26619: Prune the unused serializers from SerializeFromObject") {
    val data = Seq(("a", 1), ("b", 2), ("c", 3))
    val ds = data.toDS().map(t => (t._1, t._2 + 1)).select("_1")
    val serializer = ds.queryExecution.optimizedPlan.collect {
      case s: SerializeFromObject => s
    }.head
    assert(serializer.serializer.size == 1)
    checkAnswer(ds, Seq(Row("a"), Row("b"), Row("c")))
  }


  // This methods checks if the given DataFrame has specified struct fields in object
  // serializer. The varargs parameter `structFields` is the struct fields for object
  // serializers. The first `structFields` is aligned with first serializer and ditto
  // for other `structFields`.
  private def testSerializer(df: DataFrame, structFields: Seq[Seq[String]]*): Unit = {
    val serializer = df.queryExecution.optimizedPlan.collect {
      case s: SerializeFromObject => s
    }.head

    def collectNamedStruct: PartialFunction[Expression, Seq[CreateNamedStruct]] = {
      case c: CreateNamedStruct => Seq(c)
    }

    serializer.serializer.zip(structFields).foreach { case (ser, fields) =>
      val structs: Seq[CreateNamedStruct] = ser.collect(collectNamedStruct).flatten
      assert(structs.size == fields.size)
      structs.zip(fields).foreach { case (struct, fieldNames) =>
        assert(struct.names.map(_.toString) == fieldNames)
      }
    }
  }

  test("Prune nested serializers: struct") {
    withSQLConf(SQLConf.SERIALIZER_NESTED_SCHEMA_PRUNING_ENABLED.key -> "true") {
      val data = Seq((("a", 1, ("aa", 1.0)), 1), (("b", 2, ("bb", 2.0)), 2),
        (("c", 3, ("cc", 3.0)), 3))
      val ds = data.toDS().map(t => (t._1, t._2 + 1))

      val df1 = ds.select("_1._1")
      testSerializer(df1, Seq(Seq("_1")))
      checkAnswer(df1, Seq(Row("a"), Row("b"), Row("c")))

      val df2 = ds.select("_1._2")
      testSerializer(df2, Seq(Seq("_2")))
      checkAnswer(df2, Seq(Row(1), Row(2), Row(3)))

      val df3 = ds.select("_1._3._1")
      testSerializer(df3, Seq(Seq("_3"), Seq("_1")))
      checkAnswer(df3, Seq(Row("aa"), Row("bb"), Row("cc")))

      val df4 = ds.select("_1._3._1", "_1._2")
      testSerializer(df4, Seq(Seq("_2", "_3"), Seq("_1")))
      checkAnswer(df4, Seq(Row("aa", 1), Row("bb", 2), Row("cc", 3)))
    }
  }

  test("Prune nested serializers: array of struct") {
    withSQLConf(SQLConf.SERIALIZER_NESTED_SCHEMA_PRUNING_ENABLED.key -> "true") {
      val arrayData = Seq((Seq(("a", 1, ("a_1", 11)), ("b", 2, ("b_1", 22))), 1, ("aa", 1.0)),
        (Seq(("c", 3, ("c_1", 33)), ("d", 4, ("d_1", 44))), 2, ("bb", 2.0)))
      val arrayDs = arrayData.toDS().map(t => (t._1, t._2 + 1, t._3))
      val df1 = arrayDs.select("_1._1")
      // The serializer creates array of struct of one field "_1".
      testSerializer(df1, Seq(Seq("_1")))
      checkAnswer(df1, Seq(Row(Seq("a", "b")), Row(Seq("c", "d"))))

      val df2 = arrayDs.select("_3._2")
      testSerializer(df2, Seq(Seq("_2")))
      checkAnswer(df2, Seq(Row(1.0), Row(2.0)))

      // This is a more complex case. We select two root fields "_1" and "_3".
      // The first serializer creates array of struct of two fields ("_1", "_3") and
      // the field "_3" is a struct of one field "_2".
      // The second serializer creates a struct of just one field "_1".
      val df3 = arrayDs.select("_1._1", "_1._3._2", "_3._1")
      testSerializer(df3, Seq(Seq("_1", "_3"), Seq("_2")), Seq(Seq("_1")))
      checkAnswer(df3, Seq(Row(Seq("a", "b"), Seq(11, 22), "aa"),
        Row(Seq("c", "d"), Seq(33, 44), "bb")))
    }
  }

  test("Prune nested serializers: map of struct") {
    withSQLConf(SQLConf.SERIALIZER_NESTED_SCHEMA_PRUNING_ENABLED.key -> "true") {
      val mapData = Seq((Map(("k", ("a_1", 11))), 1), (Map(("k", ("b_1", 22))), 2),
        (Map(("k", ("c_1", 33))), 3))
      val mapDs = mapData.toDS().map(t => (t._1, t._2 + 1))
      val df1 = mapDs.select("_1.k._1")
      testSerializer(df1, Seq(Seq("_1")))
      checkAnswer(df1, Seq(Row("a_1"), Row("b_1"), Row("c_1")))

      val df2 = mapDs.select("_1.k._2")
      testSerializer(df2, Seq(Seq("_2")))
      checkAnswer(df2, Seq(Row(11), Row(22), Row(33)))

      val df3 = mapDs.select(expr("map_values(_1)._2[0]"))
      testSerializer(df3, Seq(Seq("_2")))
      checkAnswer(df3, Seq(Row(11), Row(22), Row(33)))
    }
  }

  test("Pruned nested serializers: map of complex key") {
    withSQLConf(SQLConf.SERIALIZER_NESTED_SCHEMA_PRUNING_ENABLED.key -> "true") {
      val mapData = Seq((Map((("1", 1), "a_1")), 1), (Map((("2", 2), "b_1")), 2),
        (Map((("3", 3), "c_1")), 3))
      val mapDs = mapData.toDS().map(t => (t._1, t._2 + 1))
      val df1 = mapDs.select(expr("map_keys(_1)._1[0]"))
      testSerializer(df1, Seq(Seq("_1")))
      checkAnswer(df1, Seq(Row("1"), Row("2"), Row("3")))
    }
  }

  test("Pruned nested serializers: map of map value") {
    withSQLConf(SQLConf.SERIALIZER_NESTED_SCHEMA_PRUNING_ENABLED.key -> "true") {
      val mapData = Seq(
        (Map(("k", Map(("k2", ("a_1", 11))))), 1),
        (Map(("k", Map(("k2", ("b_1", 22))))), 2),
        (Map(("k", Map(("k2", ("c_1", 33))))), 3))
      val mapDs = mapData.toDS().map(t => (t._1, t._2 + 1))
      val df = mapDs.select("_1.k.k2._1")
      testSerializer(df, Seq(Seq("_1")))
    }
  }

  test("Pruned nested serializers: map of map key") {
    withSQLConf(SQLConf.SERIALIZER_NESTED_SCHEMA_PRUNING_ENABLED.key -> "true") {
      val mapData = Seq(
        (Map((Map((("1", 1), "val1")), "a_1")), 1),
        (Map((Map((("2", 2), "val2")), "b_1")), 2),
        (Map((Map((("3", 3), "val3")), "c_1")), 3))
      val mapDs = mapData.toDS().map(t => (t._1, t._2 + 1))
      val df = mapDs.select(expr("map_keys(map_keys(_1)[0])._1[0]"))
      testSerializer(df, Seq(Seq("_1")))
      checkAnswer(df, Seq(Row("1"), Row("2"), Row("3")))
    }
  }

  test("SPARK-27871: Dataset encoder should benefit from codegen cache") {
    def checkCodegenCache(createDataset: () => Dataset[_]): Unit = {
      def getCodegenCount(): Long = CodegenMetrics.METRIC_COMPILATION_TIME.getCount()

      val count1 = getCodegenCount()
      // trigger codegen for Dataset
      createDataset().collect()
      val count2 = getCodegenCount()
      // codegen happens
      assert(count2 > count1)

      // trigger codegen for another Dataset of same type
      createDataset().collect()
      // codegen cache should work for Datasets of same type.
      val count3 = getCodegenCount()
      assert(count3 == count2)
    }

    withClue("array type") {
      checkCodegenCache(() => Seq(Seq("abc")).toDS())
    }

    withClue("map type") {
      checkCodegenCache(() => Seq(Map("abc" -> 1)).toDS())
    }

    withClue("array of map") {
      checkCodegenCache(() => Seq(Seq(Map("abc" -> 1))).toDS())
    }
  }

  test("SPARK-32652: Pruned nested serializers: RowEncoder") {
    val df = Seq(("a", 1), ("b", 2), ("c", 3)).toDF("i", "j")
    val encoder = ExpressionEncoder(new StructType().add("s", df.schema))
    val query = df.map(row => Row(row))(encoder).select("s.i")
    testSerializer(query, Seq(Seq("i")))
    checkAnswer(query, Seq(Row("a"), Row("b"), Row("c")))
  }
}
