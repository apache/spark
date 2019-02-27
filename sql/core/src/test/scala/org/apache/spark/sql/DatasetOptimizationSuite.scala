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

import org.apache.spark.sql.catalyst.expressions.CreateNamedStruct
import org.apache.spark.sql.catalyst.plans.logical.SerializeFromObject
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSQLContext

class DatasetOptimizationSuite extends QueryTest with SharedSQLContext {
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

    serializer.serializer.zip(structFields).foreach { case (serializer, fields) =>
      val structs = serializer.collect {
        case c: CreateNamedStruct => c
      }
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
}
