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

import org.apache.spark.ml.param.ParamsSuite
import org.apache.spark.ml.util.{DefaultReadWriteTest, MLTest}
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.apache.spark.storage.StorageLevel

class SQLTransformerSuite extends MLTest with DefaultReadWriteTest {

  import testImplicits._

  test("params") {
    ParamsSuite.checkParams(new SQLTransformer())
  }

  test("transform numeric data") {
    val original = Seq((0, 1.0, 3.0), (2, 2.0, 5.0)).toDF("id", "v1", "v2")
    val sqlTrans = new SQLTransformer().setStatement(
      "SELECT *, (v1 + v2) AS v3, (v1 * v2) AS v4 FROM __THIS__")
     val expected = Seq((0, 1.0, 3.0, 4.0, 3.0), (2, 2.0, 5.0, 7.0, 10.0))
      .toDF("id", "v1", "v2", "v3", "v4")
    val resultSchema = sqlTrans.transformSchema(original.schema)
    testTransformerByGlobalCheckFunc[(Int, Double, Double)](
      original,
      sqlTrans,
      "id",
      "v1",
      "v2",
      "v3",
      "v4") { rows =>
      assert(rows.head.schema.toString == resultSchema.toString)
      assert(resultSchema == expected.schema)
      assert(rows == expected.collect().toSeq)
      assert(original.sparkSession.catalog.listTables().count() == 0)
    }
  }

  test("read/write") {
    val t = new SQLTransformer()
      .setStatement("select * from __THIS__")
    testDefaultReadWrite(t)
  }

  test("transformSchema") {
    val df = spark.range(10)
    val outputSchema = new SQLTransformer()
      .setStatement("SELECT id + 1 AS id1 FROM __THIS__")
      .transformSchema(df.schema)
    val expected = StructType(Seq(StructField("id1", LongType, nullable = false)))
    assert(outputSchema === expected)
  }

  test("SPARK-22538: SQLTransformer should not unpersist given dataset") {
    val df = spark.range(10).toDF()
    df.cache()
    df.count()
    assert(df.storageLevel != StorageLevel.NONE)
    val sqlTrans = new SQLTransformer()
      .setStatement("SELECT id + 1 AS id1 FROM __THIS__")
    testTransformerByGlobalCheckFunc[Long](df, sqlTrans, "id1") { _ => }
    assert(df.storageLevel != StorageLevel.NONE)
  }
}
