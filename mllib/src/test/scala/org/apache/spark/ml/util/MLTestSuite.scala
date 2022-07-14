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

package org.apache.spark.ml.util

import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.sql.Row

class MLTestSuite extends MLTest {

  import testImplicits._

  test("test transformer on stream data") {

    val data = Seq((0, "a"), (1, "b"), (2, "c"), (3, "d"), (4, "e"), (5, "f"))
      .toDF("id", "label")
    val indexer = new StringIndexer().setStringOrderType("alphabetAsc")
      .setInputCol("label").setOutputCol("indexed")
    val indexerModel = indexer.fit(data)
    testTransformer[(Int, String)](data, indexerModel, "id", "indexed") {
      case Row(id: Int, indexed: Double) =>
        assert(id === indexed.toInt)
    }
    testTransformerByGlobalCheckFunc[(Int, String)] (data, indexerModel, "id", "indexed") { rows =>
      assert(rows.map(_.getDouble(1)).max === 5.0)
    }

    intercept[Exception] {
      testTransformerOnStreamData[(Int, String)](data, indexerModel, "id", "indexed") {
        case Row(id: Int, indexed: Double) =>
          assert(id != indexed.toInt)
      }
    }
    intercept[Exception] {
      testTransformerOnStreamData[(Int, String)](data, indexerModel, "id", "indexed") {
        rows: scala.collection.Seq[Row] =>
          assert(rows.map(_.getDouble(1)).max === 1.0)
      }
    }
  }
}
