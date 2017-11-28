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

import org.apache.spark.ml.Transformer
import org.apache.spark.sql.{DataFrame, Encoder, Row}
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.streaming.StreamTest

trait TransformerStreamTest extends StreamTest {

  def testTransformer[A : Encoder](dataframe: DataFrame, transformers: Seq[Transformer],
      selectResCols: Seq[String])(checkFunction: Row => Unit): Unit = {

    val columnNames = dataframe.schema.fieldNames
    val stream = MemoryStream[A]
    val streamDF = stream.toDS().toDF(columnNames: _*)

    val data = dataframe.as[A].collect()

    val streamOutput = transformers.foldLeft(streamDF) {
      case (data, transformer) => transformer.transform(data)
    }.select(selectResCols(0), selectResCols.takeRight(selectResCols.length - 1): _*)
    testStream(streamOutput) (
      AddData(stream, data: _*),
      CheckAnswer(checkFunction)
    )

    val dfOutput = transformers.foldLeft(dataframe) {
      case (data, transformer) => transformer.transform(data)
    }
    dfOutput.collect().foreach {row =>
      checkFunction(row)
    }
  }

}
