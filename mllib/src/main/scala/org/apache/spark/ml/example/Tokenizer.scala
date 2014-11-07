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

package org.apache.spark.ml.example

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.{HasInputCol, HasOutputCol, ParamMap}
import org.apache.spark.sql.SchemaRDD
import org.apache.spark.sql.catalyst.analysis.Star
import org.apache.spark.sql.catalyst.dsl._

/**
 * A simple tokenizer that splits input string by white spaces.
 */
class Tokenizer extends Transformer with HasInputCol with HasOutputCol {

  def setInputCol(value: String) = { set(inputCol, value); this }
  def setOutputCol(value: String) = { set(outputCol, value); this }

  override def transform(dataset: SchemaRDD, paramMap: ParamMap): SchemaRDD = {
    import dataset.sqlContext._
    val map = this.paramMap ++ paramMap
    val split: String => Seq[String] = (text) => {
      text.split("\\s").toSeq
    }
    dataset.select(Star(None), split.call(map(inputCol).attr) as map(outputCol))
  }
}
