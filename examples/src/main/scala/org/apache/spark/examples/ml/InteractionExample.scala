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

// scalastyle:off println
package org.apache.spark.examples.ml

// $example on$
import org.apache.spark.ml.feature.Interaction
// $example off$
import org.apache.spark.sql.SparkSession

object InteractionExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("InteractionExample")
      .getOrCreate()

    // $example on$
    val df = spark.createDataFrame(Seq(
      (0, 1, 2),
      (1, 4, 3),
      (2, 6, 1),
      (3, 10, 8),
      (4, 9, 2),
      (5, 1, 1)
    )).toDF("id1", "id2", "id3")

    val interaction = new Interaction()
      .setInputCols(Array("id1", "id2", "id3"))
      .setOutputCol("interactedCol")

    val interacted = interaction.transform(df)

    interacted.show()
    // $example off$

    spark.stop()
  }
}
// scalastyle:on println
