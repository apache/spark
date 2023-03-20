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

/*
 * Created by ruilibuaa
 * 2023-3-19
 */

package org.apache.spark.sql

import org.apache.spark.SparkException
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.test.SharedSparkSession


class LegacyErrorTempSuit extends SharedSparkSession {


  test("CANNOT_ZIP_MAPS") {

    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val maxRoundedArrayLength = 2

    val zipMapsbyUniqueKey: UserDefinedFunction = udf(
      (mapA: Map[String, Int], mapB: Map[String, Int]) => {
        if (mapA.keySet.intersect(mapB.keySet).nonEmpty) {
          throw new
              IllegalArgumentException(s"Maps have overlapping keys")
        }
        val mergedMap = mapA ++ mapB
        val mergeMapSize = mergedMap.size
        // GlobalVariables.mergeMapSize = Some(mergeMapSize)
        if (mergeMapSize > maxRoundedArrayLength) {
          throw new
              SparkException(s"Unsuccessful try to zip maps with $mergeMapSize unique keys " +
                s"due to exceeding the array size limit $maxRoundedArrayLength.")
        }
        mergedMap
      }
    )

    val data = Seq(
      (1, Map("a" -> 1, "b" -> 2), Map("c" -> 3, "d" -> 4)),
      (2, Map("e" -> 5, "f" -> 6), Map("g" -> 7, "h" -> 8))
    )

    val exception = intercept[SparkException] {
      val df = data.toDF("id", "map1", "map2")
      val zippedDf = df.withColumn("zipped_map", zipMapsbyUniqueKey(col("map1"), col("map2")))
      zippedDf.collect()
    }

    val errorMessage = exception.getMessage
    if (errorMessage.contains("exceeding the array size limit")) {
      assert(errorMessage.contains(s"$maxRoundedArrayLength"))
    } else {
      fail("Unexpected error message")
    }

  }

}
