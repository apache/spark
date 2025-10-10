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

package org.apache.spark.ml.regression

import org.apache.spark.SparkFunSuite
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.DataFrame

class ArimaRegressionSuite extends SparkFunSuite {

  test("basic model fit and transform") {
    val spark = sparkSession
    import spark.implicits._

    val df = Seq(1.0, 2.0, 3.0, 4.0).toDF("y")
    val arima = new ArimaRegression().setP(1).setD(0).setQ(1)
    val model = arima.fit(df)

    val transformed = model.transform(df)
    assert(transformed.columns.contains("prediction"))
    assert(transformed.count() == df.count())
  }
}

