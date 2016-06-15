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

package org.apache.spark.ml.linalg

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.types._

class MatrixUDTSuite extends SparkFunSuite {

  test("preloaded MatrixUDT") {
    val dm1 = new DenseMatrix(2, 2, Array(0.9, 1.2, 2.3, 9.8))
    val dm2 = new DenseMatrix(3, 2, Array(0.0, 1.21, 2.3, 9.8, 9.0, 0.0))
    val dm3 = new DenseMatrix(0, 0, Array())
    val sm1 = dm1.toSparse
    val sm2 = dm2.toSparse
    val sm3 = dm3.toSparse

    for (m <- Seq(dm1, dm2, dm3, sm1, sm2, sm3)) {
      val udt = UDTRegistration.getUDTFor(m.getClass.getName).get.newInstance()
        .asInstanceOf[MatrixUDT]
      assert(m === udt.deserialize(udt.serialize(m)))
      assert(udt.typeName == "matrix")
      assert(udt.simpleString == "matrix")
    }
  }
}
