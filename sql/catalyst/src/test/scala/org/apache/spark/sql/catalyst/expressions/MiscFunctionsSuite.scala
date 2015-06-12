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

package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.dsl.expressions._

class MiscFunctionsSuite extends SparkFunSuite with ExpressionEvalHelper {

  test("md5") {
    val s1 = 'a.string.at(0)
    val s2 = 'a.binary.at(0)
    checkEvaluation(Md5(s1), "902fbdd2b1df0c4f70b4a5d23525e932", create_row("ABC"))
    checkEvaluation(Md5(s2), "6ac1e56bc78f031059be7be854522c4c", create_row(Array[Byte](1,2,3,4,5,6)))
  }

}
