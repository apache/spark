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

package org.apache.spark.sql.catalyst.util

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.{DataTypeMismatch, TypeCheckSuccess}
import org.apache.spark.sql.types._

class TypeUtilsSuite extends SparkFunSuite {

  private def typeCheckPass(types: Seq[DataType]): Unit = {
    assert(TypeUtils.checkForSameTypeInputExpr(types, "a") == TypeCheckSuccess)
  }

  private def typeCheckFail(types: Seq[DataType]): Unit = {
    assert(TypeUtils.checkForSameTypeInputExpr(types, "a")
      .isInstanceOf[DataTypeMismatch])
  }

  test("checkForSameTypeInputExpr") {
    typeCheckPass(Nil)
    typeCheckPass(StringType :: Nil)
    typeCheckPass(StringType :: StringType :: Nil)

    typeCheckFail(StringType :: IntegerType :: Nil)
    typeCheckFail(StringType :: IntegerType :: Nil)

    // Should also work on arrays. See SPARK-14990
    typeCheckPass(ArrayType(StringType, containsNull = true) ::
      ArrayType(StringType, containsNull = false) :: Nil)
  }
}
