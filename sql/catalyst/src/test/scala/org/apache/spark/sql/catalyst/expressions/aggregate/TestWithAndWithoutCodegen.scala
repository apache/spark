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
package org.apache.spark.sql.catalyst.expressions.aggregate

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.expressions.CodegenObjectFactoryMode
import org.apache.spark.sql.catalyst.plans.SQLHelper
import org.apache.spark.sql.internal.SQLConf

trait TestWithAndWithoutCodegen extends SparkFunSuite with SQLHelper {
  def testBothCodegenAndInterpreted(name: String)(f: => Unit): Unit = {
    val modes = Seq(CodegenObjectFactoryMode.CODEGEN_ONLY, CodegenObjectFactoryMode.NO_CODEGEN)
    for (fallbackMode <- modes) {
      test(s"$name with $fallbackMode") {
        withSQLConf(SQLConf.CODEGEN_FACTORY_MODE.key -> fallbackMode.toString) {
          f
        }
      }
    }
  }
}
