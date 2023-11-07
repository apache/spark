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

package org.apache.spark.sql.catalyst.analysis

import scala.collection.immutable.LazyList

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.types._

/**
 * Note: this test supports Scala 2.13. A parallel source tree has a 2.12 implementation.
 */
class ExtractGeneratorSuite extends AnalysisTest {

  test("SPARK-34141: ExtractGenerator with lazy project list") {
    val b = AttributeReference("b", ArrayType(StringType))()

    val columns = AttributeReference("a", StringType)() #:: b #:: LazyList.empty
    val explode = Alias(Explode(b), "c")()

    val rel = LocalRelation(output = columns)
    val plan = Project(rel.output ++ (explode :: Nil), rel)

    assertAnalysisSuccess(plan)
  }
}
