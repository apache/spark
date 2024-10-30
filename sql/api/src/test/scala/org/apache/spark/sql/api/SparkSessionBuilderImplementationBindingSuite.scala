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
package org.apache.spark.sql.api

// scalastyle:off funsuite
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.sql.functions.sum

/**
 * Test suite for SparkSession implementation binding.
 */
trait SparkSessionBuilderImplementationBindingSuite extends AnyFunSuite with BeforeAndAfterAll {
// scalastyle:on
  protected def configure(builder: SparkSessionBuilder): builder.type = builder

  test("range") {
    val session = configure(SparkSession.builder()).getOrCreate()
    import session.implicits._
    val df = session.range(10).agg(sum("id")).as[Long]
    assert(df.head() == 45)
  }
}
