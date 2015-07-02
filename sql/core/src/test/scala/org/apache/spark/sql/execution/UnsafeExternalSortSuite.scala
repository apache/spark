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

package org.apache.spark.sql.execution

import scala.util.Random

import org.scalatest.BeforeAndAfterAll

import org.apache.spark.sql.{RandomDataGenerator, Row, SQLConf}
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.test.TestSQLContext
import org.apache.spark.sql.types._


class UnsafeExternalSortSuite extends SparkPlanTest with BeforeAndAfterAll {

  override def beforeAll(): Unit = {
    TestSQLContext.conf.setConf(SQLConf.CODEGEN_ENABLED, true)
  }

  override def afterAll(): Unit = {
    TestSQLContext.conf.setConf(SQLConf.CODEGEN_ENABLED, SQLConf.CODEGEN_ENABLED.defaultValue.get)
  }

  // Test sorting on different data types
  (DataTypeTestUtils.atomicTypes ++ Set(NullType)).foreach{ dataType =>
    for (nullable <- Seq(true, false)) {
      RandomDataGenerator.forType(dataType, nullable).foreach { randomDataGenerator =>
        test(s"sorting on $dataType with nullable=$nullable") {
          val inputData = Seq.fill(1024)(randomDataGenerator()).filter {
            case d: Double => !d.isNaN
            case f: Float => !java.lang.Float.isNaN(f)
            case x => true
          }
          val inputDf = TestSQLContext.createDataFrame(
            TestSQLContext.sparkContext.parallelize(Random.shuffle(inputData).map(v => Row(v))),
            StructType(StructField("a", dataType, nullable = true) :: Nil)
          )
          checkAnswer(
            inputDf,
            UnsafeExternalSort('a.asc :: Nil, global = false, _: SparkPlan),
            Sort('a.asc :: Nil, global = false, _: SparkPlan)
          )
        }
      }
    }
  }
}
