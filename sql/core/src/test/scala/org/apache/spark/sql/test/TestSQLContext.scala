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

package org.apache.spark.sql.test

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLConf, SQLContext}


/**
 * A special [[SQLContext]] prepared for testing.
 */
private[sql] class TestSQLContext(sc: SparkContext) extends SQLContext(sc) { self =>

  def this() {
    this(new SparkContext("local[2]", "test-sql-context",
      new SparkConf().set("spark.sql.testkey", "true")))
  }

  protected[sql] override lazy val conf: SQLConf = new SQLConf {

    clear()

    override def clear(): Unit = {
      super.clear()

      // Make sure we start with the default test configs even after clear
      TestSQLContext.overrideConfs.map {
        case (key, value) => setConfString(key, value)
      }
    }
  }

  // Needed for Java tests
  def loadTestData(): Unit = {
    testData.loadTestData()
  }

  private object testData extends SQLTestData {
    protected override def sqlContext: SQLContext = self
  }
}

private[sql] object TestSQLContext {

  /**
   * A map used to store all confs that need to be overridden in sql/core unit tests.
   */
  val overrideConfs: Map[String, String] =
    Map(
      // Fewer shuffle partitions to speed up testing.
      SQLConf.SHUFFLE_PARTITIONS.key -> "5")
}
