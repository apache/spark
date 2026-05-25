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

package org.apache.spark.sql.test.connect

import org.apache.spark.sql.{DataFrame, QueryTest => BaseQueryTest, Row}

/**
 * Extends [[BaseQueryTest]] for use with Connect sessions.
 *
 * Overrides [[checkAnswer]] to avoid classic-only code paths (e.g. `queryExecution`,
 * `logicalPlan`, `materializedRdd`) that are not available on Connect DataFrames.
 */
trait QueryTest extends BaseQueryTest with SparkSessionProvider {

  override protected def checkAnswer(df: => DataFrame, expectedAnswer: Seq[Row]): Unit = {
    val sparkAnswer = df.collect().toSeq
    BaseQueryTest.sameRows(expectedAnswer, sparkAnswer) match {
      case Some(errorMessage) => fail(errorMessage)
      case None =>
    }
  }
}
