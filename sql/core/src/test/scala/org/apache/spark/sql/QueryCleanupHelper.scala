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

package org.apache.spark.sql

import org.scalatest.Assertions

import org.apache.spark.annotation.Experimental
import org.apache.spark.util.SparkErrorUtils

/**
 * Provides [[withTable]], [[withView]], and [[withUserDefinedFunction]]
 */
@Experimental
trait QueryCleanupHelper extends SparkSessionProvider with Assertions {

  /**
   * Drops tables `tableNames` after calling `f`.
   */
  protected def withTable(tableNames: String*)(f: => Unit): Unit = {
    SparkErrorUtils.tryWithSafeFinally(f) {
      tableNames.foreach { name =>
        spark.sql(s"DROP TABLE IF EXISTS $name")
      }
    }
  }

  /**
   * Drops views `viewNames` after calling `f`.
   */
  protected def withView(viewNames: String*)(f: => Unit): Unit = {
    SparkErrorUtils.tryWithSafeFinally(f)(
      viewNames.foreach { name =>
        spark.sql(s"DROP VIEW IF EXISTS $name")
      }
    )
  }

  protected def withUserDefinedFunction(functions: (String, Boolean)*)(f: => Unit): Unit = {
    try {
      f
    } finally {
      functions.foreach { case (functionName, isTemporary) =>
        val withTemporary = if (isTemporary) "TEMPORARY" else ""
        spark.sql(s"DROP $withTemporary FUNCTION IF EXISTS $functionName")
        assert(
          !spark.catalog.functionExists(functionName),
          s"Function $functionName should have been dropped. But, it still exists.")
      }
    }
  }
}
