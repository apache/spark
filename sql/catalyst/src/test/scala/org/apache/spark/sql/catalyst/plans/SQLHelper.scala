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
package org.apache.spark.sql.catalyst.plans

import java.io.File
import java.time.ZoneId

import scala.util.control.NonFatal

import org.scalatest.Assertions.fail

import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.catalyst.util.DateTimeTestUtils
import org.apache.spark.sql.catalyst.util.DateTimeUtils.getZoneId
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.util.Utils

trait SQLHelper extends SQLConfHelper {

  /**
   * Generates a temporary path without creating the actual file/directory, then pass it to `f`. If
   * a file/directory is created there by `f`, it will be delete after `f` returns.
   */
  protected def withTempPath(f: File => Unit): Unit = {
    val path = Utils.createTempDir()
    path.delete()
    try f(path) finally Utils.deleteRecursively(path)
  }


  def testSpecialDatetimeValues[T](test: ZoneId => T): Unit = {
    DateTimeTestUtils.outstandingTimezonesIds.foreach { timeZone =>
      withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> timeZone) {
        val zoneId = getZoneId(timeZone)
        // The test can fail around midnight if it gets the reference value
        // before midnight but tested code resolves special value after midnight.
        // Retry can guarantee that both values were taken on the same day.
        try {
          test(zoneId)
        } catch {
          case NonFatal(_) => test(zoneId)
        }
      }
    }
  }

  protected lazy val sparkHome: String = {
    if (!(sys.props.contains("spark.test.home") || sys.env.contains("SPARK_HOME"))) {
      fail("spark.test.home or SPARK_HOME is not set.")
    }
    sys.props.getOrElse("spark.test.home", sys.env("SPARK_HOME"))
  }
}
