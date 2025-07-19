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

import org.apache.spark.sql.errors.QueryExecutionErrors

object ThetaSketch {
  // Bounds copied from DataSketches' ThetaUtil
  final val MIN_LG_NOM_LONGS = 4
  final val MAX_LG_NOM_LONGS = 26

  /**
   * Validates the lgNomLongs parameter for Theta sketch size.
   * Throws a Spark SQL exception if the value is out of bounds.
   *
   * @param lgNomLongs Log2 of nominal entries
   */
  def checkLgNomLongs(lgNomLongs: Int): Unit = {
    if (lgNomLongs < MIN_LG_NOM_LONGS || lgNomLongs > MAX_LG_NOM_LONGS) {
      throw QueryExecutionErrors.thetaInvalidLgNomEntries(
        min = MIN_LG_NOM_LONGS,
        max = MAX_LG_NOM_LONGS,
        value = lgNomLongs
      )
    }
  }
}
