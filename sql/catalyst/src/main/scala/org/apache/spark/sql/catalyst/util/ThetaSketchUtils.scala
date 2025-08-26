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

object ThetaSketchUtils {
  /*
   * Bounds copied from DataSketches' ThetaUtil. These define the valid range for lgNomEntries,
   * which is the log-base-2 of the nominal number of entries that determines the sketch size.
   * The actual number of buckets in the sketch = 2^lgNomEntries.
   * MIN_LG_NOM_LONGS = 4 means minimum 16 buckets (2^4), MAX_LG_NOM_LONGS = 26 means maximum
   * ~67 million buckets (2^26). These bounds ensure reasonable memory usage while maintaining
   * sketch accuracy for cardinality estimation.
  */
  final val MIN_LG_NOM_LONGS = 4
  final val MAX_LG_NOM_LONGS = 26
  final val DEFAULT_LG_NOM_LONGS = 12

  /**
   * Validates the lgNomLongs parameter for Theta sketch size. Throws a Spark SQL exception if the
   * value is out of bounds.
   *
   * @param lgNomLongs
   *   Log2 of nominal entries
   */
  def checkLgNomLongs(lgNomLongs: Int): Unit = {
    if (lgNomLongs < MIN_LG_NOM_LONGS || lgNomLongs > MAX_LG_NOM_LONGS) {
      throw QueryExecutionErrors.thetaInvalidLgNomEntries(
        min = MIN_LG_NOM_LONGS,
        max = MAX_LG_NOM_LONGS,
        value = lgNomLongs)
    }
  }
}
