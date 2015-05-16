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

package org.apache.spark.sql.hive

import org.apache.spark.annotation.Experimental
import org.apache.spark.sql.{DataFrame, SaveMode}

package object orc {
  /**
   * ::Experimental::
   *
   * Extra ORC file loading functionality on [[HiveContext]] through implicit conversion.
   *
   * @since 1.4.0
   */
  @Experimental
  implicit class OrcContext(sqlContext: HiveContext) {
    /**
     * ::Experimental::
     *
     * Loads specified Parquet files, returning the result as a [[DataFrame]].
     *
     * @since 1.4.0
     */
    @Experimental
    @scala.annotation.varargs
    def orcFile(paths: String*): DataFrame = {
      val orcRelation = OrcRelation(paths.toArray, Map.empty)(sqlContext)
      sqlContext.baseRelationToDataFrame(orcRelation)
    }
  }

  /**
   * ::Experimental::
   *
   * Extra ORC file writing functionality on [[DataFrame]] through implicit conversion
   *
   * @since 1.4.0
   */
  @Experimental
  implicit class OrcDataFrame(dataFrame: DataFrame) {
    /**
     * ::Experimental::
     *
     * Saves the contents of this [[DataFrame]] as an ORC file, preserving the schema.  Files that
     * are written out using this method can be read back in as a [[DataFrame]] using
     * [[OrcContext.orcFile()]].
     *
     * @since 1.4.0
     */
    @Experimental
    def saveAsOrcFile(path: String, mode: SaveMode = SaveMode.Overwrite): Unit = {
      dataFrame.save(path, source = classOf[DefaultSource].getCanonicalName, mode)
    }
  }

  // This constant duplicates `OrcInputFormat.SARG_PUSHDOWN`, which is unfortunately not public.
  private[orc] val SARG_PUSHDOWN = "sarg.pushdown"
}
