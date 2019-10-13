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

package org.apache.spark.graph.api

import java.util.Locale

import org.apache.spark.annotation.Evolving
import org.apache.spark.sql.SaveMode

@Evolving
abstract class PropertyGraphWriter(val graph: PropertyGraph) {

  protected var saveMode: SaveMode = SaveMode.ErrorIfExists
  protected var format: String =
    graph.cypherSession.sparkSession.sessionState.conf.defaultDataSourceName

  /**
   * Specifies the behavior when the graph already exists. Options include:
   * <ul>
   * <li>`SaveMode.Overwrite`: overwrite the existing data.</li>
   * <li>`SaveMode.Ignore`: ignore the operation (i.e. no-op).</li>
   * <li>`SaveMode.ErrorIfExists`: throw an exception at runtime.</li>
   * </ul>
   * <p>
   * When writing the default option is `ErrorIfExists`.
   *
   * @since 3.0.0
   */
  def mode(mode: SaveMode): PropertyGraphWriter = {
    mode match {
      case SaveMode.Append =>
        throw new IllegalArgumentException(s"Unsupported save mode: $mode. " +
          "Accepted save modes are 'overwrite', 'ignore', 'error', 'errorifexists'.")
      case _ =>
        this.saveMode = mode
    }
    this
  }

  /**
   * Specifies the behavior when the graph already exists. Options include:
   * <ul>
   * <li>`overwrite`: overwrite the existing graph.</li>
   * <li>`ignore`: ignore the operation (i.e. no-op).</li>
   * <li>`error` or `errorifexists`: default option, throw an exception at runtime.</li>
   * </ul>
   *
   * @since 3.0.0
   */
  def mode(saveMode: String): PropertyGraphWriter = {
    saveMode.toLowerCase(Locale.ROOT) match {
      case "overwrite" => mode(SaveMode.Overwrite)
      case "ignore" => mode(SaveMode.Ignore)
      case "error" | "errorifexists" | "default" => mode(SaveMode.ErrorIfExists)
      case _ => throw new IllegalArgumentException(s"Unknown save mode: $saveMode. " +
        "Accepted save modes are 'overwrite', 'ignore', 'error', 'errorifexists'.")
    }
  }

  /**
   * Specifies the underlying output data format. Built-in options include "parquet", "json", etc.
   *
   * @since 3.0.0
   */
  def format(format: String): PropertyGraphWriter = {
    this.format = format
    this
  }

  /**
   * Saves the content of the `PropertyGraph` at the specified path.
   *
   * @since 3.0.0
   */
  def save(path: String): Unit

}
