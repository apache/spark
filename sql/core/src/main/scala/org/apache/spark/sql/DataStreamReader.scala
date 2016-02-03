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

import scala.collection.JavaConverters._

import org.apache.spark.Logging
import org.apache.spark.annotation.Experimental
import org.apache.spark.sql.execution.datasources.ResolvedDataSource
import org.apache.spark.sql.execution.streaming.StreamingRelation
import org.apache.spark.sql.types.StructType

/**
 * :: Experimental ::
 * An interface to reading streaming data.  Use `sqlContext.streamFrom` to access these methods.
 *
 * {{{
 *   val df = sqlContext.streamFrom
 *    .format("...")
 *    .open()
 * }}}
 */
@Experimental
class DataStreamReader private[sql](sqlContext: SQLContext) extends Logging {

  /**
   * Specifies the input data source format.
   *
   * @since 2.0.0
   */
  def format(source: String): DataStreamReader = {
    this.source = source
    this
  }

  /**
   * Specifies the input schema. Some data streams (e.g. JSON) can infer the input schema
   * automatically from data. By specifying the schema here, the underlying data stream can
   * skip the schema inference step, and thus speed up data reading.
   *
   * @since 2.0.0
   */
  def schema(schema: StructType): DataStreamReader = {
    this.userSpecifiedSchema = Option(schema)
    this
  }

  /**
   * Adds an input option for the underlying data stream.
   *
   * @since 2.0.0
   */
  def option(key: String, value: String): DataStreamReader = {
    this.extraOptions += (key -> value)
    this
  }

  /**
   * (Scala-specific) Adds input options for the underlying data stream.
   *
   * @since 2.0.0
   */
  def options(options: scala.collection.Map[String, String]): DataStreamReader = {
    this.extraOptions ++= options
    this
  }

  /**
   * Adds input options for the underlying data stream.
   *
   * @since 2.0.0
   */
  def options(options: java.util.Map[String, String]): DataStreamReader = {
    this.options(options.asScala)
    this
  }

  /**
   * Loads streaming input in as a [[DataFrame]], for data streams that don't require a path (e.g.
   * external key-value stores).
   *
   * @since 2.0.0
   */
  def open(): DataFrame = {
    val resolved = ResolvedDataSource.createSource(
      sqlContext,
      userSpecifiedSchema = userSpecifiedSchema,
      providerName = source,
      options = extraOptions.toMap)
    DataFrame(sqlContext, StreamingRelation(resolved))
  }

  /**
   * Loads input in as a [[DataFrame]], for data streams that read from some path.
   *
   * @since 2.0.0
   */
  def open(path: String): DataFrame = {
    option("path", path).open()
  }

  ///////////////////////////////////////////////////////////////////////////////////////
  // Builder pattern config options
  ///////////////////////////////////////////////////////////////////////////////////////

  private var source: String = sqlContext.conf.defaultDataSourceName

  private var userSpecifiedSchema: Option[StructType] = None

  private var extraOptions = new scala.collection.mutable.HashMap[String, String]

}
