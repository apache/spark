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

import java.nio.ByteBuffer

import org.apache.spark.SparkContext
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{SparkSession, SQLContext}


/**
 * An instance of the Spark SQL execution engine that integrates with data stored in Hive.
 * Configuration for Hive is read from hive-site.xml on the classpath.
 */
@deprecated("Use SparkSession.builder.enableHiveSupport instead", "2.0.0")
class HiveContext private[hive](_sparkSession: SparkSession)
  extends SQLContext(_sparkSession) with Logging {

  self =>

  def this(sc: SparkContext) = {
    this(SparkSession.builder().enableHiveSupport().sparkContext(sc).getOrCreate())
  }

  def this(sc: JavaSparkContext) = this(sc.sc)

  /**
   * Returns a new HiveContext as new session, which will have separated SQLConf, UDF/UDAF,
   * temporary tables and SessionState, but sharing the same CacheManager, IsolatedClientLoader
   * and Hive client (both of execution and metadata) with existing HiveContext.
   */
  override def newSession(): HiveContext = {
    new HiveContext(sparkSession.newSession())
  }

  /**
   * Invalidate and refresh all the cached the metadata of the given table. For performance reasons,
   * Spark SQL or the external data source library it uses might cache certain metadata about a
   * table, such as the location of blocks. When those change outside of Spark SQL, users should
   * call this function to invalidate the cache.
   *
   * @since 1.3.0
   */
  def refreshTable(tableName: String): Unit = {
    sparkSession.catalog.refreshTable(tableName)
  }

}

object HiveContext {
  // In spark-thriftserver, session-level config is required. The session-level
  // config has been set to the thread local variable of SessionState during
  // HiveSessionImpl::acquire. But the IsolatedClientLoader will reload the new
  // SessionState for the isolated metastore version, then SessionState::tss
  // will be reconstructed, then session-level config will be missing. And
  // sessionHiveConfBuffer will not be loaded by IsolatedClientLoader and use
  // ByteBuffer to store the config. Therefore sessionHiveConfBuffer is
  // introduced to ensure that the config is not lost.
  private val sessionHiveConfBuffer: ThreadLocal[ByteBuffer] = ThreadLocal.withInitial(() => null)

  def setSessionHiveConfBuffer(buffer: ByteBuffer): Unit = {
    sessionHiveConfBuffer.set(buffer)
  }

  def getSessionHiveConfBuffer(): ByteBuffer = {
    sessionHiveConfBuffer.get()
  }
}
