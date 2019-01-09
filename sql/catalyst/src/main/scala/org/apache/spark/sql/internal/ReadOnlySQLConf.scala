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

package org.apache.spark.sql.internal

import java.util.{Map => JMap}

import org.apache.spark.TaskContext
import org.apache.spark.internal.config.{ConfigEntry, ConfigProvider, ConfigReader}

/**
 * A readonly SQLConf that will be created by tasks running at the executor side. It reads the
 * configs from the local properties which are propagated from driver to executors.
 */
class ReadOnlySQLConf(context: TaskContext) extends SQLConf {

  @transient override val settings: JMap[String, String] = {
    context.getLocalProperties.asInstanceOf[JMap[String, String]]
  }

  @transient override protected val reader: ConfigReader = {
    new ConfigReader(new TaskContextConfigProvider(context))
  }

  override protected def setConfWithCheck(key: String, value: String): Unit = {
    throw new UnsupportedOperationException("Cannot mutate ReadOnlySQLConf.")
  }

  override def unsetConf(key: String): Unit = {
    throw new UnsupportedOperationException("Cannot mutate ReadOnlySQLConf.")
  }

  override def unsetConf(entry: ConfigEntry[_]): Unit = {
    throw new UnsupportedOperationException("Cannot mutate ReadOnlySQLConf.")
  }

  override def clear(): Unit = {
    throw new UnsupportedOperationException("Cannot mutate ReadOnlySQLConf.")
  }

  override def clone(): SQLConf = {
    throw new UnsupportedOperationException("Cannot clone/copy ReadOnlySQLConf.")
  }

  override def copy(entries: (ConfigEntry[_], Any)*): SQLConf = {
    throw new UnsupportedOperationException("Cannot clone/copy ReadOnlySQLConf.")
  }
}

class TaskContextConfigProvider(context: TaskContext) extends ConfigProvider {
  override def get(key: String): Option[String] = Option(context.getLocalProperty(key))
}
