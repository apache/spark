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

package org.apache.spark.sql.jdbc

import java.sql.{Connection, DriverManager}
import java.util.Properties
import java.util.logging.Logger

object UnserializableDriverHelper {

  import scala.collection.JavaConverters._

  def replaceDriverDuring[T](f: => T): T = {
    object UnserializableH2Driver extends org.h2.Driver {

      override def connect(url: String, info: Properties): Connection = {

        val result = super.connect(url, info)
        info.put("unserializableDriver", this)
        result
      }

      override def getParentLogger: Logger = null
    }

    val oldDrivers = DriverManager.getDrivers.asScala.toList.filter(_.acceptsURL("jdbc:h2:"))
    oldDrivers.foreach(DriverManager.deregisterDriver)
    DriverManager.registerDriver(UnserializableH2Driver)

    val result = try {
      f
    } finally {
      DriverManager.deregisterDriver(UnserializableH2Driver)
      oldDrivers.foreach(DriverManager.registerDriver)
    }
    result
  }
}
