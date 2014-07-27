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

package org.apache.spark.sql.hive.thriftserver

import scala.collection.JavaConversions._

import java.io.IOException
import java.util.{List => JList}
import javax.security.auth.login.LoginException

import org.apache.commons.logging.Log
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.shims.ShimLoader
import org.apache.hive.service.Service.STATE
import org.apache.hive.service.auth.HiveAuthFactory
import org.apache.hive.service.cli.CLIService
import org.apache.hive.service.{AbstractService, Service, ServiceException}

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.hive.thriftserver.ReflectionUtils._

private[hive] class SparkSQLCLIService(hiveContext: HiveContext)
  extends CLIService
  with ReflectedCompositeService {

  override def init(hiveConf: HiveConf) {
    setSuperField(this, "hiveConf", hiveConf)

    val sparkSqlSessionManager = new SparkSQLSessionManager(hiveContext)
    setSuperField(this, "sessionManager", sparkSqlSessionManager)
    addService(sparkSqlSessionManager)

    try {
      HiveAuthFactory.loginFromKeytab(hiveConf)
      val serverUserName = ShimLoader.getHadoopShims
        .getShortUserName(ShimLoader.getHadoopShims.getUGIForConf(hiveConf))
      setSuperField(this, "serverUserName", serverUserName)
    } catch {
      case e @ (_: IOException | _: LoginException) =>
        throw new ServiceException("Unable to login to kerberos with given principal/keytab", e)
    }

    initCompositeService(hiveConf)
  }
}

private[thriftserver] trait ReflectedCompositeService { this: AbstractService =>
  def initCompositeService(hiveConf: HiveConf) {
    // Emulating `CompositeService.init(hiveConf)`
    val serviceList = getAncestorField[JList[Service]](this, 2, "serviceList")
    serviceList.foreach(_.init(hiveConf))

    // Emulating `AbstractService.init(hiveConf)`
    invoke(classOf[AbstractService], this, "ensureCurrentState", classOf[STATE] -> STATE.NOTINITED)
    setAncestorField(this, 3, "hiveConf", hiveConf)
    invoke(classOf[AbstractService], this, "changeState", classOf[STATE] -> STATE.INITED)
    getAncestorField[Log](this, 3, "LOG").info(s"Service: $getName is inited.")
  }
}
