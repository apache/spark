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

class SparkSQLCLIService(hiveContext: HiveContext)
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
