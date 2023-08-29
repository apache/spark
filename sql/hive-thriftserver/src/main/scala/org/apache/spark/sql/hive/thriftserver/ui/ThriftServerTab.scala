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

package org.apache.spark.sql.hive.thriftserver.ui

import javax.servlet.http.{HttpServlet, HttpServletRequest, HttpServletResponse}

import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.ui.{SparkUI, SparkUITab}
import org.apache.spark.ui.JettyUtils.createServletHandler

/**
 * Spark Web UI tab that shows statistics of jobs running in the thrift server.
 * This assumes the given SparkContext has enabled its SparkUI.
 */
private[thriftserver] class ThriftServerTab(
   val store: HiveThriftServer2AppStatusStore,
   sparkUI: SparkUI) extends SparkUITab(sparkUI, "sqlserver") with Logging {
  override val name = "JDBC/ODBC Server"

  val parent = sparkUI
  val startTime = sparkUI.store.applicationInfo().attempts.head.startTime
  private[thriftserver] val overriddenSQLConf = new java.util.HashMap[String, String]

  attachPage(new ThriftServerPage(this))
  attachPage(new ThriftServerSessionPage(this))
  parent.attachTab(this)
  parent.attachHandler(createServletHandler("/sqlserver/overridesqlconf", new HttpServlet {
    override def doPost(req: HttpServletRequest, resp: HttpServletResponse): Unit = {
      val key = req.getParameter("key")
      val value = req.getParameter("value")
      var alertMsg = ""
      if (SQLConf.isStaticConfigKey(key)) {
        alertMsg = s"alert('Cannot modify the value of a static config: $key.');"
      } else {
        try {
          SQLConf.get.setConfString(key, value) // Used to check if the value is valid.
          overriddenSQLConf.put(key, value)
        } catch {
          case e: Throwable =>
            val msg = s"Failed to override SQL configuration: ${e.getMessage}."
            logWarning(msg)
            alertMsg = s"alert('$msg');"
        }
      }
      resp.setContentType("text/html; charset=UTF-8");
      // scalastyle:off println
      resp.getWriter
        .println(s"<script>$alertMsg;window.location.replace(document.referrer);</script>")
      // scalastyle:on println
    }

    override def doGet(req: HttpServletRequest, resp: HttpServletResponse): Unit = {
      doPost(req, resp)
    }
  }, ""))
  def detach(): Unit = {
    sparkUI.detachTab(this)
  }

  override def displayOrder: Int = 1
}

private[thriftserver] object ThriftServerTab {
  def getSparkUI(sparkContext: SparkContext): SparkUI = {
    sparkContext.ui.getOrElse {
      throw QueryExecutionErrors.parentSparkUIToAttachTabNotFoundError()
    }
  }
}
