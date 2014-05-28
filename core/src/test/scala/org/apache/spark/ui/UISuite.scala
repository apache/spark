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

package org.apache.spark.ui

import java.net.ServerSocket
import javax.servlet.http.HttpServletRequest

import scala.io.Source
import scala.language.postfixOps
import scala.util.{Failure, Success, Try}

import org.eclipse.jetty.server.Server
import org.eclipse.jetty.servlet.ServletContextHandler
import org.scalatest.FunSuite
import org.scalatest.concurrent.Eventually._
import org.scalatest.time.SpanSugar._

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.LocalSparkContext._
import scala.xml.Node

class UISuite extends FunSuite {

  ignore("basic ui visibility") {
    withSpark(new SparkContext("local", "test")) { sc =>
      // test if the ui is visible, and all the expected tabs are visible
      eventually(timeout(10 seconds), interval(50 milliseconds)) {
        val html = Source.fromURL(sc.ui.appUIAddress).mkString
        assert(!html.contains("random data that should not be present"))
        assert(html.toLowerCase.contains("stages"))
        assert(html.toLowerCase.contains("storage"))
        assert(html.toLowerCase.contains("environment"))
        assert(html.toLowerCase.contains("executors"))
      }
    }
  }

  ignore("visibility at localhost:4040") {
    withSpark(new SparkContext("local", "test")) { sc =>
      // test if visible from http://localhost:4040
      eventually(timeout(10 seconds), interval(50 milliseconds)) {
        val html = Source.fromURL("http://localhost:4040").mkString
        assert(html.toLowerCase.contains("stages"))
      }
    }
  }

  ignore("attaching a new tab") {
    withSpark(new SparkContext("local", "test")) { sc =>
      val sparkUI = sc.ui

      val newTab = new WebUITab(sparkUI, "foo") {
        attachPage(new WebUIPage("") {
          def render(request: HttpServletRequest): Seq[Node] = {
            <b>"html magic"</b>
          }
        })
      }
      sparkUI.attachTab(newTab)
      eventually(timeout(10 seconds), interval(50 milliseconds)) {
        val html = Source.fromURL(sc.ui.appUIAddress).mkString
        assert(!html.contains("random data that should not be present"))

        // check whether new page exists
        assert(html.toLowerCase.contains("foo"))

        // check whether other pages still exist
        assert(html.toLowerCase.contains("stages"))
        assert(html.toLowerCase.contains("storage"))
        assert(html.toLowerCase.contains("environment"))
        assert(html.toLowerCase.contains("executors"))
      }

      eventually(timeout(10 seconds), interval(50 milliseconds)) {
        val html = Source.fromURL(sc.ui.appUIAddress.stripSuffix("/") + "/foo").mkString
        // check whether new page exists
        assert(html.contains("magic"))
      }
    }
  }

  test("jetty selects different port under contention") {
    val startPort = 4040
    val server = new Server(startPort)

    Try { server.start() } match {
      case Success(s) =>
      case Failure(e) =>
      // Either case server port is busy hence setup for test complete
    }
    val serverInfo1 = JettyUtils.startJettyServer(
      "0.0.0.0", startPort, Seq[ServletContextHandler](), new SparkConf)
    val serverInfo2 = JettyUtils.startJettyServer(
      "0.0.0.0", startPort, Seq[ServletContextHandler](), new SparkConf)
    // Allow some wiggle room in case ports on the machine are under contention
    val boundPort1 = serverInfo1.boundPort
    val boundPort2 = serverInfo2.boundPort
    assert(boundPort1 != startPort)
    assert(boundPort2 != startPort)
    assert(boundPort1 != boundPort2)
  }

  test("jetty binds to port 0 correctly") {
    val serverInfo = JettyUtils.startJettyServer(
      "0.0.0.0", 0, Seq[ServletContextHandler](), new SparkConf)
    val server = serverInfo.server
    val boundPort = serverInfo.boundPort
    assert(server.getState === "STARTED")
    assert(boundPort != 0)
    Try { new ServerSocket(boundPort) } match {
      case Success(s) => fail("Port %s doesn't seem used by jetty server".format(boundPort))
      case Failure(e) =>
    }
  }

  test("verify appUIAddress contains the scheme") {
    withSpark(new SparkContext("local", "test")) { sc =>
      val uiAddress = sc.ui.appUIAddress
      assert(uiAddress.equals("http://" + sc.ui.appUIHostPort))
    }
  }

  test("verify appUIAddress contains the port") {
    withSpark(new SparkContext("local", "test")) { sc =>
      val splitUIAddress = sc.ui.appUIAddress.split(':')
      assert(splitUIAddress(2).toInt == sc.ui.boundPort)
    }
  }
}
