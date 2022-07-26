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

package org.apache.spark.jmx

import java.io.{ByteArrayOutputStream, PrintWriter}
import javax.servlet.http.{HttpServletRequest, HttpServletResponse}

import org.mockito.Mockito._
import scala.collection.JavaConverters._

import org.apache.spark.SparkFunSuite

class JMXJsonServletTest extends SparkFunSuite {

  import JMXJsonServlet._

  private val servlet = new JMXJsonServlet

  test("match qryName") {
    // check true
    assert(checkQryName("*:*"))
    assert(checkQryName("key:value"))
    // check false
    assert(!checkQryName("single-key"))
  }

  test("test all attribute") {
    val response = buildResponse()
    val request = mock(classOf[HttpServletRequest])
    val output = new ByteArrayOutputStream()
    when(response.getWriter).thenReturn(new PrintWriter(output))
    servlet.doGet(request, response)
    assert(output.size() > 0)
  }

  test("test bad request") {
    val response = buildResponse()
    val request = mock(classOf[HttpServletRequest])
    val output = new ByteArrayOutputStream()
    when(response.getWriter).thenReturn(new PrintWriter(output))
    when(request.getParameter("qry")).thenReturn("single-key")
    // do get here
    servlet.doGet(request, response)
    // {
    //  "result" : "ERROR",
    //  "message" : "query format is not as expected."
    // }
    assert(output.toString().contains("ERROR"))
  }

  test("qry *:*") {
    val response = buildResponse()
    val request = mock(classOf[HttpServletRequest])
    val output = new ByteArrayOutputStream()
    when(response.getWriter).thenReturn(new PrintWriter(output))
    servlet.doGet(request, response)
    // scalastyle:off
    println(output.toString())
  }

  private def buildResponse(): HttpServletResponse = {
    val response = mock(classOf[HttpServletResponse])
    buildBasicResponse(response)
    when(response.getHeaderNames).thenReturn(List(
      ACCESS_CONTROL_ALLOW_ORIGIN, ACCESS_CONTROL_ALLOW_METHODS
    ).asJava)
    response
  }
}
