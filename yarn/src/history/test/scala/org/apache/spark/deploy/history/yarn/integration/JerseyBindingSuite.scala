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

package org.apache.spark.deploy.history.yarn.integration

import java.io.{ByteArrayInputStream, FileNotFoundException, IOException}
import java.net.URI

import com.sun.jersey.api.client.{ClientHandlerException, ClientResponse, UniformInterfaceException}

import org.apache.spark.deploy.history.yarn.rest.{JerseyBinding, UnauthorizedRequestException}
import org.apache.spark.deploy.history.yarn.testtools.AbstractYarnHistoryTests

/**
 * Unit test of how well the Jersey Binding works -especially some error handling logic
 * Which can follow different paths
 */
class JerseyBindingSuite extends AbstractYarnHistoryTests {

  val uriPath = "http://spark.apache.org"
  val uri = new URI(uriPath)

  def translate(ex: Throwable): Throwable = {
    JerseyBinding.translateException("GET", uri, ex)
  }

  /**
   * Build a [[UniformInterfaceException]] with the given string body
   * @param status status code
   * @param body body message
   * @param buffer buffer flag
   * @return new instance
   */
  def newUIE(status: Int, body: String, buffer: Boolean ): UniformInterfaceException = {
    val response = new ClientResponse(status,
             null,
             new ByteArrayInputStream(body.getBytes("UTF-8")),
             null)
    new UniformInterfaceException(response, buffer)
  }

  /**
   * If a [[ClientHandlerException]] contains an IOE, it
   * is unwrapped and returned
   */
  test("UnwrapIOEinClientHandler") {
    val fnfe = new FileNotFoundException("/tmp")
    val che = new ClientHandlerException(fnfe)
    assertResult(fnfe) {
      translate(che)
    }
  }

  /**
   * If a [[ClientHandlerException]] does not contains an IOE, it
   * is wrapped, but the inner text is extracted
   */
  test("BuildIOEinClientHandler") {
    val npe = new NullPointerException("oops")
    val che = new ClientHandlerException(npe)
    val ex = translate(che)
    assert(che === ex.getCause)
    assertExceptionDetails(ex, "oops", uriPath)
  }

  /**
   * If a [[ClientHandlerException]] does not contains an IOE, it
   * is unwrapped and returned
   */
  test("EmptyClientHandlerException") {
    val che = new ClientHandlerException("che")
    val ex = translate(che)
    assert(che === ex.getCause)
    assertExceptionDetails(ex, "che", uriPath)
  }

  /**
   * If the URI passed into translating a CHE is null, no
   * URI is printed
   */
  test("Null URI for ClientHandlerException") {
    val che = new ClientHandlerException("che")
    val ex = JerseyBinding.translateException("POST", null, che)
    assert(che === ex.getCause)
    assertExceptionDetails(ex, "POST", "unknown")
  }

  test("UniformInterfaceException null response") {
    // bufferResponseEntity must be false to avoid triggering NPE in constructor
    val uie = new UniformInterfaceException("uae", null, false)
    val ex = translate(uie)
    assert(uie === ex.getCause)
    assertExceptionDetails(ex, "uae", uriPath)
  }

  test("UniformInterfaceException 404 no body response") {
    val uie = newUIE(404, "", false)
    val ex = translate(uie)
    assert(uie === ex.getCause)
    assert(ex.isInstanceOf[FileNotFoundException], s"not FileNotFoundException: $ex")
    assertExceptionDetails(ex, uriPath, uriPath)
  }

  test("UniformInterfaceException 403 forbidden") {
    val uie = newUIE(403, "forbidden", false)
    val ex = translate(uie)
    assert(uie === ex.getCause)
    assert(ex.isInstanceOf[UnauthorizedRequestException], s"not UnauthorizedRequestException: $ex")
    assertExceptionDetails(ex, "Forbidden", uriPath)
  }

  test("UniformInterfaceException 500 response") {
    val uie = newUIE(500, "internal error", false)
    val ex = translate(uie)
    assert(uie === ex.getCause)
    assert(ex.isInstanceOf[IOException], s"not IOException: $ex")
    assertExceptionDetails(ex, "500", uriPath)
  }

}
