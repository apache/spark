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

package org.apache.spark.deploy.history

import jakarta.servlet.http.HttpServletRequest
import org.mockito.Mockito.{mock, when}

import org.apache.spark.{SparkConf, SparkFunSuite}

class LogPageSuite extends SparkFunSuite {

  test("render encodes the logType parameter embedded in the inline script") {
    val page = new LogPage(new SparkConf(false))
    val request = mock(classOf[HttpServletRequest])
    // A value that would break out of the single-quoted JavaScript string literal, close the
    // script element, or start a new statement if it were emitted into the page verbatim.
    when(request.getParameter("logType")).thenReturn("stdout');alert('xss')</script>")
    val html = page.render(request).mkString

    // The value is URL-encoded, so the quotes, angle brackets and slash that would break out of
    // the inline <script> (or inject extra /log parameters) become inert percent escapes.
    assert(html.contains("logType=stdout%27%29%3Balert%28%27xss%27%29%3C%2Fscript%3E"))
    // The raw, structure-breaking form must never reach the generated query string. (The page
    // title reflects logType too, but there the XML library HTML-escapes it, so it is safe.)
    assert(!html.contains("logType=stdout');"))
  }
}
