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

import scala.xml.Elem

import org.apache.spark.SparkFunSuite

class UIUtilsSuite extends SparkFunSuite {
  import UIUtils._

  test("makeDescription") {
    verify(
      """test <a href="/link"> text </a>""",
      <span class="description-input">test <a href="/link"> text </a></span>,
      "Correctly formatted text with only anchors and relative links should generate HTML"
    )

    verify(
      """test <a href="/link" text </a>""",
      <span class="description-input">{"""test <a href="/link" text </a>"""}</span>,
      "Badly formatted text should make the description be treated as a streaming instead of HTML"
    )

    verify(
      """test <a href="link"> text </a>""",
      <span class="description-input">{"""test <a href="link"> text </a>"""}</span>,
      "Non-relative links should make the description be treated as a string instead of HTML"
    )

    verify(
      """test<a><img></img></a>""",
      <span class="description-input">{"""test<a><img></img></a>"""}</span>,
      "Non-anchor elements should make the description be treated as a string instead of HTML"
    )

    verify(
      """test <a href="/link"> text </a>""",
      <span class="description-input">test <a href="base/link"> text </a></span>,
      baseUrl = "base",
      errorMsg = "Base URL should be prepended to html links"
    )
  }

  private def verify(
      desc: String, expected: Elem, errorMsg: String = "", baseUrl: String = ""): Unit = {
    val generated = makeDescription(desc, baseUrl)
    assert(generated.sameElements(expected),
      s"\n$errorMsg\n\nExpected:\n$expected\nGenerated:\n$generated")
  }
}
