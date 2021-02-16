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

package org.apache.spark.sql.catalyst.expressions.xml

import javax.xml.xpath.XPathConstants.STRING

import org.w3c.dom.Node
import org.w3c.dom.NodeList

import org.apache.spark.SparkFunSuite

/**
 * Unit tests for [[UDFXPathUtil]]. Loosely based on Hive's TestUDFXPathUtil.java.
 */
class UDFXPathUtilSuite extends SparkFunSuite {

  private lazy val util = new UDFXPathUtil

  test("illegal arguments") {
    // null args
    assert(util.eval(null, "a/text()", STRING) == null)
    assert(util.eval("<a><b>b1</b><b>b2</b><b>b3</b><c>c1</c><c>c2</c></a>", null, STRING) == null)
    assert(
      util.eval("<a><b>b1</b><b>b2</b><b>b3</b><c>c1</c><c>c2</c></a>", "a/text()", null) == null)

    // empty String args
    assert(util.eval("", "a/text()", STRING) == null)
    assert(util.eval("<a><b>b1</b><b>b2</b><b>b3</b><c>c1</c><c>c2</c></a>", "", STRING) == null)

    // wrong expression:
    intercept[RuntimeException] {
      util.eval("<a><b>b1</b><b>b2</b><b>b3</b><c>c1</c><c>c2</c></a>", "a/text(", STRING)
    }
  }

  test("generic eval") {
    val ret =
      util.eval("<a><b>b1</b><b>b2</b><b>b3</b><c>c1</c><c>c2</c></a>", "a/c[2]/text()", STRING)
    assert(ret == "c2")
  }

  test("boolean eval") {
    var ret =
      util.evalBoolean("<a><b>true</b><b>false</b><b>b3</b><c>c1</c><c>c2</c></a>", "a/b[1]/text()")
    assert(ret)

    ret = util.evalBoolean("<a><b>true</b><b>false</b><b>b3</b><c>c1</c><c>c2</c></a>", "a/b[4]")
    assert(ret == false)
  }

  test("string eval") {
    var ret =
      util.evalString("<a><b>true</b><b>false</b><b>b3</b><c>c1</c><c>c2</c></a>", "a/b[3]/text()")
    assert(ret == "b3")

    ret =
      util.evalString("<a><b>true</b><b>false</b><b>b3</b><c>c1</c><c>c2</c></a>", "a/b[4]/text()")
    assert(ret == "")

    ret = util.evalString(
      "<a><b>true</b><b k=\"foo\">FALSE</b><b>b3</b><c>c1</c><c>c2</c></a>", "a/b[2]/@k")
    assert(ret == "foo")
  }

  test("embedFailure") {
    import org.apache.commons.io.FileUtils
    import java.io.File
    val secretValue = String.valueOf(Math.random)
    val tempFile = File.createTempFile("verifyembed", ".tmp")
    tempFile.deleteOnExit()
    val fname = tempFile.getAbsolutePath

    FileUtils.writeStringToFile(tempFile, secretValue)

    val xml =
      s"""<?xml version="1.0" encoding="utf-8"?>
        |<!DOCTYPE test [
        |    <!ENTITY embed SYSTEM "$fname">
        |]>
        |<foo>&embed;</foo>
      """.stripMargin
    val evaled = new UDFXPathUtil().evalString(xml, "/foo")
    assert(evaled.isEmpty)
  }

  test("number eval") {
    var ret =
      util.evalNumber("<a><b>true</b><b>false</b><b>b3</b><c>c1</c><c>-77</c></a>", "a/c[2]")
    assert(ret == -77.0d)

    ret = util.evalNumber(
      "<a><b>true</b><b k=\"foo\">FALSE</b><b>b3</b><c>c1</c><c>c2</c></a>", "a/b[2]/@k")
    assert(ret.isNaN)
  }

  test("node eval") {
    val ret = util.evalNode("<a><b>true</b><b>false</b><b>b3</b><c>c1</c><c>-77</c></a>", "a/c[2]")
    assert(ret != null && ret.isInstanceOf[Node])
  }

  test("node list eval") {
    val ret = util.evalNodeList("<a><b>true</b><b>false</b><b>b3</b><c>c1</c><c>-77</c></a>", "a/*")
    assert(ret != null && ret.isInstanceOf[NodeList])
    assert(ret.asInstanceOf[NodeList].getLength == 5)
  }
}
