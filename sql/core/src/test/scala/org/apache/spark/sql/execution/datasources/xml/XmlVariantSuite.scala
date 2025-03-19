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
package org.apache.spark.sql.execution.datasources.xml

import java.time.ZoneOffset

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.xml.{StaxXmlParser, XmlOptions}
import org.apache.spark.sql.test.SharedSparkSession

class XmlVariantSuite
    extends QueryTest
    with SharedSparkSession
    with TestXmlData {

  private val baseOptions = Map("rowTag" -> "ROW", "valueTag" -> "_VALUE", "attributePrefix" -> "_")

  private def testParser(
      xml: String,
      expectedJsonStr: String,
      extraOptions: Map[String, String] = Map.empty): Unit = {
    val parsed = StaxXmlParser.parseVariant(xml, XmlOptions(baseOptions ++ extraOptions))
    assert(parsed.toJson(ZoneOffset.UTC) == expectedJsonStr)
  }

  test("Parser: parse primitive XML elements (long, decimal, double, etc.) as variants") {
    // Boolean -> Boolean
    testParser("<ROW><isActive>false</isActive></ROW>", """{"isActive":false}""")
    testParser("<ROW><isActive>true</isActive></ROW>", """{"isActive":true}""")

    // Integer -> Long
    testParser("<ROW><id>2</id></ROW>", """{"id":2}""")
    testParser("<ROW><id>+2</id></ROW>", """{"id":2}""")
    testParser("<ROW><id>-2</id></ROW>", """{"id":-2}""")

    // Decimal -> Decimal
    testParser(
      xml = "<ROW><price>158,058,049.001</price></ROW>",
      expectedJsonStr = """{"price":158058049.001}""",
      extraOptions = Map("prefersDecimal" -> "true")
    )
    testParser(
      xml = "<ROW><decimal>10.05</decimal></ROW>",
      expectedJsonStr = """{"decimal":10.05}""",
      extraOptions = Map("prefersDecimal" -> "true")
    )

    // Double -> Double
    testParser("<ROW><double>1.00</double></ROW>", """{"double":1.0}""")
    testParser("<ROW><double>+1.00</double></ROW>", """{"double":1.0}""")
    testParser("<ROW><double>-1.00</double></ROW>", """{"double":-1.0}""")

    // Date -> String
    testParser(
      xml = "<ROW><createdAt>2023-10-01</createdAt></ROW>",
      expectedJsonStr = """{"createdAt":"2023-10-01"}""",
      extraOptions = Map("dateFormat" -> "yyyy-MM-dd")
    )

    // Timestamp -> String
    testParser(
      xml = "<ROW><createdAt>2023-10-01T12:00:00Z</createdAt></ROW>",
      expectedJsonStr = """{"createdAt":"2023-10-01T12:00:00Z"}""",
      extraOptions = Map("timestampFormat" -> "yyyy-MM-dd'T'HH:mm:ssX")
    )

    // String -> String
    testParser("<ROW><name>Sam</name></ROW>", """{"name":"Sam"}""")
    // Strings with spaces
    testParser(
      "<ROW><note>  hello world  </note></ROW>",
      expectedJsonStr = """{"note":"  hello world  "}""",
      extraOptions = Map("ignoreSurroundingSpaces" -> "false")
    )
    testParser(
      xml = "<ROW><note>  hello world  </note></ROW>",
      expectedJsonStr = """{"note":"hello world"}"""
    )
  }

  test("Parser: parse XML elements as variant object") {
    testParser(
      xml = "<ROW><info><name>Sam</name><amount>93</amount></info></ROW>",
      expectedJsonStr = """{"info":{"amount":93,"name":"Sam"}}"""
    )
  }

  test("Parser: parse XML elements as variant array") {
    testParser(
      xml = "<ROW><array>1</array><array>2</array></ROW>",
      expectedJsonStr = """{"array":[1,2]}"""
    )
  }

  test("Parser: parser XML attributes as variants") {
    // XML elements with only attributes
    testParser(
      xml = "<ROW id=\"2\" name=\"Sam\" amount=\"93\"></ROW>",
      expectedJsonStr = """{"_amount":93,"_id":2,"_name":"Sam"}"""
    )

    // XML elements with attributes and elements
    testParser(
      xml = "<ROW id=\"2\" name=\"Sam\"><amount>93</amount></ROW>",
      expectedJsonStr = """{"_id":2,"_name":"Sam","amount":93}"""
    )

    // XML elements with attributes and nested elements
    testParser(
      xml = "<ROW id=\"2\" name=\"Sam\"><info><amount>93</amount></info></ROW>",
      expectedJsonStr = """{"_id":2,"_name":"Sam","info":{"amount":93}}"""
    )

    // XML elements with attributes and value tag
    testParser(
      xml = "<ROW id=\"2\" name=\"Sam\">93</ROW>",
      expectedJsonStr = """{"_VALUE":93,"_id":2,"_name":"Sam"}"""
    )
  }

  test("Parser: parse XML value tags as variants") {
    // XML elements with value tags and attributes
    testParser(
      xml = "<ROW id=\"2\" name=\"Sam\">93</ROW>",
      expectedJsonStr = """{"_VALUE":93,"_id":2,"_name":"Sam"}"""
    )

    // XML elements with value tags and nested elements
    testParser(
      xml = "<ROW><info>Sam<amount>93</amount></info></ROW>",
      expectedJsonStr = """{"info":{"_VALUE":"Sam","amount":93}}"""
    )
  }

  test("Parser: null and empty XML elements are parsed as variant null") {
    // XML elements with null and empty values
    testParser(
      xml = "<ROW><name></name><amount>93</amount></ROW>",
      expectedJsonStr = """{"amount":93,"name":null}"""
    )
    testParser(
      xml = "<ROW><name>Sam</name><amount>n/a</amount></ROW>",
      expectedJsonStr = """{"amount":null,"name":"Sam"}""",
      extraOptions = Map("nullValue" -> "n/a")
    )
  }

  test("Parser: parse mixed types as variants") {
    val expectedJsonStr =
      """
        |{
        |   "arrayOfArray1":[
        |     {"item":[1,2,3]},
        |     {"item":["str1","str2"]}
        |   ],
        |   "arrayOfArray2":[
        |     {"item":[1,2,3]},
        |     {"item":[1.1,2.1,3.1]}
        |   ],
        |   "arrayOfBigInteger":[9.223372036854776E20,-9.223372036854776E20],
        |   "arrayOfBoolean":[true,false,true],
        |   "arrayOfDouble":[1.2,1.7976931348623157,4.9E-324,2.2250738585072014E-308],
        |   "arrayOfInteger":[1,2147483647,-2147483648],
        |   "arrayOfLong":[21474836470,9223372036854775807,-9223372036854775808],
        |   "arrayOfNull":[null,null],
        |   "arrayOfString":["str1","str2"],
        |   "arrayOfStruct":[
        |     {"field1":true,"field2":"str1"},
        |     {"field1":false},
        |     {"field3":null}
        |   ],
        |   "struct":{
        |     "field1":true,
        |     "field2":9.223372036854776E19
        |   },
        |   "structWithArrayFields":{
        |     "field1":[4,5,6],
        |     "field2":["str1","str2"]
        |   }
        |}
        |""".stripMargin.replaceAll("\\s+", "")
    testParser(
      xml = complexFieldAndType1.head,
      expectedJsonStr = expectedJsonStr
    )

    val expectedJsonStr2 =
      """
        |{
        |   "arrayOfArray1":[
        |     {"array":{"item":5}},
        |     {
        |       "array":[
        |         {"item":[6,7]},
        |         {"item":8}
        |       ]
        |     }
        |   ],
        |   "arrayOfArray2":[
        |     {"array":{"item":{"inner1":"str1"}}},
        |     {
        |       "array":[
        |         null,
        |         {
        |           "item":[
        |             {"inner2":["str3","str33"]},
        |             {"inner1":"str11","inner2":"str4"}
        |           ]
        |         }
        |       ]
        |     },
        |     {
        |       "array":{
        |         "item":{
        |           "inner3":[
        |             {"inner4":[2,3]},
        |             null
        |           ]
        |         }
        |       }
        |     }
        |   ]
        |}
        """.stripMargin.replaceAll("\\s+", "")
    testParser(
      xml = complexFieldAndType2.head,
      expectedJsonStr = expectedJsonStr2
    )
  }
}
