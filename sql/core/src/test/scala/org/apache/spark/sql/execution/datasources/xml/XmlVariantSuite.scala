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

import org.apache.spark.sql.{AnalysisException, DataFrame, QueryTest, Row}
import org.apache.spark.sql.catalyst.xml.{StaxXmlParser, XmlOptions}
import org.apache.spark.sql.functions.{col, variant_get}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

class XmlVariantSuite extends QueryTest with SharedSparkSession with TestXmlData {

  private val baseOptions = Map("rowTag" -> "ROW", "valueTag" -> "_VALUE", "attributePrefix" -> "_")

  private val resDir = "test-data/xml-resources/"

  // ==========================
  // ====== Parser tests ======
  // ==========================

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

    // Long -> Long
    testParser("<ROW><id>2</id></ROW>", """{"id":2}""")
    testParser("<ROW><id>+2</id></ROW>", """{"id":2}""")
    testParser("<ROW><id>-2</id></ROW>", """{"id":-2}""")

    // Decimal -> Decimal
    testParser(
      xml = "<ROW><price>158,058,049.001</price></ROW>",
      expectedJsonStr = """{"price":158058049.001}"""
    )
    testParser(
      xml = "<ROW><decimal>10.05</decimal></ROW>",
      expectedJsonStr = """{"decimal":10.05}"""
    )
    testParser(
      xml = "<ROW><amount>5.0</amount></ROW>",
      expectedJsonStr = """{"amount":5}"""
    )

    // Date -> String
    testParser(
      xml = "<ROW><createdAt>2023-10-01</createdAt></ROW>",
      expectedJsonStr = """{"createdAt":"2023-10-01"}"""
    )

    // Timestamp -> String
    testParser(
      xml = "<ROW><createdAt>2023-10-01T12:00:00Z</createdAt></ROW>",
      expectedJsonStr = """{"createdAt":"2023-10-01T12:00:00Z"}"""
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
    // Scientic numbers -> String
    testParser(
      xml = "<ROW><amount>4.9E-324</amount></ROW>",
      expectedJsonStr = """{"amount":"4.9E-324"}"""
    )
  }

  test("Parser: parse XML attributes as variants") {
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

  test("Parser: Parse whitespaces with quotes") {
    // XML elements with whitespaces
    testParser(
      xml = s"""
               |<ROW>
               |  <a>" "</a>
               |  <b>" "<c>1</c></b>
               |  <d><e attr=" "></e></d>
               |</ROW>
               |""".stripMargin,
      expectedJsonStr = """{"a":"\" \"","b":{"_VALUE":"\" \"","c":1},"d":{"e":{"_attr":" "}}}""",
      extraOptions = Map("ignoreSurroundingSpaces" -> "false")
    )
  }

  test("Parser: Comments are ignored") {
    testParser(
      xml = """
              |<ROW>
              |   <!-- comment -->
              |   <name><!-- before value --> Sam <!-- after value --></name>
              |   <!-- comment -->
              |   <amount>93</amount>
              |</ROW>
              |""".stripMargin,
      expectedJsonStr = """{"amount":93,"name":"Sam"}"""
    )
  }

  test("Parser: CDATA should be handled properly") {
    testParser(
      xml = """
              |<!-- CDATA outside row should be ignored -->
              |<ROW>
              |   <name><![CDATA[Sam]]></name>
              |   <amount>93</amount>
              |</ROW>
              |""".stripMargin,
      expectedJsonStr = """{"amount":93,"name":"Sam"}"""
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
        |   "arrayOfBigInteger":[922337203685477580700,-922337203685477580800],
        |   "arrayOfBoolean":[true,false,true],
        |   "arrayOfDouble":[1.2,1.7976931348623157,"4.9E-324","2.2250738585072014E-308"],
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
        |     "field2":92233720368547758070
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

  test("Parser: Case sensitivity test") {
    val xmlString =
      """
        |<ROW>
        |   <a>1<b>2</b></a>
        |   <A>3<b>4</b></A>
        |</ROW>
        |""".stripMargin
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
      testParser(
        xml = xmlString,
        expectedJsonStr = """{"a":[{"_VALUE":1,"b":2},{"_VALUE":3,"b":4}]}"""
      )
    }
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
      testParser(
        xml = xmlString,
        expectedJsonStr = """{"A":{"_VALUE":3,"b":4},"a":{"_VALUE":1,"b":2}}"""
      )
    }
  }

  test("Parser: XML array elements interspersed between other elements") {
    testParser(
      xml = """
              |<ROW>
              |   <a>1</a>
              |   <b>2</b>
              |   <a>3</a>
              |</ROW>
              |""".stripMargin,
      expectedJsonStr = """{"a":[1,3],"b":2}"""
    )

    testParser(
      xml = """
              |<ROW>
              |   value1
              |   <a>1</a>
              |   value2
              |   <a>2</a>
              |   value3
              |</ROW>
              |""".stripMargin,
      expectedJsonStr = """{"_VALUE":["value1","value2","value3"],"a":[1,2]}"""
    )

    // long and double
    testParser(
      xml = """
              |<ROW>
              |  <a>
              |    1
              |    <b>2</b>
              |    3
              |    <b>4</b>
              |    5.0
              |  </a>
              |</ROW>
              |""".stripMargin,
      expectedJsonStr = """{"a":{"_VALUE":[1,3,5],"b":[2,4]}}"""
    )

    // Comments
    testParser(
      xml = """
              |<ROW>
              |   <!-- comment -->
              |   <a>1</a>
              |   <!-- comment -->
              |   <b>2</b>
              |   <!-- comment -->
              |   <a>3</a>
              |</ROW>
              |""".stripMargin,
      expectedJsonStr = """{"a":[1,3],"b":2}"""
    )
  }

  // =======================
  // ====== DSL tests ======
  // =======================

  private def createDSLDataFrame(
      fileName: String,
      singleVariantColumn: Option[String] = None,
      schemaDDL: Option[String] = None,
      extraOptions: Map[String, String] = Map.empty): DataFrame = {
    assert(
      singleVariantColumn.isDefined || schemaDDL.isDefined,
      "Either singleVariantColumn or schema must be defined to ingest XML files as variants via DSL"
    )
    var reader = spark.read.format("xml").options(baseOptions ++ extraOptions)
    singleVariantColumn.foreach(
      singleVariantColumnName =>
        reader = reader.option("singleVariantColumn", singleVariantColumnName)
    )
    schemaDDL.foreach(s => reader = reader.schema(s))

    reader.load(getTestResourcePath(resDir + fileName))
  }

  test("DSL: read XML files using singleVariantColumn") {
    val df = createDSLDataFrame(fileName = "cars.xml", singleVariantColumn = Some("var"))
    checkAnswer(
      df.select(variant_get(col("var"), "$.year", "int")),
      Seq(Row(2012), Row(1997), Row(2015))
    )
  }

  test("DSL: read XML files with defined schema") {
    val df = createDSLDataFrame(
      fileName = "cars.xml",
      schemaDDL = Some("year variant, make string, model string, comment string")
    )
    checkAnswer(
      df.select(variant_get(col("year"), "$", "int")),
      Seq(Row(2012), Row(1997), Row(2015))
    )
  }

  // TODO: This should be allowed once we support variant ingestion with malformed record handling
  test("DSL: read XML files using both singleVariantColumn and schema should fail") {
    checkError(
      exception = intercept[AnalysisException] {
        createDSLDataFrame(
          fileName = "cars.xml",
          singleVariantColumn = Some("var"),
          schemaDDL = Some("year variant, make string, model string, comment string")
        )
      },
      condition = "INVALID_SINGLE_VARIANT_COLUMN",
      parameters = Map.empty
    )
  }

  test("DSL: test XSD validation") {
    val df = createDSLDataFrame(
      fileName = "basket_invalid.xml",
      singleVariantColumn = Some("var"),
      extraOptions = Map(
        "rowTag" -> "basket",
        "rowValidationXSDPath" -> getTestResourcePath(resDir + "basket.xsd").replace("file:/", "/")
      )
    )
    checkAnswer(
      df.select(variant_get(col("var"), "$", "string")),
      Seq(
        // The first row matches the XSD and thus is parsed as Variant successfully
        Row("""{"entry":[{"key":1,"value":"fork"},{"key":2,"value":"cup"}]}"""),
        // The second row fails the XSD validation and is not parsed
        Row(null)
      )
    )
  }

  // =======================
  // ====== SQL tests ======
  // =======================

  test("SQL: read an entire XML record as variant using from_xml SQL expression") {
    val xmlStr =
      """
        |<ROW>
        |    <year>2012<!--A comment within tags--></year>
        |    <make>Tesla</make>
        |    <model>S</model>
        |    <comment>No comment</comment>
        |</ROW>
        |""".stripMargin

    // Read the entire XML record as a single variant
    // Verify we can extract fields from the variant type
    checkAnswer(
      spark
        .sql(s"""SELECT from_xml('$xmlStr', 'variant') as var""")
        .select(variant_get(col("var"), "$.year", "int")),
      Seq(Row(2012))
    )
  }

  test("SQL: read partial XML record as variant using from_xml with a defined schema") {
    val xmlStr =
      """
        |<ROW>
        |    <year>2012<!--A comment within tags--></year>
        |    <make>Tesla</make>
        |    <model>S</model>
        |    <comment>No comment</comment>
        |</ROW>
        |""".stripMargin
    // Read specific elements in the XML record as variant
    val schemaDDL = "year variant, make string, model string, comment string"
    // Verify we can extract fields from the variant type
    checkAnswer(
      spark
        .sql(s"""SELECT from_xml('$xmlStr', '$schemaDDL') as car""".stripMargin)
        .select(variant_get(col("car.year"), "$", "int")),
      Seq(Row(2012))
    )
  }
}
