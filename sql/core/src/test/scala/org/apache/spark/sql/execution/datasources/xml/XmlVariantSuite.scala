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

import java.io.CharArrayWriter
import java.time.ZoneOffset

import org.apache.spark.SparkException
import org.apache.spark.sql.{AnalysisException, DataFrame, QueryTest, Row}
import org.apache.spark.sql.catalyst.xml.{StaxXmlGenerator, StaxXmlParser, XmlOptions}
import org.apache.spark.sql.functions.{col, variant_get}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.VariantType
import org.apache.spark.types.variant.{Variant, VariantBuilder}
import org.apache.spark.unsafe.types.VariantVal

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
    // This is parsed as String, because it is too large for Decimal
    testParser(
      xml = "<ROW><amount>1e40</amount></ROW>",
      expectedJsonStr = """{"amount":"1e40"}"""
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
  }

  test("Parser: parse XML attributes as variants") {
    // XML elements with only attributes
    testParser(
      xml = "<ROW id=\"2\"></ROW>",
      expectedJsonStr = """{"_id":2}"""
    )
    testParser(
      xml = "<ROW><a><b attr=\"1\"></b></a></ROW>",
      expectedJsonStr = """{"a":{"b":{"_attr":1}}}"""
    )
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
      xml = """<ROW><name></name><amount>93</amount><space> </space><newline>
                      </newline></ROW>""",
      expectedJsonStr = """{"amount":93,"name":null,"newline":null,"space":null}"""
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
              |   <!-- <a>1</a> -->
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

  // ==============================
  // ====== DSL reader tests ======
  // ==============================

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
      fileName = "books-complicated.xml",
      schemaDDL = Some(
        "_id variant, " + // Attribute as variant
        "author string, " +
        "title string, " +
        "genre struct<genreid int, name variant>, " + // Struct with variant
        "price variant, " + // Scalar as variant
        "publish_dates struct<publish_date array<variant>>" // Array with variant
      ),
      extraOptions = Map("rowTag" -> "book")
    )
    checkAnswer(
      df.select(
        variant_get(col("_id"), "$", "string"),
        variant_get(col("genre.name"), "$", "string"),
        variant_get(col("price"), "$", "double"),
        variant_get(col("publish_dates.publish_date").getItem(0), "$.month", "int")
      ),
      Seq(
        Row("bk101", "Computer", 44.95, 10),
        Row("bk102", "Fantasy", 5.95, 12),
        Row("bk103", "Fantasy", null, 11)
      )
    )
  }

  test("DSL: provided schema in singleVariantColumn mode") {
    // Specified schema in singleVariantColumn mode can't contain columns other than the variant
    // column and the corrupted record column
    checkError(
      exception = intercept[AnalysisException] {
        createDSLDataFrame(
          fileName = "cars.xml",
          singleVariantColumn = Some("var"),
          schemaDDL = Some("year variant, make string, model string, comment string")
        )
      },
      condition = "INVALID_SINGLE_VARIANT_COLUMN",
      parameters = Map(
        "schema" -> """"STRUCT<year: VARIANT, make: STRING, model: STRING, comment: STRING>""""
      )
    )
    checkError(
      exception = intercept[AnalysisException] {
        createDSLDataFrame(
          fileName = "cars.xml",
          singleVariantColumn = Some("var"),
          schemaDDL = Some("_corrupt_record string")
        )
      },
      condition = "INVALID_SINGLE_VARIANT_COLUMN",
      parameters = Map(
        "schema" -> """"STRUCT<_corrupt_record: STRING>""""
      )
    )

    // Valid schema in singleVariantColumn mode
    createDSLDataFrame(
      fileName = "cars.xml",
      singleVariantColumn = Some("var"),
      schemaDDL = Some("var variant")
    )
    createDSLDataFrame(
      fileName = "cars.xml",
      singleVariantColumn = Some("var"),
      schemaDDL = Some("var variant, _corrupt_record string")
    )
  }

  test("DSL: handle malformed record in singleVariantColumn mode") {
    // FAILFAST mode
    checkError(
      exception = intercept[SparkException] {
        createDSLDataFrame(
          fileName = "cars-malformed.xml",
          singleVariantColumn = Some("var"),
          extraOptions = Map("mode" -> "FAILFAST")
        ).collect()
      }.getCause.asInstanceOf[SparkException],
      condition = "MALFORMED_RECORD_IN_PARSING.WITHOUT_SUGGESTION",
      parameters = Map("badRecord" -> "[null]", "failFastMode" -> "FAILFAST")
    )

    // PERMISSIVE mode
    val df = createDSLDataFrame(
      fileName = "cars-malformed.xml",
      singleVariantColumn = Some("var"),
      extraOptions = Map("mode" -> "PERMISSIVE")
    )
    checkAnswer(
      df.select(variant_get(col("var"), "$.year", "int")),
      Seq(Row(2015), Row(null), Row(null))
    )

    // DROPMALFORMED mode
    val df2 = createDSLDataFrame(
      fileName = "cars-malformed.xml",
      singleVariantColumn = Some("var"),
      extraOptions = Map("mode" -> "DROPMALFORMED")
    )
    checkAnswer(
      df2.select(variant_get(col("var"), "$.year", "int")),
      Seq(Row(2015))
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

  // ============================
  // ====== from_xml tests ======
  // ============================

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
        |   <scalar>Hello</scalar>
        |   <arrayField><a>1</a></arrayField>
        |   <arrayField><a>2</a></arrayField>
        |   <structField>
        |     <a><b>3</b></a>
        |     <c><d>4</d></c>
        |   </structField>
        |   <nestedVariantField>
        |     <a>5</a>
        |     <b>
        |        <c>6</c>
        |     </b>
        |   </nestedVariantField>
        |   <mapField>
        |     <a><b>7</b></a>
        |     <c>8</c>
        |   </mapField>
        | </ROW>
        | """.stripMargin.replaceAll("\\s+", "")
    // Read specific elements in the XML record as variant
    val schemaDDL =
      "scalar variant, " +
      "arrayField array<variant>, " + // Array with variants
      "structField struct<a variant, c struct<d variant>>, " + // Struct with variants
      "nestedVariantField variant, " +
      "mapField map<string, variant>"
    // Verify we can extract fields from the variant type
    checkAnswer(
      spark
        .sql(s"""SELECT from_xml('$xmlStr', '$schemaDDL') as row""".stripMargin)
        .select(
          variant_get(col("row.scalar"), "$", "string"),
          variant_get(col("row.arrayField").getItem(0), "$.a", "int"),
          variant_get(col("row.arrayField").getItem(1), "$.a", "int"),
          variant_get(col("row.structField.a"), "$.b", "int"),
          variant_get(col("row.structField.c.d"), "$", "int"),
          variant_get(col("row.nestedVariantField"), "$.a", "int"),
          variant_get(col("row.nestedVariantField"), "$.b.c", "int"),
          variant_get(col("row.mapField.a"), "$.b", "int"),
          variant_get(col("row.mapField.c"), "$", "int")
        ),
      Seq(Row("Hello", 1, 2, 3, 4, 5, 6, 7, 8))
    )
  }

  // =============================
  // ====== Generator tests ======
  // =============================

  private val writer = new CharArrayWriter()

  private def testGenerator(
      v: Variant,
      expectedXml: String,
      extraOptions: Map[String, String]): Unit = {
    testGenerator(new VariantVal(v.getValue, v.getMetadata), expectedXml, extraOptions)
  }

  private def testGenerator(
      v: VariantVal,
      expectedXml: String,
      extraOptions: Map[String, String] = Map.empty): Unit = {
    val gen = new StaxXmlGenerator(
      schema = VariantType,
      writer = writer,
      options = new XmlOptions(baseOptions ++ extraOptions),
      validateStructure = false
    )
    gen.write(v)
    gen.flush()
    val xmlString = writer.toString
    writer.reset()
    assert(xmlString == expectedXml)
  }

  test("Generator: serialize Variant primitive fields to XML") {
    def testPrimitive(
        primitiveValue: Any,
        expectedXml: String,
        extraOptions: Map[String, String] = Map.empty): Unit = {
      val builder = new VariantBuilder(false)
      primitiveValue match {
        case null => builder.appendNull()
        case v: String => builder.appendString(v)
        case v: Int => builder.appendLong(v)
        case v: Long => builder.appendLong(v)
        case v: Boolean => builder.appendBoolean(v)
        case v: Double => builder.appendDouble(v)
        case v: java.math.BigDecimal => builder.appendDecimal(v)
        case v: Float => builder.appendFloat(v)
        case v: java.sql.Date => builder.appendDate(v.toLocalDate.toEpochDay.toInt)
        case v: java.sql.Timestamp =>
          builder.appendTimestamp(v.getTime * 1000L)
        case v: java.util.UUID => builder.appendUuid(v)
      }
      testGenerator(builder.result(), expectedXml, extraOptions)
    }

    // NULL
    testPrimitive(null, "<ROW/>")
    testPrimitive(null, "<ROW>null</ROW>", extraOptions = Map("nullValue" -> "null"))

    // Long
    testPrimitive(1, "<ROW>1</ROW>")
    testPrimitive(9223372036854775807L, "<ROW>9223372036854775807</ROW>") // Max long
    testPrimitive(-9223372036854775808L, "<ROW>-9223372036854775808</ROW>") // Min long

    // Boolean
    testPrimitive(true, "<ROW>true</ROW>")
    testPrimitive(false, "<ROW>false</ROW>")

    // Double
    testPrimitive(1.0, "<ROW>1.0</ROW>")
    testPrimitive(Double.MaxValue, "<ROW>1.7976931348623157E308</ROW>")
    testPrimitive(Double.MinValue, "<ROW>-1.7976931348623157E308</ROW>")
    testPrimitive(Double.MinPositiveValue, "<ROW>4.9E-324</ROW>")

    // Decimal
    testPrimitive(new java.math.BigDecimal("1.0"), "<ROW>1</ROW>")
    testPrimitive(new java.math.BigDecimal("1E-10"), "<ROW>1E-10</ROW>")
    testPrimitive(new java.math.BigDecimal("123456789.987654321"), "<ROW>123456789.987654321</ROW>")

    // Float
    testPrimitive(1.0f, "<ROW>1.0</ROW>")
    testPrimitive(Float.MaxValue, "<ROW>3.4028235E38</ROW>")
    testPrimitive(Float.MinValue, "<ROW>-3.4028235E38</ROW>")

    // Date
    testPrimitive(
      java.sql.Date.valueOf("2023-10-01"),
      "<ROW>2023-10-01</ROW>"
    )
    testPrimitive(
      java.sql.Date.valueOf("2023-10-01"),
      "<ROW>10/01/2023</ROW>",
      extraOptions = Map("dateFormat" -> "MM/dd/yyyy")
    )

    // Timestamp
    testPrimitive(
      java.sql.Timestamp.valueOf("2023-10-01 12:00:00"),
      "<ROW>2023-10-01T12:00:00-07</ROW>",
      extraOptions = Map("timestampFormat" -> "yyyy-MM-dd'T'HH:mm:ssX")
    )
    testPrimitive(
      java.sql.Timestamp.valueOf("1970-01-01 00:00:00"),
      "<ROW>1970-01-01T00:00:00Z</ROW>",
      extraOptions = Map("timestampFormat" -> "yyyy-MM-dd'T'HH:mm:ss'Z'")
    )
    testPrimitive(
      java.sql.Timestamp.valueOf("1970-01-01 12:30:45"),
      "<ROW>01/01/1970 12:30:45 PM</ROW>",
      extraOptions = Map("timestampFormat" -> "MM/dd/yyyy hh:mm:ss a")
    )

    // UUID
    val uuid = java.util.UUID.randomUUID()
    testPrimitive(uuid, s"<ROW>${uuid.toString}</ROW>")

    // String
    testPrimitive("Hello World", "<ROW>Hello World</ROW>")
  }

  test("Generator: serialize Variant array fields to XML") {
    Seq(
      """<ROW>
        |    <array>1</array>
        |    <array>2</array>
        |    <array>3</array>
        |</ROW>""".stripMargin,
      """<ROW>
        |    <array>2</array>
        |    <array>1</array>
        |    <array>3</array>
        |</ROW>""".stripMargin,
      """<ROW>
        |    <array>3</array>
        |    <array>2</array>
        |    <array>1</array>
        |</ROW>""".stripMargin
    ).foreach { xmlString =>
      testGenerator(
        StaxXmlParser.parseVariant(xmlString, XmlOptions(baseOptions)),
        expectedXml = xmlString,
        extraOptions = Map.empty
      )
    }
  }

  test("Generator: serialize Variant object to XML") {
    val xmlString =
      """<ROW>
        |    <struct>
        |        <field1>1</field1>
        |        <field2>2</field2>
        |    </struct>
        |</ROW>""".stripMargin
    testGenerator(
      StaxXmlParser.parseVariant(xmlString, XmlOptions(baseOptions)),
      expectedXml = xmlString,
      extraOptions = Map.empty
    )
  }

  test("Generator: serialize Variant object with attribute fields to XML") {
    val xmlString =
      """<ROW>
        |    <struct attr1="1" attr2="2">
        |        <field1>3</field1>
        |        <field2>4</field2>
        |    </struct>
        |</ROW>""".stripMargin
    testGenerator(
      StaxXmlParser.parseVariant(xmlString, XmlOptions(baseOptions)),
      expectedXml = xmlString,
      extraOptions = Map.empty
    )
  }

  test("Generator: serialize Variant object with value tag to XML") {
    val xmlString =
      """<ROW>
        |    <struct>value
        |        <field1>2</field1>
        |        <field2>3</field2>
        |    </struct>
        |</ROW>""".stripMargin
    testGenerator(
      StaxXmlParser.parseVariant(xmlString, XmlOptions(baseOptions)),
      expectedXml = xmlString,
      extraOptions = Map.empty
    )
  }

  // =============================
  // ====== DSL write tests ======
  // =============================

  test("DSL: save singleVariantColumn to XML") {
    // Load the XML file as a single variant column
    val df = spark.read
      .format("xml")
      .option("singleVariantColumn", "var")
      .options(baseOptions)
      .load(getTestResourcePath(resDir + "cars.xml"))

    withTempDir { dir =>
      // Write the single Variant column Dataframe to XML
      val outputPath = dir.getCanonicalPath
      df.write
        .format("xml")
        .option("rowTag", "ROW")
        .option("singleVariantColumn", "var")
        .mode("overwrite")
        .save(outputPath)

      // Check if the written XML file matches the original XML file
      val df1 = spark.read
        .format("xml")
        .options(baseOptions)
        .load(getTestResourcePath(resDir + "cars.xml"))
      val df2 = spark.read
        .format("xml")
        .options(baseOptions)
        .load(outputPath)
      checkAnswer(df1, df2)
    }
  }

  test("DSL: save Dataframe with child variant columns to XML") {
    // Load the XML file as a struct with two variant columns
    val df = spark.read
      .format("xml")
      .option("rowTag", "book")
      .schema(
        "_id string, author string, title string, genre variant, price double, " +
        "publish_dates variant"
      )
      .load(getTestResourcePath(resDir + "books-complicated.xml"))

    withTempDir { dir =>
      // Write the Dataframe with the two Variant columns to XML
      val outputPath = dir.getCanonicalPath
      df.write
        .format("xml")
        .option("rowTag", "book")
        .mode("overwrite")
        .save(outputPath)

      // Check if the written XML file matches the original XML file
      val df1 = spark.read
        .format("xml")
        .option("rowTag", "book")
        .load(getTestResourcePath(resDir + "books-complicated.xml"))
      val df2 = spark.read
        .format("xml")
        .option("rowTag", "book")
        .load(outputPath)
      checkAnswer(df1, df2)
    }
  }

  // =============================
  // ====== to_xml tests =========
  // =============================

  test("SQL: to_xml with a single variant column") {
    val xmlStr =
      """
        |<ROW>
        |    <year>2012<!--A comment within tags--></year>
        |    <make>Tesla</make>
        |    <model>S</model>
        |    <comment>NoComment</comment>
        |</ROW>
        |""".stripMargin

    // Read the entire XML record as a single variant
    val xmlResult = spark
      .sql(s"""SELECT to_xml(from_xml('$xmlStr', 'variant'))""")
      .collect()
      .map(_.getString(0).replaceAll("\\s+", ""))
    val expectedResult =
      """<ROW>
        |    <comment>NoComment</comment>
        |    <make>Tesla</make>
        |    <model>S</model>
        |    <year>2012</year>
        |</ROW>""".stripMargin.replaceAll("\\s+", "")
    assert(xmlResult.head === expectedResult)
  }

  test("SQL: to_xml with a subset of variant columns") {
    val xmlStr =
      """
        |<book>
        |   <author>Gambardella</author>
        |   <title>Hello</title>
        |   <genre>
        |     <genreid>1</genreid>
        |     <name>Computer</name>
        |   </genre>
        |   <price>44.95</price>
        |   <publish_dates>
        |     <publish_date>
        |       <day>1</day>
        |       <month>10</month>
        |       <year>2000</year>
        |     </publish_date>
        |   </publish_dates>
        | </book>
        | """.stripMargin.replaceAll("\\s+", "")

    // Read the entire XML record as a single variant
    val schemaDDL =
      "author string, title string, genre variant, price double, publish_dates variant"
    val xmlResult = spark
      .sql(s"""SELECT to_xml(from_xml('$xmlStr', '$schemaDDL'), map('rowTag', 'book'))""")
      .collect()
      .map(_.getString(0).replaceAll("\\s+", ""))
    assert(xmlResult.head === xmlStr)
  }
}
