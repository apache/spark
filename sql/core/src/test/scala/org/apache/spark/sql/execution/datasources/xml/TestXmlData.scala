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

import java.io.{File, RandomAccessFile}

import org.apache.spark.sql.{Dataset, Encoders, SparkSession}

private[xml] trait TestXmlData {
  protected def spark: SparkSession

  def sampledTestData: Dataset[String] = {
    spark
      .range(0, 100, 1)
      .map { index =>
        val predefinedSample = Set[Long](3, 18, 20, 24, 50, 60, 87, 99)
        if (predefinedSample.contains(index)) {
          index.toString
        } else {
          (index.toDouble + 0.1).toString
        }
      }(Encoders.STRING)
  }

  def withCorruptedFile(dir: File, format: String = "gz", numBytesToCorrupt: Int = 50)(
      f: File => Unit): Unit = {
    // find the targeted files and corrupt the first one
    val files = dir.listFiles().filter(file => file.isFile && file.getName.endsWith(format))
    val raf = new RandomAccessFile(files.head.getPath, "rw")

    // disable checksum verification
    import org.apache.hadoop.fs.Path
    val fs = new Path(dir.getPath).getFileSystem(spark.sessionState.newHadoopConf())
    fs.setVerifyChecksum(false)
    // delete crc files
    val crcFiles = dir.listFiles
      .filter(file => file.isFile && file.getName.endsWith("crc"))
    crcFiles.foreach { file =>
      assert(file.exists())
      file.delete()
      assert(!file.exists())
    }

    // corrupt the file
    val fileSize = raf.length()
    // avoid the last few bytes as it might contain crc
    raf.seek(fileSize - numBytesToCorrupt - 100)
    for (_ <- 1 to numBytesToCorrupt) {
      val randomByte = (Math.random() * 256).toByte
      raf.writeByte(randomByte)
    }
    raf.close()
    f(dir)
    fs.setVerifyChecksum(true)
  }

  def primitiveFieldValueTypeConflict: Seq[String] =
    """<ROW>
          |  <num_num_1>11</num_num_1>
          |  <num_num_2/>
          |  <num_num_3>1.1</num_num_3>
          |  <num_bool>true</num_bool>
          |  <num_str>13.1</num_str>
          |  <str_bool>str1</str_bool>
          |</ROW>
          |""".stripMargin ::
    """
           |<ROW>
           |  <num_num_1/>
           |  <num_num_2>21474836470.9</num_num_2>
           |  <num_num_3/>
           |  <num_bool>12</num_bool>
           |  <num_str/>
           |  <str_bool>true</str_bool>
           |</ROW>""".stripMargin ::
    """
            |<ROW>
            |  <num_num_1>21474836470</num_num_1>
            |  <num_num_2>92233720368547758070</num_num_2>
            |  <num_num_3>100</num_num_3>
            |  <num_bool>false</num_bool>
            |  <num_str>str1</num_str>
            |  <str_bool>false</str_bool>
            |</ROW>""".stripMargin ::
    """
            |<ROW>
            |  <num_num_1>21474836570</num_num_1>
            |  <num_num_2>1.1</num_num_2>
            |  <num_num_3>21474836470</num_num_3>
            |  <num_bool/>
            |  <num_str>92233720368547758070</num_str>
            |  <str_bool/>
            |</ROW>""".stripMargin :: Nil

  def xmlNullStruct: Seq[String] =
    """<ROW>
          |  <nullstr></nullstr>
          |  <ip>27.31.100.29</ip>
          |  <headers>
          |    <Host>1.abc.com</Host>
          |    <Charset>UTF-8</Charset>
          |  </headers>
          |</ROW>""".stripMargin ::
    """<ROW>
          |  <nullstr></nullstr>
          |  <ip>27.31.100.29</ip>
          |  <headers/>
          |</ROW>""".stripMargin ::
    """<ROW>
          |  <nullstr></nullstr>
          |  <ip>27.31.100.29</ip>
          |  <headers></headers>
          |</ROW>""".stripMargin ::
    """<ROW>
          |  <nullstr/>
          |  <ip>27.31.100.29</ip>
          |  <headers/>
          |</ROW>""".stripMargin :: Nil

  def complexFieldValueTypeConflict: Seq[String] =
    """<ROW>
      <num_struct>11</num_struct>
      <str_array>1</str_array>
      <str_array>2</str_array>
      <str_array>3</str_array>
      <array></array>
      <struct_array></struct_array>
      <struct></struct>
    </ROW>""" ::
    """<ROW>
      <num_struct>
        <field>false</field>
      </num_struct>
      <str_array/>
      <array/>
      <struct_array></struct_array>
      <struct/>
    </ROW>""" ::
    """<ROW>
      <num_struct/>
      <str_array>str</str_array>
      <array>4</array>
      <array>5</array>
      <array>6</array>
      <struct_array>7</struct_array>
      <struct_array>8</struct_array>
      <struct_array>9</struct_array>
      <struct>
        <field/>
      </struct>
    </ROW>""" ::
    """<ROW>
      <num_struct></num_struct>
      <str_array>str1</str_array>
      <str_array>str2</str_array>
      <str_array>33</str_array>
      <array>7</array>
      <struct_array>
        <field>true</field>
      </struct_array>
      <struct>
        <field>str</field>
      </struct>
    </ROW>""" :: Nil

  def arrayElementTypeConflict: Seq[String] =
    """
          |<ROW>
          |   <array1>
          |      <element>1</element>
          |      <element>1.1</element>
          |      <element>true</element>
          |      <element/>
          |      <element>
          |         <array/>
          |      </element>
          |      <element>
          |         <object/>
          |      </element>
          |   </array1>
          |   <array1>
          |      <element>
          |         <array>
          |            <element>2</element>
          |            <element>3</element>
          |            <element>4</element>
          |         </array>
          |      </element>
          |      <element>
          |         <object>
          |            <field>str</field>
          |         </object>
          |      </element>
          |   </array1>
          |   <array2>
          |      <field>214748364700</field>
          |   </array2>
          |   <array2>
          |      <field>1</field>
          |   </array2>
          |</ROW>
          |""".stripMargin ::
    """
          |<ROW>
          |  <array3>
          |    <field>str</field>
          |  </array3>
          |  <array3>
          |    <field>1</field>
          |  </array3>
          |</ROW>
          |""".stripMargin ::
    """
          |<ROW>
          |  <array3>1</array3>
          |  <array3>2</array3>
          |  <array3>3</array3>
          |</ROW>
          |""".stripMargin :: Nil

  def missingFields: Seq[String] =
    """
        <ROW><a>true</a></ROW>
        """ ::
    """
        <ROW><b>21474836470</b></ROW>
        """ ::
    """
        <ROW><c>33</c><c>44</c></ROW>
        """ ::
    """
        <ROW><d><field>true</field></d></ROW>
        """ ::
    """
        <ROW><e>str</e></ROW>
        """ :: Nil

  // XML doesn't support array of arrays
  // It only supports array of structs
  def complexFieldAndType1: Seq[String] =
    """
          |<ROW>
          |  <struct>
          |    <field1>true</field1>
          |    <field2>92233720368547758070</field2>
          |  </struct>
          |  <structWithArrayFields>
          |    <field1>4</field1>
          |    <field1>5</field1>
          |    <field1>6</field1>
          |    <field2>str1</field2>
          |    <field2>str2</field2>
          |  </structWithArrayFields>
          |  <arrayOfString>str1</arrayOfString>
          |  <arrayOfString>str2</arrayOfString>
          |  <arrayOfInteger>1</arrayOfInteger>
          |  <arrayOfInteger>2147483647</arrayOfInteger>
          |  <arrayOfInteger>-2147483648</arrayOfInteger>
          |  <arrayOfLong>21474836470</arrayOfLong>
          |  <arrayOfLong>9223372036854775807</arrayOfLong>
          |  <arrayOfLong>-9223372036854775808</arrayOfLong>
          |  <arrayOfBigInteger>922337203685477580700</arrayOfBigInteger>
          |  <arrayOfBigInteger>-922337203685477580800</arrayOfBigInteger>
          |  <arrayOfDouble>1.2</arrayOfDouble>
          |  <arrayOfDouble>1.7976931348623157</arrayOfDouble>
          |  <arrayOfDouble>4.9E-324</arrayOfDouble>
          |  <arrayOfDouble>2.2250738585072014E-308</arrayOfDouble>
          |  <arrayOfBoolean>true</arrayOfBoolean>
          |  <arrayOfBoolean>false</arrayOfBoolean>
          |  <arrayOfBoolean>true</arrayOfBoolean>
          |  <arrayOfNull></arrayOfNull>
          |  <arrayOfNull></arrayOfNull>
          |  <arrayOfStruct>
          |    <field1>true</field1>
          |    <field2>str1</field2>
          |  </arrayOfStruct>
          |  <arrayOfStruct>
          |    <field1>false</field1>
          |  </arrayOfStruct>
          |  <arrayOfStruct>
          |    <field3/>
          |  </arrayOfStruct>
          |<arrayOfArray1>
          |  <item>1</item><item>2</item><item>3</item>
          |</arrayOfArray1>
          |<arrayOfArray1>
          |  <item>str1</item><item>str2</item>
          |</arrayOfArray1>
          |<arrayOfArray2>
          |  <item>1</item><item>2</item><item>3</item>
          |</arrayOfArray2>
          |<arrayOfArray2>
          |  <item>1.1</item><item>2.1</item><item>3.1</item>
          |</arrayOfArray2>
          |</ROW>
          |
          |""".stripMargin :: Nil

  def complexFieldAndType2: Seq[String] =
    """
          |<ROW>
          |  <arrayOfArray1>
          |  <array>
          |    <item>5</item>
          |  </array>
          |</arrayOfArray1>
          |<arrayOfArray1>
          |  <array>
          |    <item>6</item><item>7</item>
          |  </array>
          |  <array>
          |    <item>8</item>
          |  </array>
          |</arrayOfArray1>
          |  <arrayOfArray2>
          |  <array>
          |    <item>
          |      <inner1>str1</inner1>
          |    </item>
          |  </array>
          |</arrayOfArray2>
          |<arrayOfArray2>
          |  <array/>
          |  <array>
          |    <item>
          |      <inner2>str3</inner2>
          |      <inner2>str33</inner2>
          |    </item>
          |    <item>
          |      <inner2>str4</inner2>
          |      <inner1>str11</inner1>
          |    </item>
          |  </array>
          |</arrayOfArray2>
          |<arrayOfArray2>
          |  <array>
          |    <item>
          |      <inner3>
          |        <inner4>2</inner4>
          |        <inner4>3</inner4>
          |      </inner3>
          |      <inner3/>
          |    </item>
          |  </array>
          |</arrayOfArray2>
          |</ROW>
          |""".stripMargin :: Nil

  def nullsInArrays: Seq[String] =
    """<ROW>
         <field1>
            <array1>
              <array2>value1</array2>
              <array2>value2</array2>
            </array1>
            <array1/>
          </field1>
          <field1/>
        </ROW>""" ::
    """
        <ROW>
          <field2/>
          <field2>
            <array1>
              <Test>1</Test>
            </array1>
            <array1/>
          </field2>
        </ROW>""" ::
    """
        <ROW>
          <field1/>
          <field1><array1/></field1>
          <field2/>
        </ROW>""" :: Nil

  def corruptRecords: Seq[String] =
    """<ROW>""" ::
    """""" ::
    """<ROW>
        |  <a>1</a>
        |  <b>2</b>
        |</ROW>""".stripMargin ::
    """
        |<ROW>
        |  <a>str_a_4</a>
        |  <b>str_b_4</b>
        |  <c>str_c_4</c>
        |</ROW>
        |""".stripMargin ::
    """
        |</ROW>
        |""".stripMargin :: Nil

  def emptyRecords: Seq[String] =
    """<ROW>
          <a><struct></struct></a>
        </ROW>""" ::
    """<ROW>
          <a>
            <struct><b><c/></b></struct>
          </a>
        </ROW>""" ::
    """<ROW>
          <b>
            <item>
              <c><struct></struct></c>
            </item>
            <item/>
          </b>
        </ROW>""" :: Nil

  def arrayAndStructRecords: Seq[String] =
    """<ROW>
          <a>
            <b>1</b>
          </a>
        </ROW>""" ::
    """<ROW>
          <a><item/><item/></a>
        </ROW>""" ::
    Nil

  def valueTagsTypeConflict: Seq[String] =
    """
      |<ROW>
      |    13.1
      |    <a>
      |        11
      |        <b>
      |            true
      |            <c>1</c>
      |        </b>
      |    </a>
      |    string
      |</ROW>
      |""".stripMargin ::
    """
      |<ROW>
      |    string
      |    <a>
      |        21474836470
      |        <b>
      |            false
      |            <c>2</c>
      |        </b>
      |    </a>
      |    true
      |</ROW>
      |""".stripMargin ::
    """
        |<ROW>
        |<a>
        |    <b>
        |        12
        |        <c>3</c>
        |    </b>
        |</a>
        |92233720368547758070
        |</ROW>
        |""".stripMargin :: Nil

  val emptyValueTags: Seq[String] =
    """
      |<ROW>
      |    str1
      |    <a>  <b>1</b>
      |    </a>str2
      |</ROW>
      |""".stripMargin ::
    """<ROW> <a><b/> value</a></ROW>""" ::
    """<ROW><a><b>3</b> </a> </ROW>""" ::
    """<ROW><a><b>4</b> </a>
      |    str3
      |</ROW>""".stripMargin :: Nil

  val multilineValueTags =
    """
      |<ROW>
      |    value1
      |    <a>1</a>
      |    value2
      |</ROW>
      |""".stripMargin ::
    """
      |<ROW>
      |    value3
      |    value4<a>1</a>
      |</ROW>
      |""".stripMargin :: Nil

  val valueTagsAroundStructs =
    """
      |<ROW>
      |    value1
      |    <a>
      |        value2
      |        <b>
      |            3
      |            <c>1</c>
      |        </b>
      |        value4
      |    </a>
      |    value5
      |</ROW>
      |""".stripMargin ::
    """
      |<ROW>
      |    value1
      |    <a>
      |        value2
      |        <b>3</b>
      |        value4
      |    </a>
      |</ROW>
      |""".stripMargin ::
  """
      |<ROW>
      |    <a>
      |        <b></b>
      |        value4
      |        <!--First comment-->
      |        value5
      |    </a>
      |    value6
      |</ROW>
      |""".stripMargin ::
    """
      |<ROW>
      |    value1
      |    <a>
      |        value2
      |        <b>
      |            3
      |            <c/>
      |        </b>
      |        value4
      |    </a>
      |    value5
      |</ROW>
      |""".stripMargin :: Nil

  val valueTagsAroundArrays =
    """
      |<ROW>
      |    value1
      |    <array1>
      |        value2
      |        <array2>
      |          1
      |          <num>1</num>
      |          2
      |        </array2>
      |        value3
      |        <!--First comment--> <!--Second comment-->
      |        value4<!--Third comment-->
      |        value5
      |        <array2>2</array2>value6
      |        value7
      |    </array1>
      |    value8
      |    <array1>
      |        value9
      |        <array2> <!--First comment--><num>2</num></array2>
      |        value10
      |        <array2></array2>
      |        <array2> <!--First comment-->
      |        <!--Second comment--></array2>
      |        <array2>3</array2>
      |        value11
      |    </array1>
      |    value12
      |    <!--First comment-->
      |    value13
      |</ROW>
      |""".stripMargin ::
    """
      |<ROW>
      |    <array1>
      |        value1
      |    </array1>
      |</ROW>
      |""".stripMargin ::
    """
      |<ROW>
      |    <array1>
      |        <array2>
      |            1
      |        </array2>
      |    </array1>
      |    value1
      |</ROW>
      |""".stripMargin :: Nil

  val valueTagConflictName =
    """<ROW>
      |    <a>1</a>
      |    2
      |</ROW>""".stripMargin :: Nil

  val valueTagWithComments =
    """
      |<ROW>
      |    <!--First comment-->
      |    <!--Second comment-->
      |    <a><!--First comment--></a>
      |    <a attr="1"><!--First comment--> <!--Second comment--></a>
      |    2
      |</ROW>
      |""".stripMargin :: Nil

  val valueTagWithCDATA =
    """
      |<ROW>
      |    <![CDATA[This is a CDATA section containing <sample1> text.]]>
      |    <a>
      |        <![CDATA[This is a CDATA section containing <sample2> text.]]>
      |        <![CDATA[This is a CDATA section containing <sample3> text.]]>
      |        <b>1</b>
      |        <![CDATA[This is a CDATA section containing <sample4> text.]]>
      |
      |        <b>2</b>
      |
      |    </a>
      |
      |</ROW>
      |""".stripMargin :: Nil

  val valueTagIsNullValue =
    """
      |<ROW>
      |    1
      |</ROW>
      |""".stripMargin :: Nil
}
