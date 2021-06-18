package org.apache.spark.sql

import org.apache.avro.Schema
import org.apache.spark.sql.avro.SchemaConverters
import org.apache.spark.sql.functions._
import org.apache.spark.sql.test.SharedSparkSession

/**
 * @author Geek
 * @date 2021-06-07 15:03:53
 *
 * from_json 测试类运行准备：
 * 1. git checkout 2.4.3.1-kl (该版本合入了 spark3.0.0 的 from_json FAILFAST 功能 )
 * 可选，mvn clean install -U
 * https://stackoverflow.com/a/26819686/9633499
 *
 * 2. 在项目路径下打开 git cmd，执行命令： 
 * ./build/spark-build-info ./core/target/extra-resources 2.4.3
 * https://stackoverflow.com/a/44416809/9633499
 * Intellij IDEA 编译spark源码报错解决方法 | Joey's Notes
 * http://joey771.cn/2017/07/05/spark/spark%E6%BA%90%E7%A0%81%E9%98%85%E8%AF%BB/Intellij%20IDEA%20%E7%BC%96%E8%AF%91spark%E6%BA%90%E7%A0%81%E6%8A%A5%E9%94%99%E8%A7%A3%E5%86%B3%E6%96%B9%E6%B3%95/
 *
 * 3. maven --> spark-catalyst --> 右键 generate sources and update folders
 * https://stackoverflow.com/a/63202594/9633499
 *
 * 参考文档： spark 发行说明：https://spark.apache.org/news/index.html
 * https://spark.apache.org/releases/spark-release-3-0-0.html
 */
class FailfastTest extends QueryTest with SharedSparkSession {

  // string --> int     转换失败
  // string --> long    转换失败
  // string --> float   转换失败
  // string --> double  转换失败
  // string --> boolean 转换失败
  // string --> null    转换失败
  // int --> string     转换成功
  // int --> long       转换成功
  // int --> float      转换成功
  // int --> double     转换成功
  // int --> boolean    转换失败
  // int --> null       转换失败
  // long --> string    转换成功
  // long --> int       转换失败
  // long --> float     转换成功
  // long --> double    转换成功
  // long --> boolean   转换失败
  // long --> null      转换失败
  // float --> string   转换成功
  // float --> int      转换失败
  // float --> long     转换失败
  // float --> double   转换成功
  // float --> boolean  转换失败
  // float --> null     转换失败
  // double --> string  转换成功
  // double --> int     转换失败
  // double --> long    转换失败
  // double --> float   转换成功
  // double --> boolean 转换失败
  // double --> null    转换失败
  // boolean --> string 转换成功
  // boolean --> int    转换失败
  // boolean --> long   转换失败
  // boolean --> float  转换失败
  // boolean --> double 转换失败
  // boolean --> null   转换失败
  // null --> string    转换成功
  // null --> int       转换成功
  // null --> long      转换成功
  // null --> float     转换成功
  // null --> double    转换成功
  // null --> boolean   转换成功

  // 嵌套的 array | struct 类型遵循如上规则
  
  import testImplicits._

  /** 简单类型 ======================================================================================================= */

  /**
   * string --> int|long|float|double|boolean|null     转换失败 
   */
  test("1.from_json with FAILFAST: string --> int") {
    val df1 = Seq(
      """ {"c_int": 123} """,
      """ {"c_int": "456"} """
    ).toDF("value")

    val dataType = SchemaConverters.toSqlType(
      new Schema.Parser().parse(
        """
            |{
            |  "type": "record",
            |  "name": "schema",
            |  "fields": [
            |    {
            |      "name": "c_int",
            |      "type": [
            |        "int"
            |      ]
            |    }
            |  ]
            |}
            |""".stripMargin
      )
    ).dataType

    val options = Map[String, String]("mode" -> "FAILFAST")
    try {
      val df2 = df1.select(
        from_json(col("value"), dataType, options).as("json")
      ).select("json.*")

      df2.printSchema()
      /*
      root
       |-- c_int: integer (nullable = true)
       */
      df2.show(false)
    } catch {
      case e: Exception => {
        e.printStackTrace()
        assert(e.getMessage.contains("Failed to parse fieldName: [c_int], fieldValue: [456], [VALUE_STRING] to target spark dataType: [IntegerType]."))
      }
    }
  }

  /**
   * string --> int|long|float|double|boolean|null     转换失败 
   */
  test("2.from_json with FAILFAST: string --> long") {
    val df1 = Seq(
      """ {"d_long": 2212345678} """,
      """ {"d_long": "2312345678"} """
    ).toDF("value")

    val dataType = SchemaConverters.toSqlType(
      new Schema.Parser().parse(
        """
            |{
            |  "type": "record",
            |  "name": "schema",
            |  "fields": [
            |    {
            |      "name": "d_long",
            |      "type": [
            |        "long"
            |      ]
            |    }
            |  ]
            |}
            |""".stripMargin
      )
    ).dataType

    val options = Map[String, String]("mode" -> "FAILFAST")
    try {
      val df2 = df1.select(
        from_json(col("value"), dataType, options).as("json")
      ).select("json.*")

      df2.printSchema()
      /*
      root
       |-- d_long: long (nullable = true)
       */
      df2.show(false)
    } catch {
      case e: Exception => {
        e.printStackTrace()
        assert(e.getMessage.contains("Failed to parse fieldName: [d_long], fieldValue: [2312345678], [VALUE_STRING] to target spark dataType: [LongType]."))
      }
    }
  }

  /**
   * string --> int|long|float|double|boolean|null     转换失败 
   */
  test("3.from_json with FAILFAST: string --> float") {
    val df1 = Seq(
      """ {"e_float": 0.12} """,
      """ {"e_float": "1.34"} """
    ).toDF("value")

    val dataType = SchemaConverters.toSqlType(
      new Schema.Parser().parse(
        """
            |{
            |  "type": "record",
            |  "name": "schema",
            |  "fields": [
            |    {
            |      "name": "e_float",
            |      "type": [
            |        "float"
            |      ]
            |    }
            |  ]
            |}
            |""".stripMargin
      )
    ).dataType

    val options = Map[String, String]("mode" -> "FAILFAST")
    try {
      val df2 = df1.select(
        from_json(col("value"), dataType, options).as("json")
      ).select("json.*")

      df2.printSchema()
      /*
      root
       |-- e_float: float (nullable = true)
       */
      df2.show(false)
    } catch {
      case e: Exception => {
        e.printStackTrace()
        assert(e.getMessage.contains("Cannot parse fieldName: [e_float], fieldValue: [1.34], [VALUE_STRING] as target spark dataType: [FloatType]."))
      }
    }
  }

  /**
   * string --> int|long|float|double|boolean|null     转换失败 
   */
  test("4.from_json with FAILFAST: string --> double") {
    val df1 = Seq(
      """ {"f_double": 0.1234567890} """,
      """ {"f_double": "1.1234567890"} """
    ).toDF("value")

    val dataType = SchemaConverters.toSqlType(
      new Schema.Parser().parse(
        """
            |{
            |  "type": "record",
            |  "name": "schema",
            |  "fields": [
            |    {
            |      "name": "f_double",
            |      "type": [
            |        "double"
            |      ]
            |    }
            |  ]
            |}
            |""".stripMargin
      )
    ).dataType

    val options = Map[String, String]("mode" -> "FAILFAST")
    try {
      val df2 = df1.select(
        from_json(col("value"), dataType, options).as("json")
      ).select("json.*")

      df2.printSchema()
      /*
      root
       |-- e_float: float (nullable = true)
       */
      df2.show(false)
    } catch {
      case e: Exception => {
        e.printStackTrace()
        assert(e.getMessage.contains("Cannot parse fieldName: [f_double], fieldValue: [1.1234567890], [VALUE_STRING] as target spark dataType: [DoubleType]."))
      }
    }
  }

  /**
   * string --> int|long|float|double|boolean|null     转换失败 
   */
  test("5.from_json with FAILFAST: string --> boolean") {
    val df1 = Seq(
      """ {"g_boolean": true} """,
      """ {"g_boolean": "false"} """
    ).toDF("value")

    val dataType = SchemaConverters.toSqlType(
      new Schema.Parser().parse(
        """
            |{
            |  "type": "record",
            |  "name": "schema",
            |  "fields": [
            |    {
            |      "name": "g_boolean",
            |      "type": [
            |        "boolean"
            |      ]
            |    }
            |  ]
            |}
            |""".stripMargin
      )
    ).dataType

    val options = Map[String, String]("mode" -> "FAILFAST")
    try {
      val df2 = df1.select(
        from_json(col("value"), dataType, options).as("json")
      ).select("json.*")

      df2.printSchema()
      /*
      root
       |-- g_boolean: boolean (nullable = true)
       */
      df2.show(false)
    } catch {
      case e: Exception => {
        e.printStackTrace()
        assert(e.getMessage.contains("Failed to parse fieldName: [g_boolean], fieldValue: [false], [VALUE_STRING] to target spark dataType: [BooleanType]."))
      }
    }
  }

  /**
   * string --> int|long|float|double|boolean|null     转换失败 
   */
  test("6.from_json with FAILFAST: string --> null") {
    val df1 = Seq(
      """ {"i_null": null} """,
      """ {"i_null": "null"} """
    ).toDF("value")

    val dataType = SchemaConverters.toSqlType(
      new Schema.Parser().parse(
        """
            |{
            |  "type": "record",
            |  "name": "schema",
            |  "fields": [
            |    {
            |      "name": "i_null",
            |      "type": [
            |        "null"
            |      ]
            |    }
            |  ]
            |}
            |""".stripMargin
      )
    ).dataType

    val options = Map[String, String]("mode" -> "FAILFAST")
    try {
      val df2 = df1.select(
        from_json(col("value"), dataType, options).as("json")
      ).select("json.*")

      df2.printSchema()
      /*
root
 |-- i_null: struct (nullable = true)
       */
      df2.show(false)
    } catch {
      case e: Exception => {
        e.printStackTrace()
        assert(e.getMessage.contains("Failed to parse fieldName: [i_null], fieldValue: [null], [VALUE_STRING] to target spark dataType: [StructType()]."))
      }
    }
  }

  /**
   * int --> string|long|float|double     转换成功
   * int --> boolean|null     转换失败
   */
  test("7.from_json with FAILFAST: int --> string|long|float|double") {
    val df1 = Seq(
      """ {"a_string": null} """,
      """ {"a_string": ""} """,
      """ {"a_string": " "} """,
      """ {"a_string": "null"} """,
      """ {"a_string": 2100001234 } """,
      """ {"d_long": null} """,
      """ {"d_long": 2300001234} """,
      """ {"d_long": 1} """,
      """ {"e_float": null} """,
      """ {"e_float": 1.12} """,
      """ {"e_float": 2} """,
      """ {"e_float": 2100001234} """
    ).toDF("value")

    val dataType = SchemaConverters.toSqlType(
      new Schema.Parser().parse(
        """
            |{
            |  "type": "record",
            |  "name": "schema",
            |  "fields": [
            |    {
            |      "name": "a_string",
            |      "type": [
            |        "string"
            |      ]
            |    },
            |    {
            |      "name": "d_long",
            |      "type": [
            |        "long"
            |      ]
            |    },
            |    {
            |      "name": "e_float",
            |      "type": [
            |        "float"
            |      ]
            |    }
            |  ]
            |}
            |""".stripMargin
      )
    ).dataType

    val options = Map[String, String]("mode" -> "FAILFAST")
    val df2 = df1.select(
      from_json(col("value"), dataType, options).as("json")
    ).select("json.*")

    df2.printSchema()
    /*
root
 |-- a_string: string (nullable = true)
 |-- d_long: long (nullable = true)
 |-- e_float: float (nullable = true)
     */
    df2.show(false)
    /*
+----------+----------+------------+
|a_string  |d_long    |e_float     |
+----------+----------+------------+
|null      |null      |null        |
|          |null      |null        |
|          |null      |null        |
|null      |null      |null        |
|2100001234|null      |null        |
|null      |null      |null        |
|null      |2300001234|null        |
|null      |1         |null        |
|null      |null      |null        |
|null      |null      |1.12        |
|null      |null      |2.0         |
|null      |null      |2.10000128E9|
+----------+----------+------------+
     */
  }

  /**
   * int --> string|long|float|double     转换成功
   * int --> boolean|null     转换失败( 0 和 1 也不可以转)
   */
  test("8.from_json with FAILFAST: int --> boolean") {
    val df1 = Seq(
      """ {"g_boolean": null} """,
      """ {"g_boolean": true} """,
      """ {"g_boolean": 0} """
    ).toDF("value")

    val dataType = SchemaConverters.toSqlType(
      new Schema.Parser().parse(
        """
            |{
            |  "type": "record",
            |  "name": "schema",
            |  "fields": [
            |    {
            |      "name": "g_boolean",
            |      "type": [
            |        "boolean"
            |      ]
            |    }
            |  ]
            |}
            |""".stripMargin
      )
    ).dataType

    val options = Map[String, String]("mode" -> "FAILFAST")
    try {
      val df2 = df1.select(
        from_json(col("value"), dataType, options).as("json")
      ).select("json.*")

      df2.printSchema()
      /*
root
 |-- g_boolean: boolean (nullable = true)
       */
      df2.show(false)
    } catch {
      case e: Exception => {
        e.printStackTrace()
        assert(e.getMessage.contains("Failed to parse fieldName: [g_boolean], fieldValue: [0], [VALUE_NUMBER_INT] to target spark dataType: [BooleanType]."))
      }
    }
  }

  /**
   * int --> string|long|float|double     转换成功
   * int --> boolean|null     转换失败
   */
  test("9.from_json with FAILFAST: int --> null") {
    val df1 = Seq(
      """ {"i_null": null} """,
      """ {"i_null": 0} """
    ).toDF("value")

    val dataType = SchemaConverters.toSqlType(
      new Schema.Parser().parse(
        """
            |{
            |  "type": "record",
            |  "name": "schema",
            |  "fields": [
            |    {
            |      "name": "i_null",
            |      "type": [
            |        "null"
            |      ]
            |    }
            |  ]
            |}
            |""".stripMargin
      )
    ).dataType

    val options = Map[String, String]("mode" -> "FAILFAST")
    try {
      val df2 = df1.select(
        from_json(col("value"), dataType, options).as("json")
      ).select("json.*")

      df2.printSchema()
      /*
root
 |-- i_null: struct (nullable = true)
       */
      df2.show(false)
    } catch {
      case e: Exception => {
        e.printStackTrace()
        assert(e.getMessage.contains("Failed to parse fieldName: [i_null], fieldValue: [0], [VALUE_NUMBER_INT] to target spark dataType: [StructType()]."))
      }
    }
  }

  /**
   * long --> string|float|double    转换成功
   * long --> int|boolean|null       转换失败
   */
  test("10.from_json with FAILFAST: long --> string|float|double") {
    val df1 = Seq(
      """ {"a_string": null} """,
      """ {"a_string": "null"} """,
      """ {"a_string": "hello"} """,
      """ {"a_string": 2200001234 } """,
      """ {"e_float": null} """,
      """ {"e_float": 0.12} """,
      """ {"e_float": 2300001234} """,
      """ {"f_double": null} """,
      """ {"f_double": 1.12345678901234} """,
      """ {"f_double": 24000012341234} """
    ).toDF("value")

    val dataType = SchemaConverters.toSqlType(
      new Schema.Parser().parse(
        """
            |{
            |  "type": "record",
            |  "name": "schema",
            |  "fields": [
            |    {
            |      "name": "a_string",
            |      "type": [
            |        "string"
            |      ]
            |    },
            |    {
            |      "name": "e_float",
            |      "type": [
            |        "float"
            |      ]
            |    },
            |    {
            |      "name": "f_double",
            |      "type": [
            |        "double"
            |      ]
            |    }
            |  ]
            |}
            |""".stripMargin
      )
    ).dataType

    val options = Map[String, String]("mode" -> "FAILFAST")
    val df2 = df1.select(
      from_json(col("value"), dataType, options).as("json")
    ).select("json.*")

    df2.printSchema()
    /*
root
 |-- a_string: string (nullable = true)
 |-- e_float: float (nullable = true)
 |-- f_double: double (nullable = true)
     */
    df2.show(false)
    /*
    +----------+------------+------------------+
    |a_string  |e_float     |f_double          |
    +----------+------------+------------------+
    |null      |null        |null              |
    |null      |null        |null              |
    |hello     |null        |null              |
    |2200001234|null        |null              |
    |null      |null        |null              |
    |null      |0.12        |null              |
    |null      |2.30000128E9|null              |
    |null      |null        |null              |
    |null      |null        |1.12345678901234  |
    |null      |null        |2.4000012341234E13|
    +----------+------------+------------------+
     */
  }

  /**
   * long --> string|float|double    转换成功
   * long --> int|boolean|null       转换失败
   */
  test("11.from_json with FAILFAST: long --> int") {
    val df1 = Seq(
      """ {"c_int": null} """,
      """ {"c_int": 25012345678} """
    ).toDF("value")

    val dataType = SchemaConverters.toSqlType(
      new Schema.Parser().parse(
        """
            |{
            |  "type": "record",
            |  "name": "schema",
            |  "fields": [
            |    {
            |      "name": "c_int",
            |      "type": [
            |        "int"
            |      ]
            |    }
            |  ]
            |}
            |""".stripMargin
      )
    ).dataType

    val options = Map[String, String]("mode" -> "FAILFAST")
    try {
      val df2 = df1.select(
        from_json(col("value"), dataType, options).as("json")
      ).select("json.*")

      df2.printSchema()
      /*
root
 |-- c_int: integer (nullable = true)
       */
      df2.show(false)
    } catch {
      case e: Exception => {
        e.printStackTrace()
        assert(e.getMessage.contains("Numeric value (25012345678) out of range of int"))
      }
    }
  }

  /**
   * long --> string|float|double    转换成功
   * long --> int|boolean|null       转换失败
   */
  test("12.from_json with FAILFAST: long --> boolean") {
    val df1 = Seq(
      """ {"g_boolean": null} """,
      """ {"g_boolean": true} """,
      """ {"g_boolean": 2212345678} """
    ).toDF("value")

    val dataType = SchemaConverters.toSqlType(
      new Schema.Parser().parse(
        """
            |{
            |  "type": "record",
            |  "name": "schema",
            |  "fields": [
            |    {
            |      "name": "g_boolean",
            |      "type": [
            |        "boolean"
            |      ]
            |    }
            |  ]
            |}
            |""".stripMargin
      )
    ).dataType

    val options = Map[String, String]("mode" -> "FAILFAST")
    try {
      val df2 = df1.select(
        from_json(col("value"), dataType, options).as("json")
      ).select("json.*")

      df2.printSchema()
      /*
root
 |-- g_boolean: boolean (nullable = true)
       */
      df2.show(false)
    } catch {
      case e: Exception => {
        e.printStackTrace()
        assert(e.getMessage.contains("Failed to parse fieldName: [g_boolean], fieldValue: [2212345678], [VALUE_NUMBER_INT] to target spark dataType: [BooleanType]."))
      }
    }
  }

  /**
   * long --> string|float|double    转换成功
   * long --> int|boolean|null       转换失败
   */
  test("13.from_json with FAILFAST: long --> null") {
    val df1 = Seq(
      """ {"i_null": null} """,
      """ {"i_null": 2212345678} """
    ).toDF("value")

    val dataType = SchemaConverters.toSqlType(
      new Schema.Parser().parse(
        """
            |{
            |  "type": "record",
            |  "name": "schema",
            |  "fields": [
            |    {
            |      "name": "i_null",
            |      "type": [
            |        "null"
            |      ]
            |    }
            |  ]
            |}
            |""".stripMargin
      )
    ).dataType

    val options = Map[String, String]("mode" -> "FAILFAST")
    try {
      val df2 = df1.select(
        from_json(col("value"), dataType, options).as("json")
      ).select("json.*")

      df2.printSchema()
      /*
root
 |-- i_null: struct (nullable = true)
       */
      df2.show(false)
    } catch {
      case e: Exception => {
        e.printStackTrace()
        assert(e.getMessage.contains("Failed to parse fieldName: [i_null], fieldValue: [2212345678], [VALUE_NUMBER_INT] to target spark dataType: [StructType()]."))
      }
    }
  }

  /**
   * float --> string|double   转换成功
   * float --> int|long|boolean|null      转换失败
   */
  test("14.from_json with FAILFAST: float --> string|double") {
    val df1 = Seq(
      """ {"a_string": null} """,
      """ {"a_string": "null"} """,
      """ {"a_string": "hello"} """,
      """ {"a_string": 1.234 } """,
      """ {"f_double": null} """,
      """ {"f_double": 1.12345} """,
      """ {"f_double": 2.12345678901} """
    ).toDF("value")

    val dataType = SchemaConverters.toSqlType(
      new Schema.Parser().parse(
        """
            |{
            |  "type": "record",
            |  "name": "schema",
            |  "fields": [
            |    {
            |      "name": "a_string",
            |      "type": [
            |        "string"
            |      ]
            |    },
            |    {
            |      "name": "f_double",
            |      "type": [
            |        "double"
            |      ]
            |    }
            |  ]
            |}
            |""".stripMargin
      )
    ).dataType

    val options = Map[String, String]("mode" -> "FAILFAST")
    val df2 = df1.select(
      from_json(col("value"), dataType, options).as("json")
    ).select("json.*")

    df2.printSchema()
    /*
root
 |-- a_string: string (nullable = true)
 |-- f_double: double (nullable = true)
     */
    df2.show(false)
    /*
    +--------+-------------+
    |a_string|f_double     |
    +--------+-------------+
    |null    |null         |
    |null    |null         |
    |hello   |null         |
    |1.234   |null         |
    |null    |null         |
    |null    |1.12345      |
    |null    |2.12345678901|
    +--------+-------------+
     */
  }

  /**
   * float --> string|double   转换成功
   * float --> int|long|boolean|null      转换失败
   */
  test("15.from_json with FAILFAST: float --> int") {
    val df1 = Seq(
      """ {"c_int": null} """,
      """ {"c_int": 12} """,
      """ {"c_int": 1.234} """
    ).toDF("value")

    val dataType = SchemaConverters.toSqlType(
      new Schema.Parser().parse(
        """
            |{
            |  "type": "record",
            |  "name": "schema",
            |  "fields": [
            |    {
            |      "name": "c_int",
            |      "type": [
            |        "int"
            |      ]
            |    }
            |  ]
            |}
            |""".stripMargin
      )
    ).dataType

    val options = Map[String, String]("mode" -> "FAILFAST")
    try {
      val df2 = df1.select(
        from_json(col("value"), dataType, options).as("json")
      ).select("json.*")

      df2.printSchema()
      /*
root
 |-- c_int: integer (nullable = true)
       */
      df2.show(false)
    } catch {
      case e: Exception => {
        e.printStackTrace()
        assert(e.getMessage.contains("Failed to parse fieldName: [c_int], fieldValue: [1.234], [VALUE_NUMBER_FLOAT] to target spark dataType: [IntegerType]."))
      }
    }
  }

  /**
   * float --> string|double   转换成功
   * float --> int|long|boolean|null      转换失败
   */
  test("16.from_json with FAILFAST: float --> long") {
    val df1 = Seq(
      """ {"d_long": null} """,
      """ {"d_long": 2212345678} """,
      """ {"d_long": 1.234} """
    ).toDF("value")

    val dataType = SchemaConverters.toSqlType(
      new Schema.Parser().parse(
        """
            |{
            |  "type": "record",
            |  "name": "schema",
            |  "fields": [
            |    {
            |      "name": "d_long",
            |      "type": [
            |        "long"
            |      ]
            |    }
            |  ]
            |}
            |""".stripMargin
      )
    ).dataType

    val options = Map[String, String]("mode" -> "FAILFAST")
    try {
      val df2 = df1.select(
        from_json(col("value"), dataType, options).as("json")
      ).select("json.*")

      df2.printSchema()
      /*
root
 |-- d_long: long (nullable = true)
       */
      df2.show(false)
    } catch {
      case e: Exception => {
        e.printStackTrace()
        assert(e.getMessage.contains("Failed to parse fieldName: [d_long], fieldValue: [1.234], [VALUE_NUMBER_FLOAT] to target spark dataType: [LongType]."))
      }
    }
  }

  /**
   * float --> string|double   转换成功
   * float --> int|long|boolean|null      转换失败
   */
  test("17.from_json with FAILFAST: float --> boolean") {
    val df1 = Seq(
      """ {"g_boolean": null} """,
      """ {"g_boolean": true} """,
      """ {"g_boolean": 1.234} """
    ).toDF("value")

    val dataType = SchemaConverters.toSqlType(
      new Schema.Parser().parse(
        """
            |{
            |  "type": "record",
            |  "name": "schema",
            |  "fields": [
            |    {
            |      "name": "g_boolean",
            |      "type": [
            |        "boolean"
            |      ]
            |    }
            |  ]
            |}
            |""".stripMargin
      )
    ).dataType

    val options = Map[String, String]("mode" -> "FAILFAST")
    try {
      val df2 = df1.select(
        from_json(col("value"), dataType, options).as("json")
      ).select("json.*")

      df2.printSchema()
      /*
root
 |-- g_boolean: boolean (nullable = true)
       */
      df2.show(false)
    } catch {
      case e: Exception => {
        e.printStackTrace()
        assert(e.getMessage.contains("Failed to parse fieldName: [g_boolean], fieldValue: [1.234], [VALUE_NUMBER_FLOAT] to target spark dataType: [BooleanType]."))
      }
    }
  }

  /**
   * float --> string|double   转换成功
   * float --> int|long|boolean|null      转换失败
   */
  test("18.from_json with FAILFAST: float --> null") {
    val df1 = Seq(
      """ {"i_null": null} """,
      """ {"i_null": 1.234} """
    ).toDF("value")

    val dataType = SchemaConverters.toSqlType(
      new Schema.Parser().parse(
        """
            |{
            |  "type": "record",
            |  "name": "schema",
            |  "fields": [
            |    {
            |      "name": "i_null",
            |      "type": [
            |        "null"
            |      ]
            |    }
            |  ]
            |}
            |""".stripMargin
      )
    ).dataType

    val options = Map[String, String]("mode" -> "FAILFAST")
    try {
      val df2 = df1.select(
        from_json(col("value"), dataType, options).as("json")
      ).select("json.*")

      df2.printSchema()
      /*
root
 |-- i_null: struct (nullable = true)
       */
      df2.show(false)
    } catch {
      case e: Exception => {
        e.printStackTrace()
        assert(e.getMessage.contains("Failed to parse fieldName: [i_null], fieldValue: [1.234], [VALUE_NUMBER_FLOAT] to target spark dataType: [StructType()]."))
      }
    }
  }

  /**
   * double --> string|float  转换成功
   * double --> int|long|boolean|null     转换失败
   */
  test("19.from_json with FAILFAST: double --> string") {
    val df1 = Seq(
      """ {"a_string": null} """,
      """ {"a_string": "null"} """,
      """ {"a_string": "hello"} """,
      """ {"a_string": 1.1234567890123456789 } """
    ).toDF("value")

    val dataType = SchemaConverters.toSqlType(
      new Schema.Parser().parse(
        """
            |{
            |  "type": "record",
            |  "name": "schema",
            |  "fields": [
            |    {
            |      "name": "a_string",
            |      "type": [
            |        "string"
            |      ]
            |    }
            |  ]
            |}
            |""".stripMargin
      )
    ).dataType

    val options = Map[String, String]("mode" -> "FAILFAST")
    val df2 = df1.select(
      from_json(col("value"), dataType, options).as("json")
    ).select("json.*")

    df2.printSchema()
    /*
  root
  |-- a_string: string (nullable = true)
     */
    df2.show(false)
    /*
    +------------------+
    |a_string          |
    +------------------+
    |null              |
    |null              |
    |hello             |
    |1.1234567890123457|
    +------------------+
     */
  }

  /**
   * double --> string|float  转换成功
   * double --> int|long|boolean|null     转换失败
   */
  test("20.from_json with FAILFAST: double --> int") {
    val df1 = Seq(
      """ {"c_int": null} """,
      """ {"c_int": 1234} """,
      """ {"c_int": 0.1234567890123456} """
    ).toDF("value")

    val dataType = SchemaConverters.toSqlType(
      new Schema.Parser().parse(
        """
            |{
            |  "type": "record",
            |  "name": "schema",
            |  "fields": [
            |    {
            |      "name": "c_int",
            |      "type": [
            |        "int"
            |      ]
            |    }
            |  ]
            |}
            |""".stripMargin
      )
    ).dataType

    val options = Map[String, String]("mode" -> "FAILFAST")
    try {
      val df2 = df1.select(
        from_json(col("value"), dataType, options).as("json")
      ).select("json.*")

      df2.printSchema()
      /*
root
 |-- c_int: integer (nullable = true)
       */
      df2.show(false)
    } catch {
      case e: Exception => {
        e.printStackTrace()
        assert(e.getMessage.contains("Failed to parse fieldName: [c_int], fieldValue: [0.1234567890123456], [VALUE_NUMBER_FLOAT] to target spark dataType: [IntegerType]."))
      }
    }
  }

  /**
   * double --> string|float  转换成功
   * double --> int|long|boolean|null     转换失败
   */
  test("21.from_json with FAILFAST: double --> long") {
    val df1 = Seq(
      """ {"d_long": null} """,
      """ {"d_long": 2312345678} """,
      """ {"d_long": 0.1234567890123456} """
    ).toDF("value")

    val dataType = SchemaConverters.toSqlType(
      new Schema.Parser().parse(
        """
            |{
            |  "type": "record",
            |  "name": "schema",
            |  "fields": [
            |    {
            |      "name": "d_long",
            |      "type": [
            |        "long"
            |      ]
            |    }
            |  ]
            |}
            |""".stripMargin
      )
    ).dataType

    val options = Map[String, String]("mode" -> "FAILFAST")
    try {
      val df2 = df1.select(
        from_json(col("value"), dataType, options).as("json")
      ).select("json.*")

      df2.printSchema()
      /*
root
 |-- c_int: integer (nullable = true)
       */
      df2.show(false)
    } catch {
      case e: Exception => {
        e.printStackTrace()
        assert(e.getMessage.contains("Failed to parse fieldName: [d_long], fieldValue: [0.1234567890123456], [VALUE_NUMBER_FLOAT] to target spark dataType: [LongType]."))
      }
    }
  }

  /**
   * double --> string|float  转换成功
   * double --> int|long|boolean|null     转换失败
   */
  test("22.from_json with FAILFAST: double --> float") {
    val df1 = Seq(
      """ {"e_float": null} """,
      """ {"e_float": 1.01} """,
      """ {"e_float": 1.7976931348623157E308 } """
    ).toDF("value")

    val dataType = SchemaConverters.toSqlType(
      new Schema.Parser().parse(
        """
            |{
            |  "type": "record",
            |  "name": "schema",
            |  "fields": [
            |    {
            |      "name": "e_float",
            |      "type": [
            |        "float"
            |      ]
            |    }
            |  ]
            |}
            |""".stripMargin
      )
    ).dataType

    val options = Map[String, String]("mode" -> "FAILFAST")
    val df2 = df1.select(
      from_json(col("value"), dataType, options).as("json")
    ).select("json.*")

    df2.printSchema()
    /*
root
|-- e_float: float (nullable = true)
     */
    df2.show(false)
    /*
    +--------+
    |e_float |
    +--------+
    |null    |
    |1.01    |
    |Infinity|
    +--------+
     */
  }

  /**
   * double --> string|float  转换成功
   * double --> int|long|boolean|null     转换失败
   */
  test("23.from_json with FAILFAST: double --> boolean") {
    val df1 = Seq(
      """ {"g_boolean": null} """,
      """ {"g_boolean": false} """,
      """ {"g_boolean": 1.7976931348623157E308} """
    ).toDF("value")

    val dataType = SchemaConverters.toSqlType(
      new Schema.Parser().parse(
        """
            |{
            |  "type": "record",
            |  "name": "schema",
            |  "fields": [
            |    {
            |      "name": "g_boolean",
            |      "type": [
            |        "boolean"
            |      ]
            |    }
            |  ]
            |}
            |""".stripMargin
      )
    ).dataType

    val options = Map[String, String]("mode" -> "FAILFAST")
    try {
      val df2 = df1.select(
        from_json(col("value"), dataType, options).as("json")
      ).select("json.*")

      df2.printSchema()
      /*
root
 |-- g_boolean: boolean (nullable = true)
       */
      df2.show(false)
    } catch {
      case e: Exception => {
        e.printStackTrace()
        assert(e.getMessage.contains("Failed to parse fieldName: [g_boolean], fieldValue: [1.7976931348623157E308], [VALUE_NUMBER_FLOAT] to target spark dataType: [BooleanType]."))
      }
    }
  }

  /**
   * double --> string|float  转换成功
   * double --> int|long|boolean|null     转换失败
   */
  test("24.from_json with FAILFAST: double --> null") {
    val df1 = Seq(
      """ {"i_null": null} """,
      """ {"i_null": 1.7976931348623157E308} """
    ).toDF("value")

    val dataType = SchemaConverters.toSqlType(
      new Schema.Parser().parse(
        """
            |{
            |  "type": "record",
            |  "name": "schema",
            |  "fields": [
            |    {
            |      "name": "i_null",
            |      "type": [
            |        "null"
            |      ]
            |    }
            |  ]
            |}
            |""".stripMargin
      )
    ).dataType

    val options = Map[String, String]("mode" -> "FAILFAST")
    try {
      val df2 = df1.select(
        from_json(col("value"), dataType, options).as("json")
      ).select("json.*")

      df2.printSchema()
      /*
root
 |-- i_null: struct (nullable = true)
       */
      df2.show(false)
    } catch {
      case e: Exception => {
        e.printStackTrace()
        assert(e.getMessage.contains("Failed to parse fieldName: [i_null], fieldValue: [1.7976931348623157E308], [VALUE_NUMBER_FLOAT] to target spark dataType: [StructType()]."))
      }
    }
  }

  /**
   * boolean --> string 转换成功
   * boolean --> int|long|float|double|null    转换失败
   */
  test("25.from_json with FAILFAST: boolean --> string") {
    val df1 = Seq(
      """ {"a_string": null} """,
      """ {"a_string": "null"} """,
      """ {"a_string": "hello"} """,
      """ {"a_string": false } """
    ).toDF("value")

    val dataType = SchemaConverters.toSqlType(
      new Schema.Parser().parse(
        """
            |{
            |  "type": "record",
            |  "name": "schema",
            |  "fields": [
            |    {
            |      "name": "a_string",
            |      "type": [
            |        "string"
            |      ]
            |    }
            |  ]
            |}
            |""".stripMargin
      )
    ).dataType

    val options = Map[String, String]("mode" -> "FAILFAST")
    val df2 = df1.select(
      from_json(col("value"), dataType, options).as("json")
    ).select("json.*")

    df2.printSchema()
    /*
  root
  |-- a_string: string (nullable = true)
     */
    df2.show(false)
    /*
+--------+
|a_string|
+--------+
|null    |
|null    |
|hello   |
|false   |
+--------+
     */
  }

  /**
   * boolean --> string 转换成功
   * boolean --> int|long|float|double|null    转换失败
   */
  test("26.from_json with FAILFAST: boolean --> int") {
    val df1 = Seq(
      """ {"c_int": null} """,
      """ {"c_int": 11} """,
      """ {"c_int": true} """
    ).toDF("value")

    val dataType = SchemaConverters.toSqlType(
      new Schema.Parser().parse(
        """
            |{
            |  "type": "record",
            |  "name": "schema",
            |  "fields": [
            |    {
            |      "name": "c_int",
            |      "type": [
            |        "int"
            |      ]
            |    }
            |  ]
            |}
            |""".stripMargin
      )
    ).dataType

    val options = Map[String, String]("mode" -> "FAILFAST")
    try {
      val df2 = df1.select(
        from_json(col("value"), dataType, options).as("json")
      ).select("json.*")

      df2.printSchema()
      /*
root
 |-- c_int: integer (nullable = true)
       */
      df2.show(false)
    } catch {
      case e: Exception => {
        e.printStackTrace()
        assert(e.getMessage.contains("Failed to parse fieldName: [c_int], fieldValue: [true], [VALUE_TRUE] to target spark dataType: [IntegerType]."))
      }
    }
  }

  /**
   * boolean --> string 转换成功
   * boolean --> int|long|float|double|null    转换失败
   */
  test("27.from_json with FAILFAST: boolean --> long") {
    val df1 = Seq(
      """ {"d_long": null} """,
      """ {"d_long": 2212345678} """,
      """ {"d_long": false} """
    ).toDF("value")

    val dataType = SchemaConverters.toSqlType(
      new Schema.Parser().parse(
        """
            |{
            |  "type": "record",
            |  "name": "schema",
            |  "fields": [
            |    {
            |      "name": "d_long",
            |      "type": [
            |        "long"
            |      ]
            |    }
            |  ]
            |}
            |""".stripMargin
      )
    ).dataType

    val options = Map[String, String]("mode" -> "FAILFAST")
    try {
      val df2 = df1.select(
        from_json(col("value"), dataType, options).as("json")
      ).select("json.*")

      df2.printSchema()
      /*
root
 |-- c_int: integer (nullable = true)
       */
      df2.show(false)
    } catch {
      case e: Exception => {
        e.printStackTrace()
        assert(e.getMessage.contains("Failed to parse fieldName: [d_long], fieldValue: [false], [VALUE_FALSE] to target spark dataType: [LongType]."))
      }
    }
  }

  /**
   * boolean --> string 转换成功
   * boolean --> int|long|float|double|null    转换失败
   */
  test("28.from_json with FAILFAST: boolean --> float") {
    val df1 = Seq(
      """ {"e_float": null} """,
      """ {"e_float": 1.23} """,
      """ {"e_float": false} """
    ).toDF("value")

    val dataType = SchemaConverters.toSqlType(
      new Schema.Parser().parse(
        """
            |{
            |  "type": "record",
            |  "name": "schema",
            |  "fields": [
            |    {
            |      "name": "e_float",
            |      "type": [
            |        "float"
            |      ]
            |    }
            |  ]
            |}
            |""".stripMargin
      )
    ).dataType

    val options = Map[String, String]("mode" -> "FAILFAST")
    try {
      val df2 = df1.select(
        from_json(col("value"), dataType, options).as("json")
      ).select("json.*")

      df2.printSchema()
      /*
root
 |-- e_float: float (nullable = true)
       */
      df2.show(false)
    } catch {
      case e: Exception => {
        e.printStackTrace()
        assert(e.getMessage.contains("Failed to parse fieldName: [e_float], fieldValue: [false], [VALUE_FALSE] to target spark dataType: [FloatType]."))
      }
    }
  }

  /**
   * boolean --> string 转换成功
   * boolean --> int|long|float|double|null    转换失败
   */
  test("29.from_json with FAILFAST: boolean --> double") {
    val df1 = Seq(
      """ {"f_double": null} """,
      """ {"f_double": 1.7976931348623157E308} """,
      """ {"f_double": false} """
    ).toDF("value")

    val dataType = SchemaConverters.toSqlType(
      new Schema.Parser().parse(
        """
            |{
            |  "type": "record",
            |  "name": "schema",
            |  "fields": [
            |    {
            |      "name": "f_double",
            |      "type": [
            |        "double"
            |      ]
            |    }
            |  ]
            |}
            |""".stripMargin
      )
    ).dataType

    val options = Map[String, String]("mode" -> "FAILFAST")
    try {
      val df2 = df1.select(
        from_json(col("value"), dataType, options).as("json")
      ).select("json.*")

      df2.printSchema()
      /*
root
 |-- f_double: double (nullable = true)
       */
      df2.show(false)
    } catch {
      case e: Exception => {
        e.printStackTrace()
        assert(e.getMessage.contains("Failed to parse fieldName: [f_double], fieldValue: [false], [VALUE_FALSE] to target spark dataType: [DoubleType]."))
      }
    }
  }

  /**
   * boolean --> string 转换成功
   * boolean --> int|long|float|double|null    转换失败
   */
  test("30.from_json with FAILFAST: boolean --> null") {
    val df1 = Seq(
      """ {"i_null": null} """,
      """ {"i_null": true} """
    ).toDF("value")

    val dataType = SchemaConverters.toSqlType(
      new Schema.Parser().parse(
        """
            |{
            |  "type": "record",
            |  "name": "schema",
            |  "fields": [
            |    {
            |      "name": "i_null",
            |      "type": [
            |        "null"
            |      ]
            |    }
            |  ]
            |}
            |""".stripMargin
      )
    ).dataType

    val options = Map[String, String]("mode" -> "FAILFAST")
    try {
      val df2 = df1.select(
        from_json(col("value"), dataType, options).as("json")
      ).select("json.*")

      df2.printSchema()
      /*
root
 |-- i_null: struct (nullable = true)
       */
      df2.show(false)
    } catch {
      case e: Exception => {
        e.printStackTrace()
        assert(e.getMessage.contains("Failed to parse fieldName: [i_null], fieldValue: [true], [VALUE_TRUE] to target spark dataType: [StructType()]."))
      }
    }
  }

  /**
   * null --> string|int|long|float|double|boolean    转换成功
   */
  test("31.from_json with FAILFAST: null --> string|int|long|float|double|boolean") {
    val df1 = Seq(
      """ {"a_string": null} """,
      """ {"c_int": null} """,
      """ {"d_long": null} """,
      """ {"e_float": null} """,
      """ {"f_double": null} """,
      """ {"g_boolean": null} """,
      """ {"i_null": null} """
    ).toDF("value")

    val dataType = SchemaConverters.toSqlType(
      new Schema.Parser().parse(
        """
            |{
            |  "type": "record",
            |  "name": "schema",
            |  "fields": [
            |    {
            |      "name": "a_string",
            |      "type": [
            |        "string"
            |      ]
            |    },
            |    {
            |      "name": "c_int",
            |      "type": [
            |        "int"
            |      ]
            |    },
            |    {
            |      "name": "d_long",
            |      "type": [
            |        "long"
            |      ]
            |    },
            |    {
            |      "name": "e_float",
            |      "type": [
            |        "float"
            |      ]
            |    },
            |    {
            |      "name": "f_double",
            |      "type": [
            |        "double"
            |      ]
            |    },
            |    {
            |      "name": "g_boolean",
            |      "type": [
            |        "boolean"
            |      ]
            |    },
            |    {
            |      "name": "i_null",
            |      "type": [
            |        "null"
            |      ]
            |    }
            |  ]
            |}
            |""".stripMargin
      )
    ).dataType

    val options = Map[String, String]("mode" -> "FAILFAST")
    val df2 = df1.select(
      from_json(col("value"), dataType, options).as("json")
    ).select("json.*")

    df2.printSchema()
    /*
root
|-- a_string: string (nullable = true)
|-- c_int: integer (nullable = true)
|-- d_long: long (nullable = true)
|-- e_float: float (nullable = true)
|-- f_double: double (nullable = true)
|-- g_boolean: boolean (nullable = true)
|-- i_null: struct (nullable = true)
     */
    df2.show(false)
    /*
    +--------+-----+------+-------+--------+---------+------+
    |a_string|c_int|d_long|e_float|f_double|g_boolean|i_null|
    +--------+-----+------+-------+--------+---------+------+
    |null    |null |null  |null   |null    |null     |null  |
    |null    |null |null  |null   |null    |null     |null  |
    |null    |null |null  |null   |null    |null     |null  |
    |null    |null |null  |null   |null    |null     |null  |
    |null    |null |null  |null   |null    |null     |null  |
    |null    |null |null  |null   |null    |null     |null  |
    |null    |null |null  |null   |null    |null     |null  |
    +--------+-----+------+-------+--------+---------+------+
    
     */
  }

  /** 嵌套类型 ======================================================================================================= */

  /**
   * 嵌套结构，输入数据格式不一致，jsonSchema 正确，没有使用 FAILFAST，结果出现空记录
   * 正常运行，第三行记录全部为空
   */
  test("32.from_json without FAILFAST: error data") {
    val df1 = Seq(
      """ {"a_string":"123","c_int":12345,"d_long":123456789012,"e_float":1.1,"f_double":0.0145,"g_boolean":false,"i_null":null,"j_undefine":[1,2]} """,
      """ {"a_string":456,"c_int":112345,"d_long":22123456789012,"e_float":1.12,"f_double":10.0145,"g_boolean":true,"i_null":null,"j_undefine":[13,2]} """,
      """ {"a_string":"4567","c_int":"112345","d_long":22123456789012,"e_float":1.12,"f_double":10.0145,"g_boolean":true,"i_null":null,"j_undefine":[13,2]} """,
      """ {"a_string":["456"],"c_int":112345,"d_long":22123456789012,"e_float":1.12,"f_double":10.0145,"g_boolean":true,"i_null":null,"j_undefine":[13,2]} """
    ).toDF("value")

    val dataType = SchemaConverters.toSqlType(
      new Schema.Parser().parse(
        """
            |{
            |  "type": "record",
            |  "name": "schema",
            |  "fields": [
            |    {
            |      "name": "a_string",
            |      "type": [
            |        "string"
            |      ]
            |    },
            |    {
            |      "name": "c_int",
            |      "type": [
            |        "int"
            |      ]
            |    },
            |    {
            |      "name": "d_long",
            |      "type": [
            |        "long"
            |      ]
            |    },
            |    {
            |      "name": "e_float",
            |      "type": [
            |        "float"
            |      ]
            |    },
            |    {
            |      "name": "f_double",
            |      "type": [
            |        "double"
            |      ]
            |    },
            |    {
            |      "name": "g_boolean",
            |      "type": [
            |        "boolean"
            |      ]
            |    },
            |    {
            |      "name": "i_null",
            |      "type": [
            |        "null"
            |      ]
            |    }
            |  ]
            |}
            |""".stripMargin
      )
    ).dataType

    val df2 = df1.select(
      from_json(col("value"), dataType).as("json")
    ).select("json.*")

    df2.printSchema()
    /*
    root
     |-- a_string: string (nullable = true)
     |-- c_int: integer (nullable = true)
     |-- d_long: long (nullable = true)
     |-- e_float: float (nullable = true)
     |-- f_double: double (nullable = true)
     |-- g_boolean: boolean (nullable = true)
     |-- i_null: struct (nullable = true)
     */
    df2.show(false)
    /*
  spark 3.1.2
  +--------+-----+------+--------+------------+
  |a_double|b_int|c_bool|d_string|e_long      |
  +--------+-----+------+--------+------------+
  |0.01    |123  |false |hello   |123456789012|
  |null    |123  |false |hello   |123456789012|
  |0.012   |456  |true  |world   |123456789022|
  +--------+-----+------+--------+------------+    
  spark: 2.4.3.1
+--------+------+--------------+-------+--------+---------+------+
|a_string|c_int |d_long        |e_float|f_double|g_boolean|i_null|
+--------+------+--------------+-------+--------+---------+------+
|123     |12345 |123456789012  |1.1    |0.0145  |false    |null  |
|456     |112345|22123456789012|1.12   |10.0145 |true     |null  |
|null    |null  |null          |null   |null    |null     |null  |
|["456"] |112345|22123456789012|1.12   |10.0145 |true     |null  |
+--------+------+--------------+-------+--------+---------+------+
     */
  }

  /**
   * 嵌套结构，输入数据格式一致，jsonSchema 正确，没有使用 FAILFAST，结果正确
   */
  test("33.complex json without FAILFAST: rcf_datalake_doc") {
    val df1 = Seq(
      """{"urls":[{"url":"https://new.qq.com/omn/20210518/20210518A056UN00","url_type":"W","url_seq":1},{"url":"http://inews.gtimg.com/newsapp_match/0/13538584839/0","url_type":"B","url_seq":1}]}""",
      """{"urls":[{"url":"https://new.qq.com/omn/20210518/20210518A056UN00","url_type":"W","url_seq":1}]}""",
      """{"urls":[null]}""",
      """{"urls":[{"url":"https://new.qq.com/omn/20210518/20210518A056UN00","url_type":"W","url_seq":1}]}"""
    ).toDF("value")

    val dataType = SchemaConverters.toSqlType(
      new Schema.Parser().parse(
        """
            |{
            |  "type": "record",
            |  "name": "jsonSchema",
            |  "fields": [
            |    {
            |      "name": "urls",
            |      "type": [
            |        "null",
            |        {
            |          "type": "array",
            |          "items": {
            |            "type": "record",
            |            "name": "url_record",
            |            "fields": [
            |              {
            |                "name": "url",
            |                "type": "string"
            |              },
            |              {
            |                "name": "url_type",
            |                "type": "string"
            |              },
            |              {
            |                "name": "url_seq",
            |                "type": "int"
            |              }
            |            ]
            |          }
            |        }
            |      ],
            |      "default": null
            |    }
            |  ]
            |}
            |""".stripMargin
      )
    ).dataType

    val df2 = df1.select(
      from_json(col("value"), dataType).as("json")
    ).select("json.*")

    df2.printSchema()
    /*
root
 |-- urls: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- url: string (nullable = true)
 |    |    |-- url_type: string (nullable = true)
 |    |    |-- url_seq: integer (nullable = true)
     */
    df2.show(false)
    /*
    +------------------------------------------------------------------------------------------------------------------------+
    |urls                                                                                                                    |
    +------------------------------------------------------------------------------------------------------------------------+
    |[[https://new.qq.com/omn/20210518/20210518A056UN00, W, 1], [http://inews.gtimg.com/newsapp_match/0/13538584839/0, B, 1]]|
    |[[https://new.qq.com/omn/20210518/20210518A056UN00, W, 1]]                                                              |
    |[]                                                                                                                      |
    |[[https://new.qq.com/omn/20210518/20210518A056UN00, W, 1]]                                                              |
    +------------------------------------------------------------------------------------------------------------------------+
     */
  }

  /**
   * 嵌套结构，输入数据格式不一致，jsonSchema 正确，使用 FAILFAST，抛异常
   */
  test("34.complex json with FAILFAST: rcf_datalake_doc") {
    val df1 = Seq(
      """{"urls":[{"url":"https://new.qq.com/omn/20210518/20210518A056UN00","url_type":"W","url_seq":1},{"url":"http://inews.gtimg.com/newsapp_match/0/13538584839/0","url_type":"B","url_seq":1}]}""",
      """{"urls":[{"url":"https://new.qq.com/omn/20210518/20210518A056UN00","url_type":"W","url_seq":"1"}]}""",
      """{"urls":[null]}""",
      """{"urls":[{"url":"https://new.qq.com/omn/20210518/20210518A056UN00","url_type":"W","url_seq":1}]}"""
    ).toDF("value")

    val dataType = SchemaConverters.toSqlType(
      new Schema.Parser().parse(
        """
            |{
            |  "type": "record",
            |  "name": "jsonSchema",
            |  "fields": [
            |    {
            |      "name": "urls",
            |      "type": [
            |        "null",
            |        {
            |          "type": "array",
            |          "items": {
            |            "type": "record",
            |            "name": "url_record",
            |            "fields": [
            |              {
            |                "name": "url",
            |                "type": "string"
            |              },
            |              {
            |                "name": "url_type",
            |                "type": "string"
            |              },
            |              {
            |                "name": "url_seq",
            |                "type": "int"
            |              }
            |            ]
            |          }
            |        }
            |      ],
            |      "default": null
            |    }
            |  ]
            |}
            |""".stripMargin
      )
    ).dataType

    try {
      val options = Map[String, String]("mode" -> "FAILFAST")
      val df2 = df1.select(
        from_json(col("value"), dataType, options).as("json")
      ).select("json.*")

      df2.printSchema()
      /*
  root
   |-- urls: array (nullable = true)
   |    |-- element: struct (containsNull = true)
   |    |    |-- url: string (nullable = true)
   |    |    |-- url_type: string (nullable = true)
   |    |    |-- url_seq: integer (nullable = true)
       */
      df2.show(false)
    } catch {
      case e: Exception =>
        e.printStackTrace()
        assert(
          e.getMessage.contains("Failed to parse fieldName: [url_seq], fieldValue: [1], [VALUE_STRING] to target spark dataType: [IntegerType].")
        )
    }
  }
}
