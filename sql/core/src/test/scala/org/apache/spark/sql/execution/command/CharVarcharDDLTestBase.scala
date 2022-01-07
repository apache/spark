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

package org.apache.spark.sql.execution.command

import org.apache.spark.SparkConf
import org.apache.spark.sql.{AnalysisException, QueryTest, Row}
import org.apache.spark.sql.catalyst.util.CharVarcharUtils
import org.apache.spark.sql.connector.catalog.InMemoryPartitionTableCatalog
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.{SharedSparkSession, SQLTestUtils}
import org.apache.spark.sql.types._

trait CharVarcharDDLTestBase extends QueryTest with SQLTestUtils {

  def format: String

  def checkColType(f: StructField, dt: DataType): Unit = {
    assert(f.dataType == CharVarcharUtils.replaceCharVarcharWithString(dt))
    assert(CharVarcharUtils.getRawType(f.metadata).contains(dt))
  }

  test("allow to change column for char(x) to char(y), x == y") {
    withTable("t") {
      sql(s"CREATE TABLE t(i STRING, c CHAR(4)) USING $format")
      sql("ALTER TABLE t CHANGE COLUMN c TYPE CHAR(4)")
      checkColType(spark.table("t").schema(1), CharType(4))
    }
  }

  test("not allow to change column for char(x) to char(y), x != y") {
    withTable("t") {
      sql(s"CREATE TABLE t(i STRING, c CHAR(4)) USING $format")
      val e = intercept[AnalysisException] {
        sql("ALTER TABLE t CHANGE COLUMN c TYPE CHAR(5)")
      }
      val v1 = e.getMessage contains "'CharType(4)' to 'c' with type 'CharType(5)'"
      val v2 = e.getMessage contains "char(4) cannot be cast to char(5)"
      assert(v1 || v2)
    }
  }

  test("not allow to change column from string to char type") {
    withTable("t") {
      sql(s"CREATE TABLE t(i STRING, c STRING) USING $format")
      val e = intercept[AnalysisException] {
        sql("ALTER TABLE t CHANGE COLUMN c TYPE CHAR(5)")
      }
      val v1 = e.getMessage contains "'StringType' to 'c' with type 'CharType(5)'"
      val v2 = e.getMessage contains "string cannot be cast to char(5)"
      assert(v1 || v2)
    }
  }

  test("not allow to change column from int to char type") {
    withTable("t") {
      sql(s"CREATE TABLE t(i int, c CHAR(4)) USING $format")
      val e = intercept[AnalysisException] {
        sql("ALTER TABLE t CHANGE COLUMN i TYPE CHAR(5)")
      }
      val v1 = e.getMessage contains "'IntegerType' to 'i' with type 'CharType(5)'"
      val v2 = e.getMessage contains "int cannot be cast to char(5)"
      assert(v1 || v2)
    }
  }

  test("allow to change column for varchar(x) to varchar(y), x == y") {
    withTable("t") {
      sql(s"CREATE TABLE t(i STRING, c VARCHAR(4)) USING $format")
      sql("ALTER TABLE t CHANGE COLUMN c TYPE VARCHAR(4)")
      checkColType(spark.table("t").schema(1), VarcharType(4))
    }
  }

  test("not allow to change column for varchar(x) to varchar(y), x > y") {
    withTable("t") {
      sql(s"CREATE TABLE t(i STRING, c VARCHAR(4)) USING $format")
      val e = intercept[AnalysisException] {
        sql("ALTER TABLE t CHANGE COLUMN c TYPE VARCHAR(3)")
      }
      val v1 = e.getMessage contains "'VarcharType(4)' to 'c' with type 'VarcharType(3)'"
      val v2 = e.getMessage contains "varchar(4) cannot be cast to varchar(3)"
      assert(v1 || v2)
    }
  }

  def checkTableSchemaTypeStr(table: String, expected: Seq[Row]): Unit = {
    checkAnswer(
      sql(s"desc $table").selectExpr("data_type").where("data_type like '%char%'"),
      expected)
  }

  test("SPARK-33901: alter table add columns should not change original table's schema") {
    withTable("t") {
      sql(s"CREATE TABLE t(i CHAR(5), c VARCHAR(4)) USING $format")
      sql("ALTER TABLE t ADD COLUMNS (d VARCHAR(5))")
      checkTableSchemaTypeStr("t", Seq(Row("char(5)"), Row("varchar(4)"), Row("varchar(5)")))
    }
  }

  test("SPARK-33901: ctas should should not change table's schema") {
    withTable("t1", "t2") {
      sql(s"CREATE TABLE t1(i CHAR(5), c VARCHAR(4)) USING $format")
      sql(s"CREATE TABLE t2 USING $format AS SELECT * FROM t1")
      checkTableSchemaTypeStr("t2", Seq(Row("char(5)"), Row("varchar(4)")))
    }
  }

  test("SPARK-37160: CREATE TABLE with CHAR_AS_VARCHAR") {
    withSQLConf(SQLConf.CHAR_AS_VARCHAR.key -> "true") {
      withTable("t") {
        sql(s"CREATE TABLE t(col CHAR(5)) USING $format")
        checkTableSchemaTypeStr("t", Seq(Row("varchar(5)")))
      }
    }
  }

  test("SPARK-37160: CREATE TABLE AS SELECT with CHAR_AS_VARCHAR") {
    withTable("t1", "t2") {
      sql(s"CREATE TABLE t1(col CHAR(5)) USING $format")
      checkTableSchemaTypeStr("t1", Seq(Row("char(5)")))
      withSQLConf(SQLConf.CHAR_AS_VARCHAR.key -> "true") {
        sql(s"CREATE TABLE t2 USING $format AS SELECT * FROM t1")
        checkTableSchemaTypeStr("t2", Seq(Row("varchar(5)")))
      }
    }
  }

  test("SPARK-37160: ALTER TABLE ADD COLUMN with CHAR_AS_VARCHAR") {
    withTable("t") {
      sql(s"CREATE TABLE t(col CHAR(5)) USING $format")
      checkTableSchemaTypeStr("t", Seq(Row("char(5)")))
      withSQLConf(SQLConf.CHAR_AS_VARCHAR.key -> "true") {
        sql("ALTER TABLE t ADD COLUMN c2 CHAR(10)")
        checkTableSchemaTypeStr("t", Seq(Row("char(5)"), Row("varchar(10)")))
      }
    }
  }

  test("SPARK-33892: DESCRIBE COLUMN w/ char/varchar") {
    withTable("t") {
      sql(s"CREATE TABLE t(v VARCHAR(3), c CHAR(5)) USING $format")
      checkAnswer(sql("desc t v").selectExpr("info_value").where("info_value like '%char%'"),
        Row("varchar(3)"))
      checkAnswer(sql("desc t c").selectExpr("info_value").where("info_value like '%char%'"),
        Row("char(5)"))
    }
  }

  test("SPARK-33892: SHOW CREATE TABLE w/ char/varchar") {
    withTable("t") {
      sql(s"CREATE TABLE t(v VARCHAR(3), c CHAR(5)) USING $format")
      val rest = sql("SHOW CREATE TABLE t").head().getString(0)
      assert(rest.contains("VARCHAR(3)"))
      assert(rest.contains("CHAR(5)"))
    }
  }
}

class FileSourceCharVarcharDDLTestSuite extends CharVarcharDDLTestBase with SharedSparkSession {
  override def format: String = "parquet"
  override protected def sparkConf: SparkConf = {
    super.sparkConf.set(SQLConf.USE_V1_SOURCE_LIST, "parquet")
  }

  // TODO(SPARK-33902): MOVE TO SUPER CLASS AFTER THE TARGET TICKET RESOLVED
  test("SPARK-33901: create table like should should not change table's schema") {
    withTable("t", "tt") {
      sql(s"CREATE TABLE tt(i CHAR(5), c VARCHAR(4)) USING $format")
      sql("CREATE TABLE t LIKE tt")
      checkTableSchemaTypeStr("t", Seq(Row("char(5)"), Row("varchar(4)")))
    }
  }

  // TODO(SPARK-33903): MOVE TO SUPER CLASS AFTER THE TARGET TICKET RESOLVED
  test("SPARK-33901: cvas should should not change view's schema") {
    withTable( "tt") {
      sql(s"CREATE TABLE tt(i CHAR(5), c VARCHAR(4)) USING $format")
      withView("t") {
        sql("CREATE VIEW t AS SELECT * FROM tt")
        checkTableSchemaTypeStr("t", Seq(Row("char(5)"), Row("varchar(4)")))
      }
    }
  }

  // TODO(SPARK-33902): MOVE TO SUPER CLASS AFTER THE TARGET TICKET RESOLVED
  test("SPARK-37160: CREATE TABLE LIKE with CHAR_AS_VARCHAR") {
    withTable("t1", "t2") {
      sql(s"CREATE TABLE t1(col CHAR(5)) USING $format")
      checkTableSchemaTypeStr("t1", Seq(Row("char(5)")))
      withSQLConf(SQLConf.CHAR_AS_VARCHAR.key -> "true") {
        sql(s"CREATE TABLE t2 LIKE t1")
        checkTableSchemaTypeStr("t2", Seq(Row("varchar(5)")))
      }
    }
  }
}

class DSV2CharVarcharDDLTestSuite extends CharVarcharDDLTestBase
  with SharedSparkSession {
  override def format: String = "foo"
  protected override def sparkConf = {
    super.sparkConf
      .set("spark.sql.catalog.testcat", classOf[InMemoryPartitionTableCatalog].getName)
      .set(SQLConf.DEFAULT_CATALOG.key, "testcat")
  }

  test("allow to change change column from char to string type") {
    withTable("t") {
      sql(s"CREATE TABLE t(i STRING, c CHAR(4)) USING $format")
      sql("ALTER TABLE t CHANGE COLUMN c TYPE STRING")
      assert(spark.table("t").schema(1).dataType === StringType)
    }
  }

  test("allow to change column from char(x) to varchar(y) type x <= y") {
    withTable("t") {
      sql(s"CREATE TABLE t(i STRING, c CHAR(4)) USING $format")
      sql("ALTER TABLE t CHANGE COLUMN c TYPE VARCHAR(4)")
      checkColType(spark.table("t").schema(1), VarcharType(4))
    }
    withTable("t") {
      sql(s"CREATE TABLE t(i STRING, c CHAR(4)) USING $format")
      sql("ALTER TABLE t CHANGE COLUMN c TYPE VARCHAR(5)")
      checkColType(spark.table("t").schema(1), VarcharType(5))
    }
  }

  test("allow to change column from varchar(x) to varchar(y) type x <= y") {
    withTable("t") {
      sql(s"CREATE TABLE t(i STRING, c VARCHAR(4)) USING $format")
      sql("ALTER TABLE t CHANGE COLUMN c TYPE VARCHAR(4)")
      checkColType(spark.table("t").schema(1), VarcharType(4))
      sql("ALTER TABLE t CHANGE COLUMN c TYPE VARCHAR(5)")
      checkColType(spark.table("t").schema(1), VarcharType(5))

    }
  }

  test("not allow to change column from char(x) to varchar(y) type x > y") {
    withTable("t") {
      sql(s"CREATE TABLE t(i STRING, c CHAR(4)) USING $format")
      val e = intercept[AnalysisException] {
        sql("ALTER TABLE t CHANGE COLUMN c TYPE VARCHAR(3)")
      }
      assert(e.getMessage contains "char(4) cannot be cast to varchar(3)")
    }
  }

  test("SPARK-37160: REPLACE TABLE with CHAR_AS_VARCHAR") {
    withSQLConf(SQLConf.CHAR_AS_VARCHAR.key -> "true") {
      withTable("t") {
        sql(s"CREATE TABLE t(col INT) USING $format")
        sql(s"REPLACE TABLE t(col CHAR(5)) USING $format")
        checkTableSchemaTypeStr("t", Seq(Row("varchar(5)")))
      }
    }
  }

  test("SPARK-37160: REPLACE TABLE AS SELECT with CHAR_AS_VARCHAR") {
    withTable("t1", "t2") {
      sql(s"CREATE TABLE t1(col CHAR(5)) USING $format")
      checkTableSchemaTypeStr("t1", Seq(Row("char(5)")))
      withSQLConf(SQLConf.CHAR_AS_VARCHAR.key -> "true") {
        sql(s"CREATE TABLE t2(col INT) USING $format")
        sql(s"REPLACE TABLE t2 AS SELECT * FROM t1")
        checkTableSchemaTypeStr("t2", Seq(Row("varchar(5)")))
      }
    }
  }

  test("SPARK-37160: ALTER TABLE ALTER/REPLACE COLUMN with CHAR_AS_VARCHAR") {
    withTable("t") {
      sql(s"CREATE TABLE t(col CHAR(5), c2 VARCHAR(10)) USING $format")
      checkTableSchemaTypeStr("t", Seq(Row("char(5)"), Row("varchar(10)")))
      withSQLConf(SQLConf.CHAR_AS_VARCHAR.key -> "true") {
        sql("ALTER TABLE t ALTER c2 TYPE CHAR(20)")
        checkTableSchemaTypeStr("t", Seq(Row("char(5)"), Row("varchar(20)")))

        sql("ALTER TABLE t REPLACE COLUMNS (col CHAR(5))")
        checkTableSchemaTypeStr("t", Seq(Row("varchar(5)")))
      }
    }
  }
}
