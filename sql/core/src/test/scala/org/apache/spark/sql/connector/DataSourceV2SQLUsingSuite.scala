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

package org.apache.spark.sql.connector

import java.util

import scala.collection.JavaConverters._

import org.apache.spark.sql.{AnalysisException, QueryTest, Row}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.catalog.{Table, TableCapability, TableProvider}
import org.apache.spark.sql.connector.expressions.{LogicalExpressions, Transform}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.StructType

class DataSourceV2SQLUsingSuite extends QueryTest with SharedSparkSession {

  override def beforeEach(): Unit = {
    super.beforeEach()
    ReadWriteV2Source.table.clear()
    PartitionedV2Source.table.clear()
  }

  test("basic") {
    withTable("t") {
      sql(s"CREATE TABLE t USING ${classOf[ReadWriteV2Source].getName}")

      val e = intercept[AnalysisException](sql("INSERT INTO t SELECT 1"))
      assert(e.message.contains("not enough data columns"))

      sql("INSERT INTO t SELECT 1, -1")
      checkAnswer(spark.table("t"), Row(1, -1))
    }
  }

  test("CREATE TABLE with a mismatched schema") {
    withTable("t") {
      val e = intercept[AnalysisException](
        sql(s"CREATE TABLE t(i INT) USING ${classOf[ReadWriteV2Source].getName}")
      )
      assert(e.getMessage.contains("returns a table which has inappropriate schema"))

      sql(s"CREATE TABLE t(i INT, j INT) USING ${classOf[ReadWriteV2Source].getName}")
      sql("INSERT INTO t SELECT 1, -1")
      checkAnswer(spark.table("t"), Row(1, -1))
    }
  }

  test("CREATE TABLE with a mismatched partitioning") {
    withTable("t") {
      val e = intercept[AnalysisException](
        sql(
          s"""
            |CREATE TABLE t(i INT, j INT) USING ${classOf[PartitionedV2Source].getName}
            |PARTITIONED BY (i)
          """.stripMargin)
      )
      assert(e.getMessage.contains("returns a table which has inappropriate partitioning"))

      sql(
        s"""
           |CREATE TABLE t(i INT, j INT) USING ${classOf[PartitionedV2Source].getName}
           |PARTITIONED BY (j)
         """.stripMargin)
      sql("INSERT INTO t SELECT 1, -1")
      checkAnswer(spark.table("t"), Row(1, -1))
    }
  }

  test("read-only table") {
    withTable("t") {
      sql(s"CREATE TABLE t USING ${classOf[ReadOnlyV2Source].getName}")
      checkAnswer(spark.table("t"), Seq(Row(1), Row(-1)))

      val e1 = intercept[AnalysisException](sql("INSERT INTO t SELECT 1, -1"))
      assert(e1.message.contains("too many data columns"))

      val e2 = intercept[AnalysisException](sql("INSERT INTO t SELECT 1"))
      assert(e2.message.contains("does not support append in batch mode"))
    }
  }

  test("write-only table") {
    withTable("t") {
      sql(s"CREATE TABLE t USING ${classOf[WriteOnlyV2Source].getName}")

      val e1 = intercept[AnalysisException](sql("INSERT INTO t SELECT 1, 1"))
      assert(e1.message.contains("too many data columns"))

      sql("INSERT INTO t SELECT 1")

      val e2 = intercept[AnalysisException](sql("SELECT * FROM t").collect())
      assert(e2.message.contains("Table write-only does not support batch scan"))
    }
  }

  test("CREATE TABLE AS SELECT") {
    withTable("t") {
      sql(
        s"""
           |CREATE TABLE t USING ${classOf[ReadWriteV2Source].getName}
           |AS SELECT 1 AS i, -1 AS j
         """.stripMargin)
      checkAnswer(spark.table("t"), Row(1, -1))

      sql("INSERT INTO t SELECT 2, -2")
      checkAnswer(spark.table("t"), Seq(Row(1, -1), Row(2, -2)))
    }
  }

  test("INSERT OVERWRITE with non-partitioned table") {
    withTable("t") {
      sql(s"CREATE TABLE t USING ${classOf[ReadWriteV2Source].getName}")

      sql("INSERT INTO t SELECT 1, -1")
      checkAnswer(spark.table("t"), Row(1, -1))

      sql("INSERT OVERWRITE t SELECT 2, -2")
      checkAnswer(spark.table("t"), Row(2, -2))
    }
  }

  test("INSERT OVERWRITE with partitioned table (static mode)") {
    withTable("t") {
      sql(s"CREATE TABLE t USING ${classOf[PartitionedV2Source].getName}")
      sql("INSERT INTO t SELECT 1, 1")
      sql("INSERT INTO t SELECT 2, 1")
      sql("INSERT INTO t SELECT 3, 2")
      checkAnswer(spark.table("t"), Seq(Row(1, 1), Row(2, 1), Row(3, 2)))

      withSQLConf(SQLConf.PARTITION_OVERWRITE_MODE.key -> "static") {
        sql("INSERT OVERWRITE t PARTITION(j=2) SELECT 4")
        checkAnswer(spark.table("t"), Seq(Row(1, 1), Row(2, 1), Row(4, 2)))

        sql("INSERT OVERWRITE t SELECT 0, 1")
        checkAnswer(spark.table("t"), Row(0, 1))
      }
    }
  }

  test("INSERT OVERWRITE with partitioned table (dynamic mode)") {
    withTable("t") {
      sql(s"CREATE TABLE t USING ${classOf[PartitionedV2Source].getName}")
      sql("INSERT INTO t SELECT 1, 1")
      sql("INSERT INTO t SELECT 2, 1")
      sql("INSERT INTO t SELECT 3, 2")
      checkAnswer(spark.table("t"), Seq(Row(1, 1), Row(2, 1), Row(3, 2)))

      withSQLConf(SQLConf.PARTITION_OVERWRITE_MODE.key -> "dynamic") {
        sql("INSERT OVERWRITE t PARTITION(j=2) SELECT 4")
        checkAnswer(spark.table("t"), Seq(Row(1, 1), Row(2, 1), Row(4, 2)))

        sql("INSERT OVERWRITE t SELECT 0, 1")
        checkAnswer(spark.table("t"), Seq(Row(0, 1), Row(4, 2)))
      }
    }
  }

  // TODO: enable it when DELETE FROM supports v2 session catalog.
  ignore("DELETE FROM") {
    withTable("t") {
      sql(s"CREATE TABLE t USING ${classOf[PartitionedV2Source].getName}")
      sql("INSERT INTO t SELECT 1, 1")
      sql("INSERT INTO t SELECT 2, 1")
      sql("INSERT INTO t SELECT 3, 2")
      checkAnswer(spark.table("t"), Seq(Row(1, 1), Row(2, 1), Row(3, 2)))

      sql("DELETE FROM t WHERE j = 2")
      checkAnswer(spark.table("t"), Seq(Row(1, 1), Row(2, 1)))
    }
  }

  test("ALTER TABLE") {
    withTable("t") {
      sql(s"CREATE TABLE t USING ${classOf[ReadWriteV2Source].getName}")
      sql("INSERT INTO t SELECT 1, -1")
      checkAnswer(spark.table("t"), Row(1, -1))

      sql("ALTER TABLE t DROP COLUMN i")
      val e = intercept[AnalysisException](sql("SELECT * FROM t"))
      assert(e.message.contains("returns a table which has inappropriate schema"))
    }
  }

  test("ALTER TABLE with table supporting schema changing") {
    withTable("t") {
      sql(s"CREATE TABLE t(i INT) USING ${classOf[DynamicSchemaV2Source].getName}")
      checkAnswer(spark.table("t"), Nil)

      sql("ALTER TABLE t ADD COLUMN j INT")
      checkAnswer(spark.table("t"), Nil)
    }
  }
}

object ReadWriteV2Source {
  val table = {
    val schema = new StructType().add("i", "int").add("j", "int")
    val partitioning = Array.empty[Transform]
    val properties = util.Collections.emptyMap[String, String]
    new InMemoryTable("read-write", schema, partitioning, properties) {}
  }
}

class ReadWriteV2Source extends TableProvider {
  // `TableProvider` will be instantiated by reflection every time it's accessed. To keep the data
  // of in-memory table, we keep the table instance in an object.
  override def loadTable(properties: util.Map[String, String]): Table = {
    ReadWriteV2Source.table
  }
}

object PartitionedV2Source {
  val table = {
    val schema = new StructType().add("i", "int").add("j", "int")
    val partitioning = Array[Transform](LogicalExpressions.identity("j"))
    val properties = util.Collections.emptyMap[String, String]
    new InMemoryTable("read-write", schema, partitioning, properties) {}
  }
}

class PartitionedV2Source extends TableProvider {
  // `TableProvider` will be instantiated by reflection every time it's accessed. To keep the data
  // of in-memory table, we keep the table instance in an object.
  override def loadTable(properties: util.Map[String, String]): Table = {
    PartitionedV2Source.table
  }
}

class ReadOnlyV2Source extends TableProvider {
  override def loadTable(properties: util.Map[String, String]): Table = {
    val schema = new StructType().add("i", "int")
    val partitions = Array.empty[Transform]
    val properties = util.Collections.emptyMap[String, String]
    val table = new InMemoryTable("read-only", schema, partitions, properties) {
      override def capabilities: util.Set[TableCapability] = {
        Set(TableCapability.BATCH_READ).asJava
      }
    }
    val rows = new BufferedRows()
    rows.withRow(InternalRow(1)).withRow(InternalRow(-1))
    table.withData(Array(rows))
  }
}

class WriteOnlyV2Source extends TableProvider {
  override def loadTable(properties: util.Map[String, String]): Table = {
    val schema = new StructType().add("i", "int")
    val partitions = Array.empty[Transform]
    val properties = util.Collections.emptyMap[String, String]
    new InMemoryTable("write-only", schema, partitions, properties) {
      override def capabilities: util.Set[TableCapability] = {
        Set(TableCapability.BATCH_WRITE).asJava
      }
    }
  }
}

class DynamicSchemaV2Source extends TableProvider {
  override def loadTable(properties: util.Map[String, String]): Table = {
    new InMemoryTable("dynamic-schema", new StructType(), Array.empty, properties) {}
  }

  override def loadTable(
      schema: StructType,
      partitions: Array[Transform],
      properties: util.Map[String, String]): Table = {
    new InMemoryTable("dynamic-schema", schema, partitions, properties) {}
  }
}
