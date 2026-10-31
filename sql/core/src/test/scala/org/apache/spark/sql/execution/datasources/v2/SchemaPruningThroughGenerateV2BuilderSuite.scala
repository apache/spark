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

package org.apache.spark.sql.execution.datasources.v2

import java.util
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.{sources, QueryTest, Row}
import org.apache.spark.sql.connector.catalog.{SupportsRead, Table, TableCapability}
import org.apache.spark.sql.connector.read.{Scan, ScanBuilder, SupportsPushDownRequiredColumns}
import org.apache.spark.sql.execution.datasources.v2.parquet.{ParquetDataSourceV2, ParquetScan}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{ArrayType, DataType, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * Pruning nested fields through generators on a DataSource V2 scan whose builder only narrows:
 * a later `pruneColumns` call cannot widen what an earlier one kept, as in Iceberg.
 */
class SchemaPruningThroughGenerateV2BuilderSuite extends QueryTest with SharedSparkSession {

  test("SPARK-47230: a rejected rewrite does not prune a narrow-only builder again") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      sql(
        """select id, array(
          |  named_struct('a', id, 'b', id * 2, 'c', id * 3, 's', cast(id * 4 as int)),
          |  named_struct('a', id * 10, 'b', id * 20, 'c', id * 30, 's', cast(id * 40 as int))
          |) as items from range(1, 3)""".stripMargin).write.parquet(path)
      val query = "select item.a, item.c from t lateral view explode(items) e as item"
      // V1 Parquet as the baseline
      var expected: Seq[Row] = Nil
      withTempView("t") {
        spark.read.parquet(path).createOrReplaceTempView("t")
        expected = sql(query).collect().toSeq
      }
      assert(expected.size == 4)

      Seq(false, true).foreach { reject =>
        withSQLConf(
            SQLConf.NESTED_SCHEMA_PRUNING_THROUGH_GENERATE_REJECT_FOR_TESTING.key ->
              reject.toString) {
          NarrowOnlyParquetSource.pruneCalls.clear()
          withTempView("t") {
            spark.read.format(classOf[NarrowOnlyParquetSource].getName).load(path)
              .createOrReplaceTempView("t")
            checkAnswer(sql(query), expected)
          }
          val calls = NarrowOnlyParquetSource.pruneCalls.map(_.get).toSeq
          assert(calls.nonEmpty)
          assert(calls.forall(_ <= 1), s"pruneColumns calls per builder: $calls")
          // a rejected rewrite plans the scan from a second, fresh builder
          if (reject) assert(calls.count(_ == 1) >= 2, s"pruneColumns calls per builder: $calls")
        }
      }
    }
  }

  test("SPARK-47230: a rejected rewrite offers the data filters to the fresh builder") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      // a fully handled partition predicate and a partially pushed data predicate
      sql("select id as n, id % 2 as p, " +
        "array(named_struct('a', id, 'b', id * 2, 'c', id * 3)) as items from range(10)")
        .write.partitionBy("p").parquet(path)
      withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> "") {
        Seq(false, true).foreach { reject =>
          withSQLConf(
              SQLConf.NESTED_SCHEMA_PRUNING_THROUGH_GENERATE_REJECT_FOR_TESTING.key ->
                reject.toString) {
            withTempView("t") {
              spark.read.parquet(path).createOrReplaceTempView("t")
              val query = sql("select item.a, item.c from t " +
                "lateral view outer explode(items) e as item where p = 1 and n = 7")
              val pushed = query.queryExecution.optimizedPlan.collect {
                case r: DataSourceV2ScanRelation => r.scan
              }.collect { case s: ParquetScan => s.pushedFilters.toSeq }.flatten
              assert(pushed.exists {
                case sources.EqualTo("n", v) => v.toString == "7"
                case _ => false
              }, s"reject=$reject: $pushed")
              checkAnswer(query, Row(7, 21) :: Nil)
            }
          }
        }
      }
    }
  }

  test("SPARK-47230: a rejected rewrite keeps the filters above the scan apart") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      sql("select id as n, if(id = 7, '7', 'bad') as s, " +
        "array(named_struct('a', id, 'b', id * 2, 'c', id * 3)) as items from range(7, 9)")
        .write.parquet(path)
      withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> "", SQLConf.ANSI_ENABLED.key -> "true") {
        Seq(false, true).foreach { reject =>
          withSQLConf(
              SQLConf.NESTED_SCHEMA_PRUNING_THROUGH_GENERATE_REJECT_FOR_TESTING.key ->
                reject.toString) {
            withTempView("t") {
              // the nondeterministic lower filter is NULL for s = 'bad', so the cast above it
              // must never see that row: `And` of the two would still evaluate the cast
              // (`monotonically_increasing_id`, unlike `rand() >= 0`, is not folded away)
              spark.read.parquet(path)
                .where("monotonically_increasing_id() >= 0 and nullif(s, 'bad') = s")
                .where("cast(s as int) = 7")
                .createOrReplaceTempView("t")
              checkAnswer(
                sql("select item.a, item.c from t lateral view explode(items) e as item"),
                Row(7, 21) :: Nil)
            }
          }
        }
      }
    }
  }
}

/** Parquet through DataSource V2, with a scan builder whose `pruneColumns` only narrows. */
class NarrowOnlyParquetSource extends ParquetDataSourceV2 {
  override def shortName(): String = "narrow-only-parquet"

  override def getTable(options: CaseInsensitiveStringMap): Table =
    new NarrowOnlyTable(super.getTable(options))

  override def getTable(options: CaseInsensitiveStringMap, schema: StructType): Table =
    new NarrowOnlyTable(super.getTable(options, schema))
}

object NarrowOnlyParquetSource {
  /** The number of `pruneColumns` calls on each builder created, in creation order. */
  val pruneCalls: ArrayBuffer[AtomicInteger] = ArrayBuffer.empty

  /** The fields of `kept` that `required` also has, recursively. */
  def narrow(kept: DataType, required: DataType): DataType = (kept, required) match {
    case (k: StructType, r: StructType) =>
      StructType(k.fields.flatMap { field =>
        r.find(_.name == field.name).map(f => field.copy(dataType = narrow(field.dataType,
          f.dataType)))
      })
    case (ArrayType(k, containsNull), ArrayType(r, _)) => ArrayType(narrow(k, r), containsNull)
    case (k, _) => k
  }
}

class NarrowOnlyTable(table: Table) extends Table with SupportsRead {
  override def name(): String = table.name()
  override def schema(): StructType = table.schema()
  override def capabilities(): util.Set[TableCapability] =
    util.EnumSet.of(TableCapability.BATCH_READ)

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    val delegate = table.asInstanceOf[SupportsRead].newScanBuilder(options)
    val calls = new AtomicInteger()
    NarrowOnlyParquetSource.pruneCalls.synchronized {
      NarrowOnlyParquetSource.pruneCalls += calls
    }
    new ScanBuilder with SupportsPushDownRequiredColumns {
      private var kept: Option[StructType] = None

      override def pruneColumns(requiredSchema: StructType): Unit = {
        calls.incrementAndGet()
        val narrowed = kept.map(NarrowOnlyParquetSource.narrow(_, requiredSchema)
          .asInstanceOf[StructType]).getOrElse(requiredSchema)
        kept = Some(narrowed)
        delegate.asInstanceOf[SupportsPushDownRequiredColumns].pruneColumns(narrowed)
      }

      override def build(): Scan = delegate.build()
    }
  }
}
