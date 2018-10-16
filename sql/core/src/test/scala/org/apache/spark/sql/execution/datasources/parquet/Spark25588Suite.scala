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

package org.apache.spark.sql.execution.datasources.parquet

import com.google.common.collect.Lists
import org.apache.avro.Schema
import org.apache.hadoop.mapreduce.Job
import org.apache.parquet.avro.{AvroParquetInputFormat, AvroReadSupport}
import org.apache.parquet.hadoop.ParquetInputFormat
import org.apache.parquet.hadoop.util.ContextUtil

import org.apache.spark.sql.{QueryTest, SQLContext}
import org.apache.spark.sql.test.SharedSQLContext

private [parquet] case class Inner(
  names: Seq[String] = Seq())

private [parquet] case class Middle(
  inners: Seq[Inner] = Seq())

private [parquet] case class Outer(
  middle: Option[Middle] = None)

class Spark25588Suite extends QueryTest with SharedSQLContext {
  import testImplicits._

  test("SPARK-25588 Write Dataset out as Parquet read in as RDD fails") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath

      val inner = Inner(Seq("name0", "name1"))
      val middle = Middle(Seq(inner))
      val outer = Outer(Some(middle))
      val dataset = spark.sparkContext.parallelize(Seq(outer)).toDS()

      // write out from dataset to parquet
      dataset.toDF().write.format("parquet").save(path)

      // read parquet in through SQL works ok
      val roundtrip = spark.read.parquet(path).as[Outer]
      assert(roundtrip.first != null)

      // read parquet in as RDD fails
      val job = Job.getInstance(spark.sessionState.newHadoopConf())
      val conf = ContextUtil.getConfiguration(job)
      ParquetInputFormat.setReadSupportClass(job, classOf[AvroReadSupport[Outer]])

      val innerSchema = Schema.createRecord("Inner", null, null, false)
      innerSchema.setFields(
        Lists.newArrayList(
          new Schema.Field("names",
            Schema.createArray(Schema.create(Schema.Type.STRING)),
            null,
            null
          )
        )
      )

      val middleSchema = Schema.createRecord("Middle", null, null, false)
      middleSchema.setFields(
        Lists.newArrayList(
          new Schema.Field("inners",
            Schema.createArray(innerSchema),
            null,
            null
          )
        )
      )

      val outerSchema = Schema.createRecord("Outer", null, null, false)
      outerSchema.setFields(
        Lists.newArrayList(
          new Schema.Field("middle",
            Schema.createUnion(
              Lists.newArrayList(Schema.create(Schema.Type.NULL), middleSchema)),
            null,
            null
          )
        )
      )

      AvroParquetInputFormat.setAvroReadSchema(job, outerSchema)

      val records = spark.sparkContext.newAPIHadoopFile(path,
        classOf[ParquetInputFormat[Outer]],
        classOf[Void],
        classOf[Outer],
        conf
      )

      assert(records.first != null)
    }
  }
}
