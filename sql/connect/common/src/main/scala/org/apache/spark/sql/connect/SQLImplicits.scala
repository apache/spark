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
package org.apache.spark.sql.connect

import scala.language.implicitConversions

import org.apache.spark.rdd.RDD
import org.apache.spark.sql
import org.apache.spark.sql.Encoder

/** @inheritdoc */
abstract class SQLImplicits private[sql] (override val session: SparkSession)
    extends sql.SQLImplicits {

  override implicit def localSeqToDatasetHolder[T: Encoder](s: Seq[T]): DatasetHolder[T] =
    new DatasetHolder[T](session.createDataset(s))

  override implicit def rddToDatasetHolder[T: Encoder](rdd: RDD[T]): DatasetHolder[T] =
    new DatasetHolder[T](session.createDataset(rdd))
}

class DatasetHolder[U](ds: Dataset[U]) extends sql.DatasetHolder[U] {
  override def toDS(): Dataset[U] = ds
  override def toDF(): DataFrame = ds.toDF()
  override def toDF(colNames: String*): DataFrame = ds.toDF(colNames: _*)
}
