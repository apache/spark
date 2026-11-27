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

package org.apache.spark.graph

import org.apache.spark.graph.GraphFrame.{DST, ID, SRC}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.graph.CacheTestUtils
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.util.Utils

trait GraphFrameTestBase extends SharedSparkSession {

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.sparkContext.setCheckpointDir(Utils.createTempDir().getCanonicalPath)
  }

  // A helper for checking possible memory leaks;
  // use it on all the methods that are persisting anythig internally.
  protected def assertNoPersistedDataLeak[T](body: => T): T = {
    val rddIdsBefore = spark.sparkContext.getPersistentRDDs.keySet
    val cachedEntriesBefore = CacheTestUtils.numCachedEntries(spark)
    val result = body
    val leakedRdds = spark.sparkContext.getPersistentRDDs
      .filter { case (id, _) => !rddIdsBefore.contains(id) }
      .values
      .toSeq
      .sortBy(_.id)
    val leakedCachedEntries = CacheTestUtils.numCachedEntries(spark) - cachedEntriesBefore
    assert(
      leakedRdds.isEmpty,
      s"${leakedRdds.size} persisted RDD(s) were not released: " +
        leakedRdds.map(rdd => s"$rdd (${rdd.getStorageLevel.description})").mkString(", "))
    assert(
      leakedCachedEntries <= 0,
      s"$leakedCachedEntries persisted Dataset(s) were not released.")
    result
  }

  protected def checkColumnType(schema: StructType, colName: String, dataType: DataType): Unit = {
    val actualDataType = schema(colName).dataType
    assert(
      actualDataType == dataType,
      s"Column $colName must be of type $dataType but was actually $actualDataType.")
  }

  protected def checkSchemaInvariant(g: GraphFrame): Unit = {
    assert(g.vertices.columns.contains(ID))
    assert(g.edges.columns.contains(SRC))
    assert(g.edges.columns.contains(DST))
  }

  protected def checkSchemaInvariants(before: GraphFrame, after: GraphFrame): Unit = {
    checkSchemaInvariant(before)
    checkSchemaInvariant(after)
    assert(after.vertices.schema(ID).dataType == before.vertices.schema(ID).dataType)
    Seq(SRC, DST).foreach { colName =>
      assert(after.edges.schema(colName).dataType == before.edges.schema(colName).dataType)
    }
    before.vertices.schema.foreach { f =>
      assert(
        after.vertices.columns.contains(f.name) && after.vertices.schema(f.name) == f,
        s"${before.vertices.schema} is not a subset of ${after.vertices.schema}")
    }
    before.edges.schema.foreach { f =>
      assert(
        after.edges.columns.contains(f.name) &&
          after.edges.schema(f.name).dataType == f.dataType,
        s"${before.edges.schema} is not a subset of ${after.edges.schema}")
    }
  }

  protected def checkSchemaInvariants(before: GraphFrame, afterVertices: DataFrame): Unit = {
    checkSchemaInvariants(before, GraphFrame(afterVertices, before.edges))
  }
}
