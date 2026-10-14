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

package org.apache.spark.sql.pipelines.autocdc

import org.apache.spark.sql.{functions => F, Column, QueryTest, Row}
import org.apache.spark.sql.classic.DataFrame
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.LongType

class Scd1BatchProcessorSuite extends QueryTest with SharedSparkSession {

  private def versionMapOf(entries: (Seq[String], Option[Long])*): Column =
    F.map(entries.flatMap { case (leafPath, sequence) =>
      Seq(
        F.lit(Scd1VersionMap.serializeKey(leafPath)),
        sequence.map(value => F.lit(value)).getOrElse(F.lit(null)))
    }: _*)

  private def lastAuthoredIn(
      leafPaths: Seq[Seq[String]],
      deleteSequence: Column = F.lit(null),
      upsertSequence: Column = F.lit(null),
      versionMap: Column = F.lit(null)): DataFrame = {
    val cdcMetadata = Scd1BatchProcessor.constructCdcMetadataCol(
      deleteSequence = deleteSequence,
      upsertSequence = upsertSequence,
      versionMap = versionMap,
      sequencingType = LongType)
    spark.range(1).select(Scd1BatchProcessor.lastAuthoredIn(cdcMetadata, leafPaths))
  }

  test("lastAuthoredIn is a delete's sequence, whichever leaves it reads") {
    checkAnswer(lastAuthoredIn(Seq(Seq("a")), deleteSequence = F.lit(9L)), Row(9L))
    checkAnswer(lastAuthoredIn(Seq.empty, deleteSequence = F.lit(9L)), Row(9L))
  }

  test("lastAuthoredIn is an upsert's sequence when the upsert has no version map") {
    checkAnswer(lastAuthoredIn(Seq(Seq("a")), upsertSequence = F.lit(5L)), Row(5L))
  }

  test("lastAuthoredIn is the greatest sequence among the leaves it reads") {
    val versionMap = versionMapOf(Seq("a") -> Some(3L), Seq("b") -> Some(7L), Seq("c") -> Some(9L))

    checkAnswer(
      lastAuthoredIn(Seq(Seq("a"), Seq("b")), upsertSequence = F.lit(9L), versionMap = versionMap),
      Row(7L))
  }

  test("lastAuthoredIn skips leaves that the version map records as unauthored") {
    val versionMap = versionMapOf(Seq("a") -> None, Seq("b") -> Some(4L))

    checkAnswer(
      lastAuthoredIn(Seq(Seq("a"), Seq("b")), upsertSequence = F.lit(4L), versionMap = versionMap),
      Row(4L))
  }

  test("lastAuthoredIn is null when the version map authors none of the leaves it reads") {
    val versionMap = versionMapOf(Seq("a") -> None, Seq("b") -> Some(4L))

    checkAnswer(
      lastAuthoredIn(Seq(Seq("a")), upsertSequence = F.lit(4L), versionMap = versionMap),
      Row(null))
    checkAnswer(
      lastAuthoredIn(Seq(Seq("missing")), upsertSequence = F.lit(4L), versionMap = versionMap),
      Row(null))
  }

  test("lastAuthoredIn is an upsert's sequence when it reads no leaves") {
    val versionMap = versionMapOf(Seq("a") -> Some(3L))

    checkAnswer(
      lastAuthoredIn(Seq.empty, upsertSequence = F.lit(5L), versionMap = versionMap),
      Row(5L))
  }

  test("lastAuthoredIn reads a nested leaf by its full path") {
    val versionMap = versionMapOf(
      Seq("profile", "city") -> Some(3L),
      Seq("profile.city") -> Some(8L))
    def lastAuthoredLeaf(leafPath: String*): DataFrame =
      lastAuthoredIn(Seq(leafPath), upsertSequence = F.lit(8L), versionMap = versionMap)

    checkAnswer(lastAuthoredLeaf("profile", "city"), Row(3L))
    checkAnswer(lastAuthoredLeaf("profile.city"), Row(8L))
  }
}
