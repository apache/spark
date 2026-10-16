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

package org.apache.spark.sql.hive.client

import java.net.URI

import org.apache.hadoop.hive.metastore.api.{FieldSchema, Partition => HiveApiPartition}
import org.apache.hadoop.hive.metastore.api.{
  PartitionListComposingSpec,
  PartitionSpec,
  PartitionSpecWithSharedSD,
  PartitionWithoutSD,
  StorageDescriptor}

import org.apache.spark.{SparkFunSuite, SparkUnsupportedOperationException}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogUtils
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable, CatalogTableType}
import org.apache.spark.sql.hive.{HiveUtils, StaticInitFlags, StaticInitInputFormat, StaticInitOutputFormat}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType

class HiveClientImplSuite extends SparkFunSuite {

  test("SPARK-59330: toHiveTable skips the format class static initializer when " +
    "spark.sql.hive.initializeMetastoreFormatClasses is false") {
    // Both call sites are exercised: toHiveTable resolves the input format via toInputFormat and
    // the output format via toOutputFormat, so each format class has its own flag.
    val table = CatalogTable(
      identifier = TableIdentifier("t", Some("default")),
      tableType = CatalogTableType.MANAGED,
      storage = CatalogStorageFormat.empty.copy(
        inputFormat = Some(classOf[StaticInitInputFormat].getName),
        outputFormat = Some(classOf[StaticInitOutputFormat].getName)),
      schema = new StructType().add("a", "int"))

    def toHiveTableWith(initialize: Boolean): Unit = {
      val conf = new SQLConf()
      conf.setConf(HiveUtils.INITIALIZE_METASTORE_FORMAT_CLASSES, initialize)
      SQLConf.withExistingConf(conf) {
        HiveClientImpl.toHiveTable(table)
      }
    }

    // The false half must run first: class initialization is one-way per JVM. Resolving the
    // format class names without initializing them must not run their static initializers.
    toHiveTableWith(initialize = false)
    assert(!StaticInitFlags.inputFormatInitialized)
    assert(!StaticInitFlags.outputFormatInitialized)

    // With initialization enabled (the default), resolving the class names runs the initializers.
    toHiveTableWith(initialize = true)
    assert(StaticInitFlags.inputFormatInitialized)
    assert(StaticInitFlags.outputFormatInitialized)
  }

  test("SPARK-59330: toHiveTable still resolves the format class when " +
    "spark.sql.hive.initializeMetastoreFormatClasses is false") {
    // false skips the static initializer but still loads the class, so a missing class fails here
    // rather than being silently deferred; this pins that contract (the reason for initialize =
    // false over dropping the resolution entirely).
    val table = CatalogTable(
      identifier = TableIdentifier("t", Some("default")),
      tableType = CatalogTableType.MANAGED,
      storage = CatalogStorageFormat.empty.copy(
        inputFormat = Some("org.apache.spark.sql.hive.DoesNotExistInputFormat")),
      schema = new StructType().add("a", "int"))
    val conf = new SQLConf()
    conf.setConf(HiveUtils.INITIALIZE_METASTORE_FORMAT_CLASSES, false)
    intercept[ClassNotFoundException] {
      SQLConf.withExistingConf(conf) {
        HiveClientImpl.toHiveTable(table)
      }
    }
  }

  test("SPARK-21529: a clear error is raised for an unsupported Hive union type") {
    val column = new FieldSchema("c", "uniontype<int,string>", null)
    checkError(
      exception = intercept[SparkUnsupportedOperationException] {
        HiveClientImpl.fromHiveColumn(column)
      },
      condition = "UNSUPPORTED_HIVE_TYPE",
      parameters = Map(
        "fieldType" -> "\"UNIONTYPE<INT,STRING>\"",
        "fieldName" -> "`c`"))
  }

  test("SPARK-21529: a Hive union type nested in a struct is detected") {
    val column = new FieldSchema("c", "struct<a:uniontype<int,string>>", null)
    checkError(
      exception = intercept[SparkUnsupportedOperationException] {
        HiveClientImpl.fromHiveColumn(column)
      },
      condition = "UNSUPPORTED_HIVE_TYPE",
      parameters = Map(
        "fieldType" -> "\"STRUCT<A:UNIONTYPE<INT,STRING>>\"",
        "fieldName" -> "`c`"))
  }

  test("Shim_v4_0 getPartitionLocations returns empty on unpartitioned table") {
    // The unpartitioned guard must return before any session config or HMS call is needed.
    val shim = new Shim_v4_0()
    val table = new org.apache.hadoop.hive.ql.metadata.Table("default", "t")
    assert(shim.getPartitionLocations(null, table, new java.util.HashMap()).isEmpty)
  }

  test("Shim_v4_0 normalizeLocation constructs expected URI across root and relPath shapes") {
    val shim = new Shim_v4_0()
    val root = "file:/data/warehouse/tbl"

    // Normal nested partition: the metastore computes relativePath as
    // location.substring(tablePath.length()), so a true child partition's relativePath always
    // carries the leading separator. normalizeLocation must not insert a second one.
    assert(shim.normalizeLocation(root, "/p1=1/p2=2") ===
      Some(new URI("file:/data/warehouse/tbl/p1=1/p2=2")))

    // Sibling directory sharing the table path as a string prefix (e.g. "<tablePath>_external/"):
    // the metastore's substring-based relativePath computation is not path-boundary aware, so it
    // has no leading separator here. normalizeLocation must reproduce the location verbatim via
    // concatenation, not insert a separator that was never there.
    assert(shim.normalizeLocation(root, "_external/p=1") ===
      Some(new URI("file:/data/warehouse/tbl_external/p=1")))

    // Empty relative path means partition sits directly at the table location
    assert(shim.normalizeLocation(root, "") === Some(new URI(root)))

    // A null relative path means the metastore reported no location for this partition
    // (either the storage descriptor or its location field was unset). No location is
    // returned, mirroring what the full-fetch path reports for such partitions and
    // avoiding any fabricated default partition path.
    assert(shim.normalizeLocation(root, null) === None)

    // Null root with non-empty relative path
    assert(shim.normalizeLocation(null, "file:/custom/loc") ===
      Some(new URI("file:/custom/loc")))

    // Both root and relative path null
    assert(shim.normalizeLocation(null, null) === None)
  }

  test("Shim_v4_0 buildPartitionSpec constructs partition spec") {
    val shim = new Shim_v4_0()

    // Values are matched to partition columns positionally.
    val spec = shim.buildPartitionSpec(Array("p1", "p2", "p3"),
      java.util.Arrays.asList("a", "b", "c"))
    assert(spec === Map("p1" -> "a", "p2" -> "b", "p3" -> "c"))

    // Defensive: a values list shorter or longer than the partition columns truncates
    // instead of throwing or misaligning.
    assert(shim.buildPartitionSpec(Array("p1", "p2"),
      java.util.Arrays.asList("only")) === Map("p1" -> "only"))
    assert(shim.buildPartitionSpec(Array("p1"),
      java.util.Arrays.asList("a", "extra")) === Map("p1" -> "a"))
  }

  test("Shim_v4_0 buildPartitionLocations matches the full-fetch location derivation " +
      "when the partition specs carry no location") {
    val shim = new Shim_v4_0()

    // How the full-fetch path (Shim_v2_0.getPartitionLocations, backed by the classic
    // getPartitions RPC) derives a partition location: one is present only if the storage
    // descriptor exists and carries one. The projection path must report exactly this.
    def fullFetchLocation(sd: StorageDescriptor): Option[URI] =
      Option(sd).flatMap(realSd => Option(realSd.getLocation)).map(CatalogUtils.stringToURI)

    def locationSd(location: String): StorageDescriptor = {
      val sd = new StorageDescriptor()
      sd.setLocation(location)
      sd
    }

    def hiveTable(tableSd: StorageDescriptor): org.apache.hadoop.hive.ql.metadata.Table = {
      val table = new org.apache.hadoop.hive.ql.metadata.Table("default", "t")
      table.setPartCols(java.util.Arrays.asList(new FieldSchema("p1", "string", "")))
      if (tableSd != null) {
        table.getTTable.setSd(tableSd)
      }
      table
    }

    def sharedSpec(rootPath: Option[String], relPath: String, value: String): PartitionSpec = {
      val group = new PartitionSpec()
      rootPath.foreach(group.setRootPath)
      val shared = new PartitionSpecWithSharedSD()
      val partition = new PartitionWithoutSD()
      partition.setValues(java.util.Arrays.asList(value))
      partition.setRelativePath(relPath)
      shared.setPartitions(java.util.Arrays.asList(partition))
      group.setSharedSDPartitionSpec(shared)
      group
    }

    def listSpec(sd: StorageDescriptor, value: String): PartitionSpec = {
      val partition = new HiveApiPartition()
      partition.setValues(java.util.Arrays.asList(value))
      partition.setSd(sd)
      val list = new PartitionListComposingSpec()
      list.setPartitions(java.util.Arrays.asList(partition))
      val group = new PartitionSpec()
      group.setPartitionList(list)
      group
    }

    val tableLocation = "file:/data/warehouse/tbl"

    // HMS leaves a PartitionWithoutSD's relative path null exactly when the partition's
    // storage descriptor carries no location, and leaves the group's root path null when
    // it groups partitions that have no storage descriptor at all. For both shapes the
    // projection path must report no location, like the full-fetch derivation above,
    // rather than fabricating a default partition path.
    val sharedCases: Seq[(Option[String], String, Option[URI])] = Seq(
      // Control: in-table partition, root plus relative path reconstructs its location
      (Some(tableLocation), "/p1=1", fullFetchLocation(locationSd(tableLocation + "/p1=1"))),
      // Storage descriptor present, its location unset
      (Some(tableLocation), null, fullFetchLocation(null)),
      // No storage descriptor: group root path unset, relative path null
      (None, null, fullFetchLocation(null)))
    sharedCases.zipWithIndex.foreach { case ((rootPath, relPath, expected), idx) =>
      val value = (idx + 1).toString
      val results = shim.buildPartitionLocations(
        hiveTable(locationSd(tableLocation)), Seq(sharedSpec(rootPath, relPath, value)))
      assert(results.size == 1)
      assert(results.head.spec == Map("p1" -> value))
      assert(results.head.locationUri == expected,
        s"shared-SD case root=$rootPath relPath=$relPath got ${results.head.locationUri}")
    }

    // Root path unset and relative path null, against a table that itself carries no
    // storage descriptor: still no location.
    val bareResults = shim.buildPartitionLocations(
      hiveTable(null), Seq(sharedSpec(None, null, "1")))
    assert(bareResults.size == 1)
    assert(bareResults.head.locationUri.isEmpty)

    // Independent-Partition shapes, used for partitions outside the table directory.
    val listCases: Seq[StorageDescriptor] = Seq(
      null,                     // no storage descriptor row
      new StorageDescriptor(),  // storage descriptor with no location
      locationSd("file:/custom/part/dir")) // control: custom location intact
    listCases.zipWithIndex.foreach { case (sd, idx) =>
      val value = (idx + 1).toString
      val results = shim.buildPartitionLocations(
        hiveTable(locationSd(tableLocation)), Seq(listSpec(sd, value)))
      assert(results.size == 1)
      assert(results.head.spec == Map("p1" -> value))
      assert(results.head.locationUri == fullFetchLocation(sd),
        s"partition-list case with SD $sd got ${results.head.locationUri}")
    }
  }
}
