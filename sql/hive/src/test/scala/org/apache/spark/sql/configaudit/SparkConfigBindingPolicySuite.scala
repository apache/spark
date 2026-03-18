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

package org.apache.spark.sql.configaudit

import scala.io.Source
import scala.jdk.CollectionConverters._

import org.apache.spark.SparkFunSuite
import org.apache.spark.internal.config.{ConfigBindingPolicy, ConfigEntry}
import org.apache.spark.sql.hive.HiveUtils
import org.apache.spark.sql.internal.SQLConf

class SparkConfigBindingPolicySuite extends SparkFunSuite {

  override def beforeAll(): Unit = {
    super.beforeAll()
    // Ensure HiveUtils configs are registered before running tests.
    // Accessing a field triggers the Scala object's static initializer.
    assert(HiveUtils.CONVERT_METASTORE_PARQUET != null)
  }

  test("Test adding bindingPolicy to config") {
    val allConfigs = SQLConf.getConfigEntries().asScala.filter { entry =>
      entry.key == SQLConf.VIEW_SCHEMA_EVOLUTION_PRESERVE_USER_COMMENTS.key
    }
    assert(allConfigs.head.bindingPolicy.isDefined)
    assert(allConfigs.head.bindingPolicy.get == ConfigBindingPolicy.SESSION)
  }

  test("Config enforcement for bindingPolicy") {
    val allConfigsWithoutBindingPolicy: Iterable[ConfigEntry[_]] =
      ConfigEntry.listAllEntries().asScala.filter { entry =>
        entry.bindingPolicy.isEmpty
      }
    val filePath = getClass.getClassLoader.getResource(
      "conf/binding-policy-exceptions/configs-without-binding-policy-exceptions").getFile
    val allowedNonViewInheritConfs: Set[String] = Source.fromFile(filePath).getLines().toSet
    val missingBindingPolicyConfigs = allConfigsWithoutBindingPolicy.filterNot { entry =>
      allowedNonViewInheritConfs.contains(entry.key)
    }.map(_.key).toList.sorted

    if (missingBindingPolicyConfigs.nonEmpty) {
      fail(
        s"The following configs do not have bindingPolicy field set. You need to define it " +
        "by using .withBindingPolicy(ConfigBindingPolicy.SESSION/PERSISTED) when you build " +
        "the config entry.\n" +
        missingBindingPolicyConfigs.mkString("\n")
      )
    }

    val allConfigsWithBindingPolicy: Iterable[ConfigEntry[_]] =
      SQLConf.getConfigEntries().asScala.filter { entry =>
        entry.bindingPolicy.isDefined
      }
    allConfigsWithBindingPolicy.foreach { entry =>
      if (allowedNonViewInheritConfs.contains(entry.key)) {
        fail(
          s"${entry.key} already has bindingPolicy set but still in the allowlist. You " +
          s"should remove ${entry.key} from " +
          "sql/hive/src/test/resources/conf/binding-policy-exceptions/" +
          "configs-without-binding-policy-exceptions"
        )
      }
    }
  }

  test("configs-without-binding-policy-exceptions file should be sorted alphabetically") {
    val filePath = getClass.getClassLoader.getResource(
      "conf/binding-policy-exceptions/configs-without-binding-policy-exceptions").getFile
    val allowedNonViewInheritConfs: Seq[String] = Source.fromFile(filePath).getLines().toSeq
    val sortedAllowedNonViewInheritConfs: Seq[String] = allowedNonViewInheritConfs.sorted
    if (allowedNonViewInheritConfs != sortedAllowedNonViewInheritConfs) {
      fail("configs-without-binding-policy-exceptions file needs to be sorted alphabetically.")
    }
  }

}
