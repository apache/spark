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

package org.apache.spark.sql.execution.datasources

import java.net.URL
import java.util

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.connector.catalog.{Table, TableProvider}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.{DataSourceRegister, DataSourceRegisterV2}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.util.Utils
import org.junit.Assert

class DataSourceRegistrationSuite extends SharedSparkSession {
  private var cl = null : DataSourceRegistrationSuiteTestingClassLoader
  private var oldcl = Utils.getContextOrSparkClassLoader : ClassLoader
  val myconf = new SQLConf()

  /*
   * Set a custom classloader for this suite
   */
  protected override def beforeAll(): Unit = {
    cl = new DataSourceRegistrationSuiteTestingClassLoader(oldcl)
    Thread.currentThread.setContextClassLoader(cl)
    assert(Utils.getContextOrSparkClassLoader == cl)
    super.beforeAll()
  }

  protected override def afterAll(): Unit = {
    Thread.currentThread().setContextClassLoader(oldcl)
    assert(Utils.getContextOrSparkClassLoader == cl)
    super.afterAll()
  }

  test("load with 0 DSv2, 0 DSv1") {
    cl.setResources("unknown", "unknown")
    intercept[ClassNotFoundException] {
      DataSource.lookupDataSourceV2("testProvider", myconf)
    }
  }

  test("load with 1 DSv2, 0 DSv1") {
    cl.setResources("unknown", "dsregistration-v2/v2Registration1")
    val ret = DataSource.lookupDataSourceV2("testProvider", myconf)
    assert(ret.get.getClass() === new v2TableProvider().getClass())
  }

  test("load with 2 DSv2, 0 DSv1") {
    cl.setResources("unknown", "dsregistration-v2/v2Registration2")
    intercept[AnalysisException] {
      DataSource.lookupDataSourceV2("testProvider", myconf)
    }
  }

  test("load with 0 DSv2, 1 DSv1") {
    cl.setResources("dsregistration-v2/v1Registration1", "unknown")
    val ret = DataSource.lookupDataSourceV2("testProvider", myconf)
    assert(ret.get.getClass() === new v1TableProvider1().getClass())
  }

  test("load with 0 DSv2, 2 DSv1") {
    cl.setResources("dsregistration-v2/v1Registration2", "unknown")
    intercept[AnalysisException] {
      DataSource.lookupDataSourceV2("testProvider", myconf)
    }
  }
}

class v1TableProvider1 extends TableProvider with DataSourceRegister {
  override def getTable(options: CaseInsensitiveStringMap): Table = return null

  override def shortName(): String = "testProvider"
}

class v1TableProvider2 extends TableProvider with DataSourceRegister {
  override def getTable(options: CaseInsensitiveStringMap): Table = return null

  override def shortName(): String = "testProvider"
}

class v2TableProvider extends TableProvider {
  override def getTable(options: CaseInsensitiveStringMap): Table = return null
}

class v2Registration1 extends DataSourceRegisterV2 {
  override def shortName(): String = "testProvider"

  override def getImplementation(): Class[_] = {
    return new v2TableProvider().getClass()
  }
}

class v2Registration2 extends DataSourceRegisterV2 {
  override def shortName(): String = "testProvider"

  override def getImplementation(): Class[_] = {
    return new v2TableProvider().getClass()
  }
}

/**
 * Custom classloader to override the default classloader's resource lookup
 * for these tests
 */
private class DataSourceRegistrationSuiteTestingClassLoader(parent: ClassLoader)
  extends ClassLoader {
  var v1Resource = "unknown": String
  var v2Resource = "unknown": String

  def setResources(v1name: String, v2name: String): Unit = {
    v1Resource = v1name
    v2Resource = v2name
  }

  override def getResources(name: String): util.Enumeration[URL] =
    if (name.equals("META-INF/services/org.apache.spark.sql.sources.DataSourceRegisterV2")) {
      super.getResources(v2Resource)
    } else if (name.equals("META-INF/services/org.apache.spark.sql.sources.DataSourceRegister")) {
        super.getResources(v1Resource)
    } else {
      super.getResources(name)
    }
}
