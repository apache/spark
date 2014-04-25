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

package org.apache.spark

import scala.collection.mutable.ArrayBuffer

import org.scalatest.FunSuite

class SecurityManagerSuite extends FunSuite {

  test("set security with conf") {
    val conf = new SparkConf
    conf.set("spark.authenticate", "true")
    conf.set("spark.authenticate.secret", "good")
    conf.set("spark.ui.acls.enable", "true")
    conf.set("spark.ui.view.acls", "user1,user2")
    val securityManager = new SecurityManager(conf);
    assert(securityManager.isAuthenticationEnabled() === true)
    assert(securityManager.uiAclsEnabled() === true)
    assert(securityManager.checkUIViewPermissions("user1") === true)
    assert(securityManager.checkUIViewPermissions("user2") === true)
    assert(securityManager.checkUIViewPermissions("user3") === false)
  }

  test("set security with api") {
    val conf = new SparkConf
    conf.set("spark.ui.view.acls", "user1,user2")
    val securityManager = new SecurityManager(conf);
    securityManager.setUIAcls(true)
    assert(securityManager.uiAclsEnabled() === true)
    securityManager.setUIAcls(false)
    assert(securityManager.uiAclsEnabled() === false)

    // acls are off so doesn't matter what view acls set to
    assert(securityManager.checkUIViewPermissions("user4") === true)

    securityManager.setUIAcls(true)
    assert(securityManager.uiAclsEnabled() === true)
    securityManager.setViewAcls(ArrayBuffer[String]("user5"), "user6,user7")
    assert(securityManager.checkUIViewPermissions("user1") === false)
    assert(securityManager.checkUIViewPermissions("user5") === true)
    assert(securityManager.checkUIViewPermissions("user6") === true)
    assert(securityManager.checkUIViewPermissions("user7") === true)
    assert(securityManager.checkUIViewPermissions("user8") === false)
    assert(securityManager.checkUIViewPermissions(null) === true)
  }
}

