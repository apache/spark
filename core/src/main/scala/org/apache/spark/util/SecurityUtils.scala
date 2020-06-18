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

package org.apache.spark.util

/**
 * Various utility methods used by Spark Security.
 */
private[spark] object SecurityUtils {
  private val JAVA_VENDOR = "java.vendor"
  private val IBM_KRB_DEBUG_CONFIG = "com.ibm.security.krb5.Krb5Debug"
  private val SUN_KRB_DEBUG_CONFIG = "sun.security.krb5.debug"

  def setGlobalKrbDebug(enabled: Boolean): Unit = {
    if (enabled) {
      if (isIBMVendor()) {
        System.setProperty(IBM_KRB_DEBUG_CONFIG, "all")
      } else {
        System.setProperty(SUN_KRB_DEBUG_CONFIG, "true")
      }
    } else {
      if (isIBMVendor()) {
        System.clearProperty(IBM_KRB_DEBUG_CONFIG)
      } else {
        System.clearProperty(SUN_KRB_DEBUG_CONFIG)
      }
    }
  }

  def isGlobalKrbDebugEnabled(): Boolean = {
    if (isIBMVendor()) {
      val debug = System.getenv(IBM_KRB_DEBUG_CONFIG)
      debug != null && debug.equalsIgnoreCase("all")
    } else {
      val debug = System.getenv(SUN_KRB_DEBUG_CONFIG)
      debug != null && debug.equalsIgnoreCase("true")
    }
  }

  /**
   * Krb5LoginModule package varies in different JVMs.
   * Please see Hadoop UserGroupInformation for further details.
   */
  def getKrb5LoginModuleName(): String = {
    if (isIBMVendor()) {
      "com.ibm.security.auth.module.Krb5LoginModule"
    } else {
      "com.sun.security.auth.module.Krb5LoginModule"
    }
  }

  private def isIBMVendor(): Boolean = {
    System.getProperty(JAVA_VENDOR).contains("IBM")
  }
}
