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
package org.apache.spark.util.expression.parserTrait

import java.lang.management.ManagementFactory

import org.apache.spark.util.expression.quantity.ByteQuantity

/**
 * Functions that query the underlying physical machine the JVM is running on
 * Taken from workerArguments.scala and modded to suite
 */
private[spark] trait MachineInfoFunctions extends FunctionExpansion {

  protected abstract override def functions: Map[String, () => Double] = Map(
    "physicalMemoryBytes".toLowerCase -> (() => inferDefaultMemory * 1.0)
  ) ++ super.functions

  private def DefaultMemSize = ByteQuantity(2, "GB")

  /**
   * Returns the amount of physical memory on the machine
   * @return Number of bytes of memory on the physical machine if possible otherwise DefaultMemSize
   */
  private def inferDefaultMemory(): Long = {
    try {
      val bean = ManagementFactory.getOperatingSystemMXBean()
      val ibmVendor = System.getProperty("java.vendor").contains("IBM")
      val getMemSizeMethod = if (ibmVendor) {
        val beanClass = Class.forName("com.ibm.lang.management.OperatingSystemMXBean")
        beanClass.getDeclaredMethod("getTotalPhysicalMemory")
      } else {
        val beanClass = Class.forName("com.sun.management.OperatingSystemMXBean")
        beanClass.getDeclaredMethod("getTotalPhysicalMemorySize")
      }
      getMemSizeMethod.invoke(bean).asInstanceOf[Long]
    } catch {
      case e: Exception => {
        System.out.println("Failed to get total physical memory. Using " + DefaultMemSize.toMB
          + " MB")
        DefaultMemSize.toBytes
      }
    }
  }
}
