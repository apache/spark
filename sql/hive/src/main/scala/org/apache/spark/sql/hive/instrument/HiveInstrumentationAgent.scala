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

package org.apache.spark.sql.hive.instrument

import javassist.ClassPool
import javassist.CtClass
import javassist.CtMethod
import javassist.ClassMap
import javassist.Modifier
import javassist.CtField
import java.util.concurrent.atomic.AtomicBoolean
import java.util.HashMap
import org.apache.hadoop.hive.shims.ShimLoader
import sun.misc.Unsafe

object HiveInstrumentationAgent {
  var latch = new AtomicBoolean(false)
  val unsafe = {
    val field = classOf[Unsafe].getDeclaredField("theUnsafe");
    field.setAccessible(true);
    field.get(null).asInstanceOf[Unsafe]
  }

  private val pool = ClassPool.getDefault();
  private val newClass = pool.get("org.apache.hadoop.hive.thrift.HadoopThriftAuthBridge23")
  private val oldClass = pool.get("org.apache.hadoop.hive.thrift.HadoopThriftAuthBridge20S")

  def instrument = {
    if (ShimLoader.getHadoopShims.isSecurityEnabled && ShimLoader.getMajorVersion == "0.23")  {
      if (!latch.getAndSet(true)) {
        val targetMethods = oldClass.getDeclaredMethods
        for (targetMethod <- targetMethods) {
          if (targetMethod.getName() == "getHadoopSaslProperties") {
            this.swapMethodBody(targetMethod)
          }
        }
        val scBytes = this.oldClass.toBytecode()
        unsafe.defineClass(null, scBytes, 0, scBytes.length,
          this.getClass.getClassLoader(), this.getClass.getProtectionDomain())
      }
    }
  }

  private def swapMethodBody(targetMethod: CtMethod) {
    val desc = targetMethod.getMethodInfo().getDescriptor()
    try {
      val sourceMethod = newClass.getMethod(targetMethod.getName(), desc)
      targetMethod.setBody(sourceMethod, null)
      println("swapMethodBody " + sourceMethod + " target: " + targetMethod)
    } catch {
      case _: Throwable =>
    }

  }
}
