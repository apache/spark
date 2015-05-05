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

import scala.reflect._

/**
 * Provides implicit functions on any object for calling methods reflectively.
 */
protected trait ReflectionMagic {
    /** code for InstanceMagic
        println(
    (1 to 22).map { n =>
      def repeat(str: String => String) = (1 to n).map(i => str(i.toString)).mkString(", ")
      val types = repeat(n => s"A$n <: AnyRef : ClassTag")
      val inArgs = repeat(n => s"a$n: A$n")
      val erasure = repeat(n => s"classTag[A$n].erasure")
      val outArgs = repeat(n => s"a$n")
      s"""|def call[$types, R](name: String, $inArgs): R = {
         |  clazz.getMethod(name, $erasure).invoke(a, $outArgs).asInstanceOf[R]
         |}""".stripMargin
    }.mkString("\n")
    )
   */

  // scalastyle:off
  protected implicit class InstanceMagic(a: Any) {
    private val clazz = a.getClass

    def call[R](name: String): R = {
      clazz.getMethod(name).invoke(a).asInstanceOf[R]
    }
    def call[A1 <: AnyRef : ClassTag, R](name: String, a1: A1): R = {
      clazz.getMethod(name, classTag[A1].erasure).invoke(a, a1).asInstanceOf[R]
    }
    def call[A1 <: AnyRef : ClassTag, A2 <: AnyRef : ClassTag, R](name: String, a1: A1, a2: A2): R = {
      clazz.getMethod(name, classTag[A1].erasure, classTag[A2].erasure).invoke(a, a1, a2).asInstanceOf[R]
    }
    def call[A1 <: AnyRef : ClassTag, A2 <: AnyRef : ClassTag, A3 <: AnyRef : ClassTag, R](name: String, a1: A1, a2: A2, a3: A3): R = {
      clazz.getMethod(name, classTag[A1].erasure, classTag[A2].erasure, classTag[A3].erasure).invoke(a, a1, a2, a3).asInstanceOf[R]
    }
    def call[A1 <: AnyRef : ClassTag, A2 <: AnyRef : ClassTag, A3 <: AnyRef : ClassTag, A4 <: AnyRef : ClassTag, R](name: String, a1: A1, a2: A2, a3: A3, a4: A4): R = {
      clazz.getMethod(name, classTag[A1].erasure, classTag[A2].erasure, classTag[A3].erasure, classTag[A4].erasure).invoke(a, a1, a2, a3, a4).asInstanceOf[R]
    }
    def call[A1 <: AnyRef : ClassTag, A2 <: AnyRef : ClassTag, A3 <: AnyRef : ClassTag, A4 <: AnyRef : ClassTag, A5 <: AnyRef : ClassTag, R](name: String, a1: A1, a2: A2, a3: A3, a4: A4, a5: A5): R = {
      clazz.getMethod(name, classTag[A1].erasure, classTag[A2].erasure, classTag[A3].erasure, classTag[A4].erasure, classTag[A5].erasure).invoke(a, a1, a2, a3, a4, a5).asInstanceOf[R]
    }
    def call[A1 <: AnyRef : ClassTag, A2 <: AnyRef : ClassTag, A3 <: AnyRef : ClassTag, A4 <: AnyRef : ClassTag, A5 <: AnyRef : ClassTag, A6 <: AnyRef : ClassTag, R](name: String, a1: A1, a2: A2, a3: A3, a4: A4, a5: A5, a6: A6): R = {
      clazz.getMethod(name, classTag[A1].erasure, classTag[A2].erasure, classTag[A3].erasure, classTag[A4].erasure, classTag[A5].erasure, classTag[A6].erasure).invoke(a, a1, a2, a3, a4, a5, a6).asInstanceOf[R]
    }
    def call[A1 <: AnyRef : ClassTag, A2 <: AnyRef : ClassTag, A3 <: AnyRef : ClassTag, A4 <: AnyRef : ClassTag, A5 <: AnyRef : ClassTag, A6 <: AnyRef : ClassTag, A7 <: AnyRef : ClassTag, R](name: String, a1: A1, a2: A2, a3: A3, a4: A4, a5: A5, a6: A6, a7: A7): R = {
      clazz.getMethod(name, classTag[A1].erasure, classTag[A2].erasure, classTag[A3].erasure, classTag[A4].erasure, classTag[A5].erasure, classTag[A6].erasure, classTag[A7].erasure).invoke(a, a1, a2, a3, a4, a5, a6, a7).asInstanceOf[R]
    }
    def call[A1 <: AnyRef : ClassTag, A2 <: AnyRef : ClassTag, A3 <: AnyRef : ClassTag, A4 <: AnyRef : ClassTag, A5 <: AnyRef : ClassTag, A6 <: AnyRef : ClassTag, A7 <: AnyRef : ClassTag, A8 <: AnyRef : ClassTag, R](name: String, a1: A1, a2: A2, a3: A3, a4: A4, a5: A5, a6: A6, a7: A7, a8: A8): R = {
      clazz.getMethod(name, classTag[A1].erasure, classTag[A2].erasure, classTag[A3].erasure, classTag[A4].erasure, classTag[A5].erasure, classTag[A6].erasure, classTag[A7].erasure, classTag[A8].erasure).invoke(a, a1, a2, a3, a4, a5, a6, a7, a8).asInstanceOf[R]
    }
    def call[A1 <: AnyRef : ClassTag, A2 <: AnyRef : ClassTag, A3 <: AnyRef : ClassTag, A4 <: AnyRef : ClassTag, A5 <: AnyRef : ClassTag, A6 <: AnyRef : ClassTag, A7 <: AnyRef : ClassTag, A8 <: AnyRef : ClassTag, A9 <: AnyRef : ClassTag, R](name: String, a1: A1, a2: A2, a3: A3, a4: A4, a5: A5, a6: A6, a7: A7, a8: A8, a9: A9): R = {
      clazz.getMethod(name, classTag[A1].erasure, classTag[A2].erasure, classTag[A3].erasure, classTag[A4].erasure, classTag[A5].erasure, classTag[A6].erasure, classTag[A7].erasure, classTag[A8].erasure, classTag[A9].erasure).invoke(a, a1, a2, a3, a4, a5, a6, a7, a8, a9).asInstanceOf[R]
    }
    def call[A1 <: AnyRef : ClassTag, A2 <: AnyRef : ClassTag, A3 <: AnyRef : ClassTag, A4 <: AnyRef : ClassTag, A5 <: AnyRef : ClassTag, A6 <: AnyRef : ClassTag, A7 <: AnyRef : ClassTag, A8 <: AnyRef : ClassTag, A9 <: AnyRef : ClassTag, A10 <: AnyRef : ClassTag, R](name: String, a1: A1, a2: A2, a3: A3, a4: A4, a5: A5, a6: A6, a7: A7, a8: A8, a9: A9, a10: A10): R = {
      clazz.getMethod(name, classTag[A1].erasure, classTag[A2].erasure, classTag[A3].erasure, classTag[A4].erasure, classTag[A5].erasure, classTag[A6].erasure, classTag[A7].erasure, classTag[A8].erasure, classTag[A9].erasure, classTag[A10].erasure).invoke(a, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10).asInstanceOf[R]
    }
    def call[A1 <: AnyRef : ClassTag, A2 <: AnyRef : ClassTag, A3 <: AnyRef : ClassTag, A4 <: AnyRef : ClassTag, A5 <: AnyRef : ClassTag, A6 <: AnyRef : ClassTag, A7 <: AnyRef : ClassTag, A8 <: AnyRef : ClassTag, A9 <: AnyRef : ClassTag, A10 <: AnyRef : ClassTag, A11 <: AnyRef : ClassTag, R](name: String, a1: A1, a2: A2, a3: A3, a4: A4, a5: A5, a6: A6, a7: A7, a8: A8, a9: A9, a10: A10, a11: A11): R = {
      clazz.getMethod(name, classTag[A1].erasure, classTag[A2].erasure, classTag[A3].erasure, classTag[A4].erasure, classTag[A5].erasure, classTag[A6].erasure, classTag[A7].erasure, classTag[A8].erasure, classTag[A9].erasure, classTag[A10].erasure, classTag[A11].erasure).invoke(a, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11).asInstanceOf[R]
    }
    def call[A1 <: AnyRef : ClassTag, A2 <: AnyRef : ClassTag, A3 <: AnyRef : ClassTag, A4 <: AnyRef : ClassTag, A5 <: AnyRef : ClassTag, A6 <: AnyRef : ClassTag, A7 <: AnyRef : ClassTag, A8 <: AnyRef : ClassTag, A9 <: AnyRef : ClassTag, A10 <: AnyRef : ClassTag, A11 <: AnyRef : ClassTag, A12 <: AnyRef : ClassTag, R](name: String, a1: A1, a2: A2, a3: A3, a4: A4, a5: A5, a6: A6, a7: A7, a8: A8, a9: A9, a10: A10, a11: A11, a12: A12): R = {
      clazz.getMethod(name, classTag[A1].erasure, classTag[A2].erasure, classTag[A3].erasure, classTag[A4].erasure, classTag[A5].erasure, classTag[A6].erasure, classTag[A7].erasure, classTag[A8].erasure, classTag[A9].erasure, classTag[A10].erasure, classTag[A11].erasure, classTag[A12].erasure).invoke(a, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12).asInstanceOf[R]
    }
    def call[A1 <: AnyRef : ClassTag, A2 <: AnyRef : ClassTag, A3 <: AnyRef : ClassTag, A4 <: AnyRef : ClassTag, A5 <: AnyRef : ClassTag, A6 <: AnyRef : ClassTag, A7 <: AnyRef : ClassTag, A8 <: AnyRef : ClassTag, A9 <: AnyRef : ClassTag, A10 <: AnyRef : ClassTag, A11 <: AnyRef : ClassTag, A12 <: AnyRef : ClassTag, A13 <: AnyRef : ClassTag, R](name: String, a1: A1, a2: A2, a3: A3, a4: A4, a5: A5, a6: A6, a7: A7, a8: A8, a9: A9, a10: A10, a11: A11, a12: A12, a13: A13): R = {
      clazz.getMethod(name, classTag[A1].erasure, classTag[A2].erasure, classTag[A3].erasure, classTag[A4].erasure, classTag[A5].erasure, classTag[A6].erasure, classTag[A7].erasure, classTag[A8].erasure, classTag[A9].erasure, classTag[A10].erasure, classTag[A11].erasure, classTag[A12].erasure, classTag[A13].erasure).invoke(a, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13).asInstanceOf[R]
    }
    def call[A1 <: AnyRef : ClassTag, A2 <: AnyRef : ClassTag, A3 <: AnyRef : ClassTag, A4 <: AnyRef : ClassTag, A5 <: AnyRef : ClassTag, A6 <: AnyRef : ClassTag, A7 <: AnyRef : ClassTag, A8 <: AnyRef : ClassTag, A9 <: AnyRef : ClassTag, A10 <: AnyRef : ClassTag, A11 <: AnyRef : ClassTag, A12 <: AnyRef : ClassTag, A13 <: AnyRef : ClassTag, A14 <: AnyRef : ClassTag, R](name: String, a1: A1, a2: A2, a3: A3, a4: A4, a5: A5, a6: A6, a7: A7, a8: A8, a9: A9, a10: A10, a11: A11, a12: A12, a13: A13, a14: A14): R = {
      clazz.getMethod(name, classTag[A1].erasure, classTag[A2].erasure, classTag[A3].erasure, classTag[A4].erasure, classTag[A5].erasure, classTag[A6].erasure, classTag[A7].erasure, classTag[A8].erasure, classTag[A9].erasure, classTag[A10].erasure, classTag[A11].erasure, classTag[A12].erasure, classTag[A13].erasure, classTag[A14].erasure).invoke(a, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14).asInstanceOf[R]
    }
    def call[A1 <: AnyRef : ClassTag, A2 <: AnyRef : ClassTag, A3 <: AnyRef : ClassTag, A4 <: AnyRef : ClassTag, A5 <: AnyRef : ClassTag, A6 <: AnyRef : ClassTag, A7 <: AnyRef : ClassTag, A8 <: AnyRef : ClassTag, A9 <: AnyRef : ClassTag, A10 <: AnyRef : ClassTag, A11 <: AnyRef : ClassTag, A12 <: AnyRef : ClassTag, A13 <: AnyRef : ClassTag, A14 <: AnyRef : ClassTag, A15 <: AnyRef : ClassTag, R](name: String, a1: A1, a2: A2, a3: A3, a4: A4, a5: A5, a6: A6, a7: A7, a8: A8, a9: A9, a10: A10, a11: A11, a12: A12, a13: A13, a14: A14, a15: A15): R = {
      clazz.getMethod(name, classTag[A1].erasure, classTag[A2].erasure, classTag[A3].erasure, classTag[A4].erasure, classTag[A5].erasure, classTag[A6].erasure, classTag[A7].erasure, classTag[A8].erasure, classTag[A9].erasure, classTag[A10].erasure, classTag[A11].erasure, classTag[A12].erasure, classTag[A13].erasure, classTag[A14].erasure, classTag[A15].erasure).invoke(a, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15).asInstanceOf[R]
    }
    def call[A1 <: AnyRef : ClassTag, A2 <: AnyRef : ClassTag, A3 <: AnyRef : ClassTag, A4 <: AnyRef : ClassTag, A5 <: AnyRef : ClassTag, A6 <: AnyRef : ClassTag, A7 <: AnyRef : ClassTag, A8 <: AnyRef : ClassTag, A9 <: AnyRef : ClassTag, A10 <: AnyRef : ClassTag, A11 <: AnyRef : ClassTag, A12 <: AnyRef : ClassTag, A13 <: AnyRef : ClassTag, A14 <: AnyRef : ClassTag, A15 <: AnyRef : ClassTag, A16 <: AnyRef : ClassTag, R](name: String, a1: A1, a2: A2, a3: A3, a4: A4, a5: A5, a6: A6, a7: A7, a8: A8, a9: A9, a10: A10, a11: A11, a12: A12, a13: A13, a14: A14, a15: A15, a16: A16): R = {
      clazz.getMethod(name, classTag[A1].erasure, classTag[A2].erasure, classTag[A3].erasure, classTag[A4].erasure, classTag[A5].erasure, classTag[A6].erasure, classTag[A7].erasure, classTag[A8].erasure, classTag[A9].erasure, classTag[A10].erasure, classTag[A11].erasure, classTag[A12].erasure, classTag[A13].erasure, classTag[A14].erasure, classTag[A15].erasure, classTag[A16].erasure).invoke(a, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16).asInstanceOf[R]
    }
    def call[A1 <: AnyRef : ClassTag, A2 <: AnyRef : ClassTag, A3 <: AnyRef : ClassTag, A4 <: AnyRef : ClassTag, A5 <: AnyRef : ClassTag, A6 <: AnyRef : ClassTag, A7 <: AnyRef : ClassTag, A8 <: AnyRef : ClassTag, A9 <: AnyRef : ClassTag, A10 <: AnyRef : ClassTag, A11 <: AnyRef : ClassTag, A12 <: AnyRef : ClassTag, A13 <: AnyRef : ClassTag, A14 <: AnyRef : ClassTag, A15 <: AnyRef : ClassTag, A16 <: AnyRef : ClassTag, A17 <: AnyRef : ClassTag, R](name: String, a1: A1, a2: A2, a3: A3, a4: A4, a5: A5, a6: A6, a7: A7, a8: A8, a9: A9, a10: A10, a11: A11, a12: A12, a13: A13, a14: A14, a15: A15, a16: A16, a17: A17): R = {
      clazz.getMethod(name, classTag[A1].erasure, classTag[A2].erasure, classTag[A3].erasure, classTag[A4].erasure, classTag[A5].erasure, classTag[A6].erasure, classTag[A7].erasure, classTag[A8].erasure, classTag[A9].erasure, classTag[A10].erasure, classTag[A11].erasure, classTag[A12].erasure, classTag[A13].erasure, classTag[A14].erasure, classTag[A15].erasure, classTag[A16].erasure, classTag[A17].erasure).invoke(a, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17).asInstanceOf[R]
    }
    def call[A1 <: AnyRef : ClassTag, A2 <: AnyRef : ClassTag, A3 <: AnyRef : ClassTag, A4 <: AnyRef : ClassTag, A5 <: AnyRef : ClassTag, A6 <: AnyRef : ClassTag, A7 <: AnyRef : ClassTag, A8 <: AnyRef : ClassTag, A9 <: AnyRef : ClassTag, A10 <: AnyRef : ClassTag, A11 <: AnyRef : ClassTag, A12 <: AnyRef : ClassTag, A13 <: AnyRef : ClassTag, A14 <: AnyRef : ClassTag, A15 <: AnyRef : ClassTag, A16 <: AnyRef : ClassTag, A17 <: AnyRef : ClassTag, A18 <: AnyRef : ClassTag, R](name: String, a1: A1, a2: A2, a3: A3, a4: A4, a5: A5, a6: A6, a7: A7, a8: A8, a9: A9, a10: A10, a11: A11, a12: A12, a13: A13, a14: A14, a15: A15, a16: A16, a17: A17, a18: A18): R = {
      clazz.getMethod(name, classTag[A1].erasure, classTag[A2].erasure, classTag[A3].erasure, classTag[A4].erasure, classTag[A5].erasure, classTag[A6].erasure, classTag[A7].erasure, classTag[A8].erasure, classTag[A9].erasure, classTag[A10].erasure, classTag[A11].erasure, classTag[A12].erasure, classTag[A13].erasure, classTag[A14].erasure, classTag[A15].erasure, classTag[A16].erasure, classTag[A17].erasure, classTag[A18].erasure).invoke(a, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18).asInstanceOf[R]
    }
    def call[A1 <: AnyRef : ClassTag, A2 <: AnyRef : ClassTag, A3 <: AnyRef : ClassTag, A4 <: AnyRef : ClassTag, A5 <: AnyRef : ClassTag, A6 <: AnyRef : ClassTag, A7 <: AnyRef : ClassTag, A8 <: AnyRef : ClassTag, A9 <: AnyRef : ClassTag, A10 <: AnyRef : ClassTag, A11 <: AnyRef : ClassTag, A12 <: AnyRef : ClassTag, A13 <: AnyRef : ClassTag, A14 <: AnyRef : ClassTag, A15 <: AnyRef : ClassTag, A16 <: AnyRef : ClassTag, A17 <: AnyRef : ClassTag, A18 <: AnyRef : ClassTag, A19 <: AnyRef : ClassTag, R](name: String, a1: A1, a2: A2, a3: A3, a4: A4, a5: A5, a6: A6, a7: A7, a8: A8, a9: A9, a10: A10, a11: A11, a12: A12, a13: A13, a14: A14, a15: A15, a16: A16, a17: A17, a18: A18, a19: A19): R = {
      clazz.getMethod(name, classTag[A1].erasure, classTag[A2].erasure, classTag[A3].erasure, classTag[A4].erasure, classTag[A5].erasure, classTag[A6].erasure, classTag[A7].erasure, classTag[A8].erasure, classTag[A9].erasure, classTag[A10].erasure, classTag[A11].erasure, classTag[A12].erasure, classTag[A13].erasure, classTag[A14].erasure, classTag[A15].erasure, classTag[A16].erasure, classTag[A17].erasure, classTag[A18].erasure, classTag[A19].erasure).invoke(a, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19).asInstanceOf[R]
    }
    def call[A1 <: AnyRef : ClassTag, A2 <: AnyRef : ClassTag, A3 <: AnyRef : ClassTag, A4 <: AnyRef : ClassTag, A5 <: AnyRef : ClassTag, A6 <: AnyRef : ClassTag, A7 <: AnyRef : ClassTag, A8 <: AnyRef : ClassTag, A9 <: AnyRef : ClassTag, A10 <: AnyRef : ClassTag, A11 <: AnyRef : ClassTag, A12 <: AnyRef : ClassTag, A13 <: AnyRef : ClassTag, A14 <: AnyRef : ClassTag, A15 <: AnyRef : ClassTag, A16 <: AnyRef : ClassTag, A17 <: AnyRef : ClassTag, A18 <: AnyRef : ClassTag, A19 <: AnyRef : ClassTag, A20 <: AnyRef : ClassTag, R](name: String, a1: A1, a2: A2, a3: A3, a4: A4, a5: A5, a6: A6, a7: A7, a8: A8, a9: A9, a10: A10, a11: A11, a12: A12, a13: A13, a14: A14, a15: A15, a16: A16, a17: A17, a18: A18, a19: A19, a20: A20): R = {
      clazz.getMethod(name, classTag[A1].erasure, classTag[A2].erasure, classTag[A3].erasure, classTag[A4].erasure, classTag[A5].erasure, classTag[A6].erasure, classTag[A7].erasure, classTag[A8].erasure, classTag[A9].erasure, classTag[A10].erasure, classTag[A11].erasure, classTag[A12].erasure, classTag[A13].erasure, classTag[A14].erasure, classTag[A15].erasure, classTag[A16].erasure, classTag[A17].erasure, classTag[A18].erasure, classTag[A19].erasure, classTag[A20].erasure).invoke(a, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19, a20).asInstanceOf[R]
    }
    def call[A1 <: AnyRef : ClassTag, A2 <: AnyRef : ClassTag, A3 <: AnyRef : ClassTag, A4 <: AnyRef : ClassTag, A5 <: AnyRef : ClassTag, A6 <: AnyRef : ClassTag, A7 <: AnyRef : ClassTag, A8 <: AnyRef : ClassTag, A9 <: AnyRef : ClassTag, A10 <: AnyRef : ClassTag, A11 <: AnyRef : ClassTag, A12 <: AnyRef : ClassTag, A13 <: AnyRef : ClassTag, A14 <: AnyRef : ClassTag, A15 <: AnyRef : ClassTag, A16 <: AnyRef : ClassTag, A17 <: AnyRef : ClassTag, A18 <: AnyRef : ClassTag, A19 <: AnyRef : ClassTag, A20 <: AnyRef : ClassTag, A21 <: AnyRef : ClassTag, R](name: String, a1: A1, a2: A2, a3: A3, a4: A4, a5: A5, a6: A6, a7: A7, a8: A8, a9: A9, a10: A10, a11: A11, a12: A12, a13: A13, a14: A14, a15: A15, a16: A16, a17: A17, a18: A18, a19: A19, a20: A20, a21: A21): R = {
      clazz.getMethod(name, classTag[A1].erasure, classTag[A2].erasure, classTag[A3].erasure, classTag[A4].erasure, classTag[A5].erasure, classTag[A6].erasure, classTag[A7].erasure, classTag[A8].erasure, classTag[A9].erasure, classTag[A10].erasure, classTag[A11].erasure, classTag[A12].erasure, classTag[A13].erasure, classTag[A14].erasure, classTag[A15].erasure, classTag[A16].erasure, classTag[A17].erasure, classTag[A18].erasure, classTag[A19].erasure, classTag[A20].erasure, classTag[A21].erasure).invoke(a, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21).asInstanceOf[R]
    }
    def call[A1 <: AnyRef : ClassTag, A2 <: AnyRef : ClassTag, A3 <: AnyRef : ClassTag, A4 <: AnyRef : ClassTag, A5 <: AnyRef : ClassTag, A6 <: AnyRef : ClassTag, A7 <: AnyRef : ClassTag, A8 <: AnyRef : ClassTag, A9 <: AnyRef : ClassTag, A10 <: AnyRef : ClassTag, A11 <: AnyRef : ClassTag, A12 <: AnyRef : ClassTag, A13 <: AnyRef : ClassTag, A14 <: AnyRef : ClassTag, A15 <: AnyRef : ClassTag, A16 <: AnyRef : ClassTag, A17 <: AnyRef : ClassTag, A18 <: AnyRef : ClassTag, A19 <: AnyRef : ClassTag, A20 <: AnyRef : ClassTag, A21 <: AnyRef : ClassTag, A22 <: AnyRef : ClassTag, R](name: String, a1: A1, a2: A2, a3: A3, a4: A4, a5: A5, a6: A6, a7: A7, a8: A8, a9: A9, a10: A10, a11: A11, a12: A12, a13: A13, a14: A14, a15: A15, a16: A16, a17: A17, a18: A18, a19: A19, a20: A20, a21: A21, a22: A22): R = {
      clazz.getMethod(name, classTag[A1].erasure, classTag[A2].erasure, classTag[A3].erasure, classTag[A4].erasure, classTag[A5].erasure, classTag[A6].erasure, classTag[A7].erasure, classTag[A8].erasure, classTag[A9].erasure, classTag[A10].erasure, classTag[A11].erasure, classTag[A12].erasure, classTag[A13].erasure, classTag[A14].erasure, classTag[A15].erasure, classTag[A16].erasure, classTag[A17].erasure, classTag[A18].erasure, classTag[A19].erasure, classTag[A20].erasure, classTag[A21].erasure, classTag[A22].erasure).invoke(a, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22).asInstanceOf[R]
    }
  }

  /** code for StaticMagic
        println(
    (1 to 22).map { n =>
      def repeat(str: String => String) = (1 to n).map(i => str(i.toString)).mkString(", ")
      val types = repeat(n => s"A$n <: AnyRef : ClassTag")
      val inArgs = repeat(n => s"a$n: A$n")
      val erasure = repeat(n => s"classTag[A$n].erasure")
      val outArgs = repeat(n => s"a$n")
      s"""|def callStatic[$types, R](name: String, $inArgs): R = {
         |  c.getDeclaredMethod(name, $erasure).invoke(c, $outArgs).asInstanceOf[R]
         |}""".stripMargin
    }.mkString("\n")
    )
   */

  protected implicit class StaticMagic(c: Class[_]) {
    def callStatic[A1 <: AnyRef : ClassTag, R](name: String, a1: A1): R = {
      c.getDeclaredMethod(name, classTag[A1].erasure).invoke(c, a1).asInstanceOf[R]
    }
    def callStatic[A1 <: AnyRef : ClassTag, A2 <: AnyRef : ClassTag, R](name: String, a1: A1, a2: A2): R = {
      c.getDeclaredMethod(name, classTag[A1].erasure, classTag[A2].erasure).invoke(c, a1, a2).asInstanceOf[R]
    }
    def callStatic[A1 <: AnyRef : ClassTag, A2 <: AnyRef : ClassTag, A3 <: AnyRef : ClassTag, R](name: String, a1: A1, a2: A2, a3: A3): R = {
      c.getDeclaredMethod(name, classTag[A1].erasure, classTag[A2].erasure, classTag[A3].erasure).invoke(c, a1, a2, a3).asInstanceOf[R]
    }
    def callStatic[A1 <: AnyRef : ClassTag, A2 <: AnyRef : ClassTag, A3 <: AnyRef : ClassTag, A4 <: AnyRef : ClassTag, R](name: String, a1: A1, a2: A2, a3: A3, a4: A4): R = {
      c.getDeclaredMethod(name, classTag[A1].erasure, classTag[A2].erasure, classTag[A3].erasure, classTag[A4].erasure).invoke(c, a1, a2, a3, a4).asInstanceOf[R]
    }
    def callStatic[A1 <: AnyRef : ClassTag, A2 <: AnyRef : ClassTag, A3 <: AnyRef : ClassTag, A4 <: AnyRef : ClassTag, A5 <: AnyRef : ClassTag, R](name: String, a1: A1, a2: A2, a3: A3, a4: A4, a5: A5): R = {
      c.getDeclaredMethod(name, classTag[A1].erasure, classTag[A2].erasure, classTag[A3].erasure, classTag[A4].erasure, classTag[A5].erasure).invoke(c, a1, a2, a3, a4, a5).asInstanceOf[R]
    }
    def callStatic[A1 <: AnyRef : ClassTag, A2 <: AnyRef : ClassTag, A3 <: AnyRef : ClassTag, A4 <: AnyRef : ClassTag, A5 <: AnyRef : ClassTag, A6 <: AnyRef : ClassTag, R](name: String, a1: A1, a2: A2, a3: A3, a4: A4, a5: A5, a6: A6): R = {
      c.getDeclaredMethod(name, classTag[A1].erasure, classTag[A2].erasure, classTag[A3].erasure, classTag[A4].erasure, classTag[A5].erasure, classTag[A6].erasure).invoke(c, a1, a2, a3, a4, a5, a6).asInstanceOf[R]
    }
    def callStatic[A1 <: AnyRef : ClassTag, A2 <: AnyRef : ClassTag, A3 <: AnyRef : ClassTag, A4 <: AnyRef : ClassTag, A5 <: AnyRef : ClassTag, A6 <: AnyRef : ClassTag, A7 <: AnyRef : ClassTag, R](name: String, a1: A1, a2: A2, a3: A3, a4: A4, a5: A5, a6: A6, a7: A7): R = {
      c.getDeclaredMethod(name, classTag[A1].erasure, classTag[A2].erasure, classTag[A3].erasure, classTag[A4].erasure, classTag[A5].erasure, classTag[A6].erasure, classTag[A7].erasure).invoke(c, a1, a2, a3, a4, a5, a6, a7).asInstanceOf[R]
    }
    def callStatic[A1 <: AnyRef : ClassTag, A2 <: AnyRef : ClassTag, A3 <: AnyRef : ClassTag, A4 <: AnyRef : ClassTag, A5 <: AnyRef : ClassTag, A6 <: AnyRef : ClassTag, A7 <: AnyRef : ClassTag, A8 <: AnyRef : ClassTag, R](name: String, a1: A1, a2: A2, a3: A3, a4: A4, a5: A5, a6: A6, a7: A7, a8: A8): R = {
      c.getDeclaredMethod(name, classTag[A1].erasure, classTag[A2].erasure, classTag[A3].erasure, classTag[A4].erasure, classTag[A5].erasure, classTag[A6].erasure, classTag[A7].erasure, classTag[A8].erasure).invoke(c, a1, a2, a3, a4, a5, a6, a7, a8).asInstanceOf[R]
    }
    def callStatic[A1 <: AnyRef : ClassTag, A2 <: AnyRef : ClassTag, A3 <: AnyRef : ClassTag, A4 <: AnyRef : ClassTag, A5 <: AnyRef : ClassTag, A6 <: AnyRef : ClassTag, A7 <: AnyRef : ClassTag, A8 <: AnyRef : ClassTag, A9 <: AnyRef : ClassTag, R](name: String, a1: A1, a2: A2, a3: A3, a4: A4, a5: A5, a6: A6, a7: A7, a8: A8, a9: A9): R = {
      c.getDeclaredMethod(name, classTag[A1].erasure, classTag[A2].erasure, classTag[A3].erasure, classTag[A4].erasure, classTag[A5].erasure, classTag[A6].erasure, classTag[A7].erasure, classTag[A8].erasure, classTag[A9].erasure).invoke(c, a1, a2, a3, a4, a5, a6, a7, a8, a9).asInstanceOf[R]
    }
    def callStatic[A1 <: AnyRef : ClassTag, A2 <: AnyRef : ClassTag, A3 <: AnyRef : ClassTag, A4 <: AnyRef : ClassTag, A5 <: AnyRef : ClassTag, A6 <: AnyRef : ClassTag, A7 <: AnyRef : ClassTag, A8 <: AnyRef : ClassTag, A9 <: AnyRef : ClassTag, A10 <: AnyRef : ClassTag, R](name: String, a1: A1, a2: A2, a3: A3, a4: A4, a5: A5, a6: A6, a7: A7, a8: A8, a9: A9, a10: A10): R = {
      c.getDeclaredMethod(name, classTag[A1].erasure, classTag[A2].erasure, classTag[A3].erasure, classTag[A4].erasure, classTag[A5].erasure, classTag[A6].erasure, classTag[A7].erasure, classTag[A8].erasure, classTag[A9].erasure, classTag[A10].erasure).invoke(c, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10).asInstanceOf[R]
    }
    def callStatic[A1 <: AnyRef : ClassTag, A2 <: AnyRef : ClassTag, A3 <: AnyRef : ClassTag, A4 <: AnyRef : ClassTag, A5 <: AnyRef : ClassTag, A6 <: AnyRef : ClassTag, A7 <: AnyRef : ClassTag, A8 <: AnyRef : ClassTag, A9 <: AnyRef : ClassTag, A10 <: AnyRef : ClassTag, A11 <: AnyRef : ClassTag, R](name: String, a1: A1, a2: A2, a3: A3, a4: A4, a5: A5, a6: A6, a7: A7, a8: A8, a9: A9, a10: A10, a11: A11): R = {
      c.getDeclaredMethod(name, classTag[A1].erasure, classTag[A2].erasure, classTag[A3].erasure, classTag[A4].erasure, classTag[A5].erasure, classTag[A6].erasure, classTag[A7].erasure, classTag[A8].erasure, classTag[A9].erasure, classTag[A10].erasure, classTag[A11].erasure).invoke(c, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11).asInstanceOf[R]
    }
    def callStatic[A1 <: AnyRef : ClassTag, A2 <: AnyRef : ClassTag, A3 <: AnyRef : ClassTag, A4 <: AnyRef : ClassTag, A5 <: AnyRef : ClassTag, A6 <: AnyRef : ClassTag, A7 <: AnyRef : ClassTag, A8 <: AnyRef : ClassTag, A9 <: AnyRef : ClassTag, A10 <: AnyRef : ClassTag, A11 <: AnyRef : ClassTag, A12 <: AnyRef : ClassTag, R](name: String, a1: A1, a2: A2, a3: A3, a4: A4, a5: A5, a6: A6, a7: A7, a8: A8, a9: A9, a10: A10, a11: A11, a12: A12): R = {
      c.getDeclaredMethod(name, classTag[A1].erasure, classTag[A2].erasure, classTag[A3].erasure, classTag[A4].erasure, classTag[A5].erasure, classTag[A6].erasure, classTag[A7].erasure, classTag[A8].erasure, classTag[A9].erasure, classTag[A10].erasure, classTag[A11].erasure, classTag[A12].erasure).invoke(c, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12).asInstanceOf[R]
    }
    def callStatic[A1 <: AnyRef : ClassTag, A2 <: AnyRef : ClassTag, A3 <: AnyRef : ClassTag, A4 <: AnyRef : ClassTag, A5 <: AnyRef : ClassTag, A6 <: AnyRef : ClassTag, A7 <: AnyRef : ClassTag, A8 <: AnyRef : ClassTag, A9 <: AnyRef : ClassTag, A10 <: AnyRef : ClassTag, A11 <: AnyRef : ClassTag, A12 <: AnyRef : ClassTag, A13 <: AnyRef : ClassTag, R](name: String, a1: A1, a2: A2, a3: A3, a4: A4, a5: A5, a6: A6, a7: A7, a8: A8, a9: A9, a10: A10, a11: A11, a12: A12, a13: A13): R = {
      c.getDeclaredMethod(name, classTag[A1].erasure, classTag[A2].erasure, classTag[A3].erasure, classTag[A4].erasure, classTag[A5].erasure, classTag[A6].erasure, classTag[A7].erasure, classTag[A8].erasure, classTag[A9].erasure, classTag[A10].erasure, classTag[A11].erasure, classTag[A12].erasure, classTag[A13].erasure).invoke(c, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13).asInstanceOf[R]
    }
    def callStatic[A1 <: AnyRef : ClassTag, A2 <: AnyRef : ClassTag, A3 <: AnyRef : ClassTag, A4 <: AnyRef : ClassTag, A5 <: AnyRef : ClassTag, A6 <: AnyRef : ClassTag, A7 <: AnyRef : ClassTag, A8 <: AnyRef : ClassTag, A9 <: AnyRef : ClassTag, A10 <: AnyRef : ClassTag, A11 <: AnyRef : ClassTag, A12 <: AnyRef : ClassTag, A13 <: AnyRef : ClassTag, A14 <: AnyRef : ClassTag, R](name: String, a1: A1, a2: A2, a3: A3, a4: A4, a5: A5, a6: A6, a7: A7, a8: A8, a9: A9, a10: A10, a11: A11, a12: A12, a13: A13, a14: A14): R = {
      c.getDeclaredMethod(name, classTag[A1].erasure, classTag[A2].erasure, classTag[A3].erasure, classTag[A4].erasure, classTag[A5].erasure, classTag[A6].erasure, classTag[A7].erasure, classTag[A8].erasure, classTag[A9].erasure, classTag[A10].erasure, classTag[A11].erasure, classTag[A12].erasure, classTag[A13].erasure, classTag[A14].erasure).invoke(c, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14).asInstanceOf[R]
    }
    def callStatic[A1 <: AnyRef : ClassTag, A2 <: AnyRef : ClassTag, A3 <: AnyRef : ClassTag, A4 <: AnyRef : ClassTag, A5 <: AnyRef : ClassTag, A6 <: AnyRef : ClassTag, A7 <: AnyRef : ClassTag, A8 <: AnyRef : ClassTag, A9 <: AnyRef : ClassTag, A10 <: AnyRef : ClassTag, A11 <: AnyRef : ClassTag, A12 <: AnyRef : ClassTag, A13 <: AnyRef : ClassTag, A14 <: AnyRef : ClassTag, A15 <: AnyRef : ClassTag, R](name: String, a1: A1, a2: A2, a3: A3, a4: A4, a5: A5, a6: A6, a7: A7, a8: A8, a9: A9, a10: A10, a11: A11, a12: A12, a13: A13, a14: A14, a15: A15): R = {
      c.getDeclaredMethod(name, classTag[A1].erasure, classTag[A2].erasure, classTag[A3].erasure, classTag[A4].erasure, classTag[A5].erasure, classTag[A6].erasure, classTag[A7].erasure, classTag[A8].erasure, classTag[A9].erasure, classTag[A10].erasure, classTag[A11].erasure, classTag[A12].erasure, classTag[A13].erasure, classTag[A14].erasure, classTag[A15].erasure).invoke(c, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15).asInstanceOf[R]
    }
    def callStatic[A1 <: AnyRef : ClassTag, A2 <: AnyRef : ClassTag, A3 <: AnyRef : ClassTag, A4 <: AnyRef : ClassTag, A5 <: AnyRef : ClassTag, A6 <: AnyRef : ClassTag, A7 <: AnyRef : ClassTag, A8 <: AnyRef : ClassTag, A9 <: AnyRef : ClassTag, A10 <: AnyRef : ClassTag, A11 <: AnyRef : ClassTag, A12 <: AnyRef : ClassTag, A13 <: AnyRef : ClassTag, A14 <: AnyRef : ClassTag, A15 <: AnyRef : ClassTag, A16 <: AnyRef : ClassTag, R](name: String, a1: A1, a2: A2, a3: A3, a4: A4, a5: A5, a6: A6, a7: A7, a8: A8, a9: A9, a10: A10, a11: A11, a12: A12, a13: A13, a14: A14, a15: A15, a16: A16): R = {
      c.getDeclaredMethod(name, classTag[A1].erasure, classTag[A2].erasure, classTag[A3].erasure, classTag[A4].erasure, classTag[A5].erasure, classTag[A6].erasure, classTag[A7].erasure, classTag[A8].erasure, classTag[A9].erasure, classTag[A10].erasure, classTag[A11].erasure, classTag[A12].erasure, classTag[A13].erasure, classTag[A14].erasure, classTag[A15].erasure, classTag[A16].erasure).invoke(c, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16).asInstanceOf[R]
    }
    def callStatic[A1 <: AnyRef : ClassTag, A2 <: AnyRef : ClassTag, A3 <: AnyRef : ClassTag, A4 <: AnyRef : ClassTag, A5 <: AnyRef : ClassTag, A6 <: AnyRef : ClassTag, A7 <: AnyRef : ClassTag, A8 <: AnyRef : ClassTag, A9 <: AnyRef : ClassTag, A10 <: AnyRef : ClassTag, A11 <: AnyRef : ClassTag, A12 <: AnyRef : ClassTag, A13 <: AnyRef : ClassTag, A14 <: AnyRef : ClassTag, A15 <: AnyRef : ClassTag, A16 <: AnyRef : ClassTag, A17 <: AnyRef : ClassTag, R](name: String, a1: A1, a2: A2, a3: A3, a4: A4, a5: A5, a6: A6, a7: A7, a8: A8, a9: A9, a10: A10, a11: A11, a12: A12, a13: A13, a14: A14, a15: A15, a16: A16, a17: A17): R = {
      c.getDeclaredMethod(name, classTag[A1].erasure, classTag[A2].erasure, classTag[A3].erasure, classTag[A4].erasure, classTag[A5].erasure, classTag[A6].erasure, classTag[A7].erasure, classTag[A8].erasure, classTag[A9].erasure, classTag[A10].erasure, classTag[A11].erasure, classTag[A12].erasure, classTag[A13].erasure, classTag[A14].erasure, classTag[A15].erasure, classTag[A16].erasure, classTag[A17].erasure).invoke(c, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17).asInstanceOf[R]
    }
    def callStatic[A1 <: AnyRef : ClassTag, A2 <: AnyRef : ClassTag, A3 <: AnyRef : ClassTag, A4 <: AnyRef : ClassTag, A5 <: AnyRef : ClassTag, A6 <: AnyRef : ClassTag, A7 <: AnyRef : ClassTag, A8 <: AnyRef : ClassTag, A9 <: AnyRef : ClassTag, A10 <: AnyRef : ClassTag, A11 <: AnyRef : ClassTag, A12 <: AnyRef : ClassTag, A13 <: AnyRef : ClassTag, A14 <: AnyRef : ClassTag, A15 <: AnyRef : ClassTag, A16 <: AnyRef : ClassTag, A17 <: AnyRef : ClassTag, A18 <: AnyRef : ClassTag, R](name: String, a1: A1, a2: A2, a3: A3, a4: A4, a5: A5, a6: A6, a7: A7, a8: A8, a9: A9, a10: A10, a11: A11, a12: A12, a13: A13, a14: A14, a15: A15, a16: A16, a17: A17, a18: A18): R = {
      c.getDeclaredMethod(name, classTag[A1].erasure, classTag[A2].erasure, classTag[A3].erasure, classTag[A4].erasure, classTag[A5].erasure, classTag[A6].erasure, classTag[A7].erasure, classTag[A8].erasure, classTag[A9].erasure, classTag[A10].erasure, classTag[A11].erasure, classTag[A12].erasure, classTag[A13].erasure, classTag[A14].erasure, classTag[A15].erasure, classTag[A16].erasure, classTag[A17].erasure, classTag[A18].erasure).invoke(c, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18).asInstanceOf[R]
    }
    def callStatic[A1 <: AnyRef : ClassTag, A2 <: AnyRef : ClassTag, A3 <: AnyRef : ClassTag, A4 <: AnyRef : ClassTag, A5 <: AnyRef : ClassTag, A6 <: AnyRef : ClassTag, A7 <: AnyRef : ClassTag, A8 <: AnyRef : ClassTag, A9 <: AnyRef : ClassTag, A10 <: AnyRef : ClassTag, A11 <: AnyRef : ClassTag, A12 <: AnyRef : ClassTag, A13 <: AnyRef : ClassTag, A14 <: AnyRef : ClassTag, A15 <: AnyRef : ClassTag, A16 <: AnyRef : ClassTag, A17 <: AnyRef : ClassTag, A18 <: AnyRef : ClassTag, A19 <: AnyRef : ClassTag, R](name: String, a1: A1, a2: A2, a3: A3, a4: A4, a5: A5, a6: A6, a7: A7, a8: A8, a9: A9, a10: A10, a11: A11, a12: A12, a13: A13, a14: A14, a15: A15, a16: A16, a17: A17, a18: A18, a19: A19): R = {
      c.getDeclaredMethod(name, classTag[A1].erasure, classTag[A2].erasure, classTag[A3].erasure, classTag[A4].erasure, classTag[A5].erasure, classTag[A6].erasure, classTag[A7].erasure, classTag[A8].erasure, classTag[A9].erasure, classTag[A10].erasure, classTag[A11].erasure, classTag[A12].erasure, classTag[A13].erasure, classTag[A14].erasure, classTag[A15].erasure, classTag[A16].erasure, classTag[A17].erasure, classTag[A18].erasure, classTag[A19].erasure).invoke(c, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19).asInstanceOf[R]
    }
    def callStatic[A1 <: AnyRef : ClassTag, A2 <: AnyRef : ClassTag, A3 <: AnyRef : ClassTag, A4 <: AnyRef : ClassTag, A5 <: AnyRef : ClassTag, A6 <: AnyRef : ClassTag, A7 <: AnyRef : ClassTag, A8 <: AnyRef : ClassTag, A9 <: AnyRef : ClassTag, A10 <: AnyRef : ClassTag, A11 <: AnyRef : ClassTag, A12 <: AnyRef : ClassTag, A13 <: AnyRef : ClassTag, A14 <: AnyRef : ClassTag, A15 <: AnyRef : ClassTag, A16 <: AnyRef : ClassTag, A17 <: AnyRef : ClassTag, A18 <: AnyRef : ClassTag, A19 <: AnyRef : ClassTag, A20 <: AnyRef : ClassTag, R](name: String, a1: A1, a2: A2, a3: A3, a4: A4, a5: A5, a6: A6, a7: A7, a8: A8, a9: A9, a10: A10, a11: A11, a12: A12, a13: A13, a14: A14, a15: A15, a16: A16, a17: A17, a18: A18, a19: A19, a20: A20): R = {
      c.getDeclaredMethod(name, classTag[A1].erasure, classTag[A2].erasure, classTag[A3].erasure, classTag[A4].erasure, classTag[A5].erasure, classTag[A6].erasure, classTag[A7].erasure, classTag[A8].erasure, classTag[A9].erasure, classTag[A10].erasure, classTag[A11].erasure, classTag[A12].erasure, classTag[A13].erasure, classTag[A14].erasure, classTag[A15].erasure, classTag[A16].erasure, classTag[A17].erasure, classTag[A18].erasure, classTag[A19].erasure, classTag[A20].erasure).invoke(c, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19, a20).asInstanceOf[R]
    }
    def callStatic[A1 <: AnyRef : ClassTag, A2 <: AnyRef : ClassTag, A3 <: AnyRef : ClassTag, A4 <: AnyRef : ClassTag, A5 <: AnyRef : ClassTag, A6 <: AnyRef : ClassTag, A7 <: AnyRef : ClassTag, A8 <: AnyRef : ClassTag, A9 <: AnyRef : ClassTag, A10 <: AnyRef : ClassTag, A11 <: AnyRef : ClassTag, A12 <: AnyRef : ClassTag, A13 <: AnyRef : ClassTag, A14 <: AnyRef : ClassTag, A15 <: AnyRef : ClassTag, A16 <: AnyRef : ClassTag, A17 <: AnyRef : ClassTag, A18 <: AnyRef : ClassTag, A19 <: AnyRef : ClassTag, A20 <: AnyRef : ClassTag, A21 <: AnyRef : ClassTag, R](name: String, a1: A1, a2: A2, a3: A3, a4: A4, a5: A5, a6: A6, a7: A7, a8: A8, a9: A9, a10: A10, a11: A11, a12: A12, a13: A13, a14: A14, a15: A15, a16: A16, a17: A17, a18: A18, a19: A19, a20: A20, a21: A21): R = {
      c.getDeclaredMethod(name, classTag[A1].erasure, classTag[A2].erasure, classTag[A3].erasure, classTag[A4].erasure, classTag[A5].erasure, classTag[A6].erasure, classTag[A7].erasure, classTag[A8].erasure, classTag[A9].erasure, classTag[A10].erasure, classTag[A11].erasure, classTag[A12].erasure, classTag[A13].erasure, classTag[A14].erasure, classTag[A15].erasure, classTag[A16].erasure, classTag[A17].erasure, classTag[A18].erasure, classTag[A19].erasure, classTag[A20].erasure, classTag[A21].erasure).invoke(c, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21).asInstanceOf[R]
    }
    def callStatic[A1 <: AnyRef : ClassTag, A2 <: AnyRef : ClassTag, A3 <: AnyRef : ClassTag, A4 <: AnyRef : ClassTag, A5 <: AnyRef : ClassTag, A6 <: AnyRef : ClassTag, A7 <: AnyRef : ClassTag, A8 <: AnyRef : ClassTag, A9 <: AnyRef : ClassTag, A10 <: AnyRef : ClassTag, A11 <: AnyRef : ClassTag, A12 <: AnyRef : ClassTag, A13 <: AnyRef : ClassTag, A14 <: AnyRef : ClassTag, A15 <: AnyRef : ClassTag, A16 <: AnyRef : ClassTag, A17 <: AnyRef : ClassTag, A18 <: AnyRef : ClassTag, A19 <: AnyRef : ClassTag, A20 <: AnyRef : ClassTag, A21 <: AnyRef : ClassTag, A22 <: AnyRef : ClassTag, R](name: String, a1: A1, a2: A2, a3: A3, a4: A4, a5: A5, a6: A6, a7: A7, a8: A8, a9: A9, a10: A10, a11: A11, a12: A12, a13: A13, a14: A14, a15: A15, a16: A16, a17: A17, a18: A18, a19: A19, a20: A20, a21: A21, a22: A22): R = {
      c.getDeclaredMethod(name, classTag[A1].erasure, classTag[A2].erasure, classTag[A3].erasure, classTag[A4].erasure, classTag[A5].erasure, classTag[A6].erasure, classTag[A7].erasure, classTag[A8].erasure, classTag[A9].erasure, classTag[A10].erasure, classTag[A11].erasure, classTag[A12].erasure, classTag[A13].erasure, classTag[A14].erasure, classTag[A15].erasure, classTag[A16].erasure, classTag[A17].erasure, classTag[A18].erasure, classTag[A19].erasure, classTag[A20].erasure, classTag[A21].erasure, classTag[A22].erasure).invoke(c, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22).asInstanceOf[R]
    }
  }
  // scalastyle:on
}
