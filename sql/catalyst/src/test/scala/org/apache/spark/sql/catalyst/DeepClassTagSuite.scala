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

package org.apache.spark.sql.catalyst

import scala.language.higherKinds
import scala.reflect.classTag

import org.scalatest.funsuite.AnyFunSuite

class DeepClassTagSuite extends AnyFunSuite {
  class A[b[_[_, _, _], _[_, _, _]]]
  class B[c[_, _, _], c1[_, _, _]]
  class C[d, d1, d2]
  class D

  type AB = A[B]
  type BC = B[C, C]
  type CD = C[D, D, D]

  // otherwise: avoid using structural types, Scalastyle confuses existential with structural types?
  // scalastyle:off
  type AExist = A[b] forSome { type b[_[_, _, _], _[_, _, _]] }
  type BExist = B[c, c1] forSome {type c[_, _, _]; type c1[_, _, _]}
  // scalastyle:on
  type CExist = C[_, _, _]

  val aCtag = classTag[AExist]
  val bCtag = classTag[BExist]
  val cCtag = classTag[CExist]
  val dCtag = classTag[D]

  val bApp = ClassTagApplication(bCtag, Nil)
  val cApp = ClassTagApplication(cCtag, Nil)
  val dApp = ClassTagApplication(dCtag, Nil)

  test("DeepClassTag, kind 3") {
    assert(DeepClassTag[AB].classTags ==
      ClassTagApplication(aCtag, List(bApp))
    )
  }

  test("DeepClassTag, kind 2") {
    assert(DeepClassTag[BC].classTags ==
      ClassTagApplication(bCtag, List(cApp, cApp))
    )
  }

  test("DeepClassTag, kind 1") {
    assert(DeepClassTag[CD].classTags ==
      ClassTagApplication(cCtag, List(dApp, dApp, dApp))
    )
  }
}
