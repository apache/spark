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

import scala.reflect.runtime.universe.{typeOf, NoPrefix, TypeRef}

import org.scalatest.funsuite.AnyFunSuite

class DeepDealiaserSuite extends AnyFunSuite {
  class Inner
  type InnerAlias = Inner

  test("deep dealias of inner class") {
    assert(DeepDealiaser.dealiasStaticType[InnerAlias] =:= typeOf[Inner])
  }

  class GenericInner[T]
  type IntGenInnerAlias = GenericInner[Int]
  type GenGenInnerAlias = GenericInner[IntGenInnerAlias]

  test("deep dealias of generic inner class") {
    assert(DeepDealiaser.dealiasStaticType[IntGenInnerAlias] =:= typeOf[GenericInner[Int]])
  }

  test("deep dealias of generic nested class with inner type argument") {
    assert(DeepDealiaser.dealiasStaticType[GenGenInnerAlias] =:=
      typeOf[GenericInner[GenericInner[Int]]])
  }

  test("deep dealias of local class") {
    class Local
    type LocalAlias = Local

    // a workaround because:
    //   weakTypeOf[Local] =:!= weakTypeOf[Local],
    //   symbolOf[Local] != symbolOf[Local]
    DeepDealiaser.dealiasStaticType[LocalAlias] match {
      case tr: TypeRef @unchecked =>
        assert(tr.pre =:= NoPrefix &&
          tr.sym.fullName == "<none>.Local")
    }
  }

  test("deep dealias of generic local class") {
    class GenericLocal[T]
    type IntGenLocalAlias = GenericLocal[Int]
    type GenGenLocalAlias = GenericLocal[IntGenLocalAlias]

    DeepDealiaser.dealiasStaticType[IntGenLocalAlias] match {
      case tr: TypeRef @unchecked =>
        assert(tr.pre =:= NoPrefix &&
          tr.sym.fullName == "<none>.GenericLocal" &&
          tr.args == List(typeOf[Int]), "generic local class")
    }

    DeepDealiaser.dealiasStaticType[GenGenLocalAlias] match {
      case tr: TypeRef @unchecked =>
        assert(tr.pre =:= NoPrefix &&
          tr.sym.fullName == "<none>.GenericLocal" &&
          tr.args.collect {case tr: TypeRef @unchecked =>
            (tr.pre, tr.sym.fullName, tr.args)
          } == List((NoPrefix, "<none>.GenericLocal", List(typeOf[Int]))),
          "generic local class with local type argument")
    }
  }
}
