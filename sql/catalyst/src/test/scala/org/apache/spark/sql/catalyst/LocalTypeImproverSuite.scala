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

import scala.reflect.classTag
import scala.reflect.runtime.{currentMirror => rm}
import scala.reflect.runtime.universe.{internal, symbolOf, weakTypeOf}

import org.scalatest.funsuite.AnyFunSuite

class LocalTypeImproverSuite extends AnyFunSuite {
  test("LocalTypeImprover") {
    case class Local()

    val expected = internal.typeRef(
      internal.thisType(symbolOf[LocalTypeImproverSuite]),
      rm.classSymbol(classOf[Local]),
      Nil
    )

    assert(
      LocalTypeImprover.improveStaticType[Local] =:= expected,
      "improveStaticType"
    )

    assert(
      LocalTypeImprover.improveDynamicType(
        weakTypeOf[Local],
        ClassTagApplication(classTag[Local], Nil)
      ) =:= expected,
      "improveDynamicType: weakTypeOf"
    )

    assert(
      LocalTypeImprover.improveDynamicType(
        internal.typeRef(
          internal.thisType(symbolOf[LocalTypeImproverSuite]),
          symbolOf[Local],
          Nil
        ),
        ClassTagApplication(classTag[Local], Nil)
      ) =:= expected,
      "improveDynamicType: manual"
    )
  }
}
