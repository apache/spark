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
package org.apache.spark.sql.catalyst.expressions.objects

import scala.collection.mutable.{Builder, IndexedSeq, WrappedArray}

import org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext

object MapObjectsCodeGenHelper {

  def doCodeGenForWrappedArrayType(ctx: CodegenContext,
      cls: Class[_], dataLength: String): (String, String => String, String) = {
    // In Scala 2.13, newBuilder method need a Construction parameters of ClassTag type
    val getBuilder = s"${cls.getName}$$.MODULE$$.newBuilder(" +
      s"scala.reflect.ClassTag$$.MODULE$$.Object())"
    val builder = ctx.freshName("collectionBuilder")
    (
      s"""
       ${classOf[Builder[_, _]].getName} $builder = $getBuilder;
       $builder.sizeHint($dataLength);
     """,
      (genValue: String) => s"$builder.$$plus$$eq($genValue);",
      s"(${cls.getName})$builder.result();"
    )
  }
}
