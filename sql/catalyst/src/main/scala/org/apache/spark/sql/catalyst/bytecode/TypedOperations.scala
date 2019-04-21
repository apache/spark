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

package org.apache.spark.sql.catalyst.bytecode

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.expressions.{Alias, Expression, GetStructField, If, Literal, NonSQLExpression}
import org.apache.spark.sql.catalyst.expressions.objects.{AssertNotNull, Invoke, LambdaVariable, MapObjects, NewInstance, StaticInvoke}
import org.apache.spark.sql.catalyst.plans.logical.{DeserializeToObject, Filter, MapElements, Project, SerializeFromObject, TypedFilter}
import org.apache.spark.sql.types.{BooleanType, ObjectType, StringType, StructType}
import org.apache.spark.util.Utils

object TypedOperations extends Logging {

  /**
   * Converts a typed map transformation with a user-defined closure to an equivalent untyped
   * map operation with Catalyst expressions by analyzing the closure bytecode.
   *
   * If the conversion cannot be performed, the result will be [[None]].
   *
   * @param serialize converts output of the typed map transformation into unsafe rows.
   * @param map the typed map transformation to apply.
   * @param deserialize converts unsafe rows into objects for the typed map transformation.
   * @return an optional [[Project]] with native Catalyst expressions.
   */
  def convertMapElements(
      serialize: SerializeFromObject,
      map: MapElements,
      deserialize: DeserializeToObject): Option[Project] = {
    try {
      val argClass = map.argumentClass
      val returnClass = getClass(map.outputObjAttr)
      val method = ClosureUtils.getMethod(map.func, argClass, returnClass)

      val thisRef = getThisRef(method)
      val argExpr = getArgExpr(method, deserialize.deserializer, argClass)
      val localVars = newLocalVarArray(method, thisRef, Seq(argExpr))

      val outputAttrs = serialize.output
      val untypedMapExpr = BehaviorConverter.convert(method, localVars)
      untypedMapExpr.flatMap {
        case e: Ref if e.dataType.isInstanceOf[StructType] =>
          val structType = e.dataType.asInstanceOf[StructType]
          val structExpr = resolveRefs(e)
          val aliases = outputAttrs.map { attr =>
            val fieldIndex = structType.fieldIndex(attr.name)
            Alias(GetStructField(structExpr, fieldIndex), attr.name)(exprId = attr.exprId)
          }
          Some(Project(aliases, deserialize.child))
        case e if outputAttrs.size == 1 && outputAttrs.head.dataType == e.dataType =>
          val outAttr = outputAttrs.head
          val alias = Alias(resolveRefs(e), outAttr.name)(exprId = outAttr.exprId)
          Some(Project(Seq(alias), deserialize.child))
        case e =>
          logWarning(s"$e cannot represent $outputAttrs")
          None
      }
    } catch {
      case e: Exception =>
        logWarning("Could not convert the typed map operation", e)
        None
    }
  }

  /**
   * Converts a typed filter with a user-defined closure to an equivalent untyped
   * filter with Catalyst expressions by analyzing the closure bytecode.
   *
   * If the conversion cannot be performed, the result will be [[None]].
   *
   * @param typedFilter the typed filter to convert.
   * @return an optional [[Filter]] with native Catalyst expressions.
   */
  def convertFilter(typedFilter: TypedFilter): Option[Filter] = {
    try {
      val argClass = typedFilter.argumentClass
      val returnClass = ScalaReflection.dataTypeJavaClass(BooleanType)
      val method = ClosureUtils.getMethod(typedFilter.func, argClass, returnClass)

      val thisRef = getThisRef(method)
      val argExpr = getArgExpr(method, typedFilter.deserializer, argClass)
      val localVars = newLocalVarArray(method, thisRef, Seq(argExpr))

      val untypedFilterExpr = BehaviorConverter.convert(method, localVars)
      untypedFilterExpr.foreach(e => require(e.dataType == BooleanType))
      untypedFilterExpr.map(e => Filter(resolveRefs(e), typedFilter.child))
    } catch {
      case e: Exception =>
        logWarning("Could not convert the typed filter", e)
        None
    }
  }

  private def getThisRef(behavior: Behavior): Option[Expression] = behavior match {
    case b: Behavior if b.isStatic =>
      None
    case b: Behavior if b.isConstructor =>
      val clazz = Utils.classForName(behavior.declaringClass.getName)
      Some(StructRef(clazz))
    case _ =>
      throw new RuntimeException("instance methods are not supported now")
  }

  private def getArgExpr(
      method: Behavior,
      deserializer: Expression,
      argClass: Class[_]): Expression = {

    val value = resolveRefs(convertDeserializer(deserializer))
    // we don't support outer variables so the number of params will be always 1 in our case
    val Array(paramType) = method.parameterTypes
    require(argClass.isPrimitive == paramType.isPrimitive)
    if (!argClass.isPrimitive) {
      // it is safe to assume argClass is precise because Datasets in Spark are not polymorphic
      ObjectRef(value, argClass)
    } else {
      value
    }
  }

  private def convertDeserializer(deserializer: Expression): Expression = deserializer transformUp {
    case Invoke(targetObject, methodName, outType, args, _, _) =>
      val argClasses = getClasses(args)
      val returnClass = ScalaReflection.dataTypeJavaClass(outType)
      val methodDescriptor = ClosureUtils.getMethodDescriptor(argClasses, returnClass)
      val clazz = ScalaReflection.dataTypeJavaClass(targetObject.dataType)
      val ctClass = CtClassPool.getCtClass(clazz)
      val method = ctClass.getMethod(methodName, methodDescriptor)
      val thisRef = Some(targetObject)
      val localVars = newLocalVarArray(method, thisRef, args)
      BehaviorConverter.convert(method, localVars).get
    case StaticInvoke(clazz, outType, methodName, args, _, _) =>
      val argClasses = getClasses(args)
      val returnClass = ScalaReflection.dataTypeJavaClass(outType)
      val methodDescriptor = ClosureUtils.getMethodDescriptor(argClasses, returnClass)
      val ctClass = CtClassPool.getCtClass(clazz)
      val method = ctClass.getMethod(methodName, methodDescriptor)
      val localVars = newLocalVarArray(method, None, args)
      BehaviorConverter.convert(method, localVars).get
    case NewInstance(clazz, args, _, _, _) =>
      val argClasses = getClasses(args)
      val constructorDescriptor = ClosureUtils.getConstructorDescriptor(argClasses)
      val ctClass = CtClassPool.getCtClass(clazz)
      val constructor = ctClass.getConstructor(constructorDescriptor)
      val thisRef = getThisRef(constructor)
      val localVars = newLocalVarArray(constructor, thisRef, args)
      BehaviorConverter.convert(constructor, localVars)
      // return initialized `this` that was passed to the constructor
      thisRef.get
    case l @ Literal(null, ObjectType(clazz)) =>
      // we need to transform null object literals inside deserializer expressions into StructRefs,
      // as invoke-like expressions such as `newInstance(class com.user.Item)` are also
      // converted into StructRefs and we must preserve the same data types for all if branches.
      // e.g., consider an example below, where both `if` branches must have the same data type.
      // `if (isnull(item#10)) null else newInstance(class com.user.Item)`
      ScalaReflection.schemaFor(clazz).dataType match {
        case st: StructType => ObjectRef(Literal(null, st), clazz)
        case _ => l
      }
    case e: AssertNotNull => e
    case e: LambdaVariable => e
    case e: MapObjects => e
    case e: NonSQLExpression =>
      throw new RuntimeException(s"$e is not supported in the deserializer")
  }

  private def getClasses(exprs: Seq[Expression]): Seq[Class[_]] = exprs.map(getClass)

  private def getClass(expr: Expression): Class[_] = expr match {
    case If(_, trueValue, falseValue) =>
      val trueValueClass = getClass(trueValue)
      val falseValueClass = getClass(falseValue)
      require(trueValueClass == falseValueClass)
      trueValueClass
    case e: Ref =>
      e.clazz
    case e if e.dataType == StringType =>
      classOf[String]
    case other =>
      ScalaReflection.dataTypeJavaClass(other.dataType)
  }
}
