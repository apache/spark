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

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.lang.invoke.{MethodHandleInfo, SerializedLambda}

import scala.collection.JavaConverters._
import scala.collection.mutable.{Map, Set, Stack}
import scala.language.existentials

import org.apache.commons.lang3.ClassUtils
import org.apache.xbean.asm6.{ClassReader, ClassVisitor, Handle, MethodVisitor, Type}
import org.apache.xbean.asm6.Opcodes._
import org.apache.xbean.asm6.tree.{ClassNode, MethodNode}

import org.apache.spark.{SparkEnv, SparkException}
import org.apache.spark.internal.Logging

/**
 * A cleaner that renders closures serializable if they can be done so safely.
 */
private[spark] object ClosureCleaner extends Logging {

  // Get an ASM class reader for a given class from the JAR that loaded it
  private[util] def getClassReader(cls: Class[_]): ClassReader = {
    // Copy data over, before delegating to ClassReader - else we can run out of open file handles.
    val className = cls.getName.replaceFirst("^.*\\.", "") + ".class"
    val resourceStream = cls.getResourceAsStream(className)
    if (resourceStream == null) {
      null
    } else {
      val baos = new ByteArrayOutputStream(128)
      Utils.copyStream(resourceStream, baos, true)
      new ClassReader(new ByteArrayInputStream(baos.toByteArray))
    }
  }

  // Check whether a class represents a Scala closure
  private def isClosure(cls: Class[_]): Boolean = {
    cls.getName.contains("$anonfun$")
  }

  // Get a list of the outer objects and their classes of a given closure object, obj;
  // the outer objects are defined as any closures that obj is nested within, plus
  // possibly the class that the outermost closure is in, if any. We stop searching
  // for outer objects beyond that because cloning the user's object is probably
  // not a good idea (whereas we can clone closure objects just fine since we
  // understand how all their fields are used).
  private def getOuterClassesAndObjects(obj: AnyRef): (List[Class[_]], List[AnyRef]) = {
    for (f <- obj.getClass.getDeclaredFields if f.getName == "$outer") {
      f.setAccessible(true)
      val outer = f.get(obj)
      // The outer pointer may be null if we have cleaned this closure before
      if (outer != null) {
        if (isClosure(f.getType)) {
          val recurRet = getOuterClassesAndObjects(outer)
          return (f.getType :: recurRet._1, outer :: recurRet._2)
        } else {
          return (f.getType :: Nil, outer :: Nil) // Stop at the first $outer that is not a closure
        }
      }
    }
    (Nil, Nil)
  }
  /**
   * Return a list of classes that represent closures enclosed in the given closure object.
   */
  private def getInnerClosureClasses(obj: AnyRef): List[Class[_]] = {
    val seen = Set[Class[_]](obj.getClass)
    val stack = Stack[Class[_]](obj.getClass)
    while (!stack.isEmpty) {
      val cr = getClassReader(stack.pop())
      if (cr != null) {
        val set = Set.empty[Class[_]]
        cr.accept(new InnerClosureFinder(set), 0)
        for (cls <- set -- seen) {
          seen += cls
          stack.push(cls)
        }
      }
    }
    (seen - obj.getClass).toList
  }

  /** Initializes the accessed fields for outer classes and their super classes. */
  private def initAccessedFields(
      accessedFields: Map[Class[_], Set[String]],
      outerClasses: Seq[Class[_]]): Unit = {
    for (cls <- outerClasses) {
      var currentClass = cls
      assert(currentClass != null, "The outer class can't be null.")

      while (currentClass != null) {
        accessedFields(currentClass) = Set.empty[String]
        currentClass = currentClass.getSuperclass()
      }
    }
  }

  /** Sets accessed fields for given class in clone object based on given object. */
  private def setAccessedFields(
      outerClass: Class[_],
      clone: AnyRef,
      obj: AnyRef,
      accessedFields: Map[Class[_], Set[String]]): Unit = {
    for (fieldName <- accessedFields(outerClass)) {
      val field = outerClass.getDeclaredField(fieldName)
      field.setAccessible(true)
      val value = field.get(obj)
      field.set(clone, value)
    }
  }

  /** Clones a given object and sets accessed fields in cloned object. */
  private def cloneAndSetFields(
      parent: AnyRef,
      obj: AnyRef,
      outerClass: Class[_],
      accessedFields: Map[Class[_], Set[String]]): AnyRef = {
    val clone = instantiateClass(outerClass, parent)

    var currentClass = outerClass
    assert(currentClass != null, "The outer class can't be null.")

    while (currentClass != null) {
      setAccessedFields(currentClass, clone, obj, accessedFields)
      currentClass = currentClass.getSuperclass()
    }

    clone
  }

  /**
   * Clean the given closure in place.
   *
   * More specifically, this renders the given closure serializable as long as it does not
   * explicitly reference unserializable objects.
   *
   * @param closure the closure to clean
   * @param checkSerializable whether to verify that the closure is serializable after cleaning
   * @param cleanTransitively whether to clean enclosing closures transitively
   */
  def clean(
      closure: AnyRef,
      checkSerializable: Boolean = true,
      cleanTransitively: Boolean = true): Unit = {
    clean(closure, checkSerializable, cleanTransitively, Map.empty)
  }

  /**
   * Helper method to clean the given closure in place.
   *
   * The mechanism is to traverse the hierarchy of enclosing closures and null out any
   * references along the way that are not actually used by the starting closure, but are
   * nevertheless included in the compiled anonymous classes. Note that it is unsafe to
   * simply mutate the enclosing closures in place, as other code paths may depend on them.
   * Instead, we clone each enclosing closure and set the parent pointers accordingly.
   *
   * By default, closures are cleaned transitively. This means we detect whether enclosing
   * objects are actually referenced by the starting one, either directly or transitively,
   * and, if not, sever these closures from the hierarchy. In other words, in addition to
   * nulling out unused field references, we also null out any parent pointers that refer
   * to enclosing objects not actually needed by the starting closure. We determine
   * transitivity by tracing through the tree of all methods ultimately invoked by the
   * inner closure and record all the fields referenced in the process.
   *
   * For instance, transitive cleaning is necessary in the following scenario:
   *
   *   class SomethingNotSerializable {
   *     def someValue = 1
   *     def scope(name: String)(body: => Unit) = body
   *     def someMethod(): Unit = scope("one") {
   *       def x = someValue
   *       def y = 2
   *       scope("two") { println(y + 1) }
   *     }
   *   }
   *
   * In this example, scope "two" is not serializable because it references scope "one", which
   * references SomethingNotSerializable. Note that, however, the body of scope "two" does not
   * actually depend on SomethingNotSerializable. This means we can safely null out the parent
   * pointer of a cloned scope "one" and set it the parent of scope "two", such that scope "two"
   * no longer references SomethingNotSerializable transitively.
   *
   * @param func the starting closure to clean
   * @param checkSerializable whether to verify that the closure is serializable after cleaning
   * @param cleanTransitively whether to clean enclosing closures transitively
   * @param accessedFields a map from a class to a set of its fields that are accessed by
   *                       the starting closure
   */
  private def clean(
      func: AnyRef,
      checkSerializable: Boolean,
      cleanTransitively: Boolean,
      accessedFields: Map[Class[_], Set[String]]): Unit = {

    // indylambda check. Most likely to be the case with 2.12, 2.13
    // so we check first
    // non LMF-closures should be less frequent from now on
    val maybeIndylambdaProxy = IndylambdaScalaClosures.getSerializationProxy(func)

    if (!isClosure(func.getClass) && maybeIndylambdaProxy.isEmpty) {
      logDebug(s"Expected a closure; got ${func.getClass.getName}")
      return
    }

    // TODO: clean all inner closures first. This requires us to find the inner objects.
    // TODO: cache outerClasses / innerClasses / accessedFields

    if (func == null) {
      return
    }

    if (maybeIndylambdaProxy.isEmpty) {
      logDebug(s"+++ Cleaning closure $func (${func.getClass.getName}) +++")

      // A list of classes that represents closures enclosed in the given one
      val innerClasses = getInnerClosureClasses(func)

      // A list of enclosing objects and their respective classes, from innermost to outermost
      // An outer object at a given index is of type outer class at the same index
      val (outerClasses, outerObjects) = getOuterClassesAndObjects(func)

      // For logging purposes only
      val declaredFields = func.getClass.getDeclaredFields
      val declaredMethods = func.getClass.getDeclaredMethods

      if (log.isDebugEnabled) {
        logDebug(s" + declared fields: ${declaredFields.size}")
        declaredFields.foreach { f => logDebug(s"     $f") }
        logDebug(s" + declared methods: ${declaredMethods.size}")
        declaredMethods.foreach { m => logDebug(s"     $m") }
        logDebug(s" + inner classes: ${innerClasses.size}")
        innerClasses.foreach { c => logDebug(s"     ${c.getName}") }
        logDebug(s" + outer classes: ${outerClasses.size}" )
        outerClasses.foreach { c => logDebug(s"     ${c.getName}") }
        logDebug(s" + outer objects: ${outerObjects.size}")
        outerObjects.foreach { o => logDebug(s"     $o") }
      }

      // Fail fast if we detect return statements in closures
      getClassReader(func.getClass).accept(new ReturnStatementFinder(), 0)

      // If accessed fields is not populated yet, we assume that
      // the closure we are trying to clean is the starting one
      if (accessedFields.isEmpty) {
        logDebug(" + populating accessed fields because this is the starting closure")
        // Initialize accessed fields with the outer classes first
        // This step is needed to associate the fields to the correct classes later
        initAccessedFields(accessedFields, outerClasses)

        // Populate accessed fields by visiting all fields and methods accessed by this and
        // all of its inner closures. If transitive cleaning is enabled, this may recursively
        // visits methods that belong to other classes in search of transitively referenced fields.
        for (cls <- func.getClass :: innerClasses) {
          getClassReader(cls).accept(new FieldAccessFinder(accessedFields, cleanTransitively), 0)
        }
      }

      logDebug(s" + fields accessed by starting closure: ${accessedFields.size} classes")
      accessedFields.foreach { f => logDebug("     " + f) }

      // List of outer (class, object) pairs, ordered from outermost to innermost
      // Note that all outer objects but the outermost one (first one in this list) must be closures
      var outerPairs: List[(Class[_], AnyRef)] = outerClasses.zip(outerObjects).reverse
      var parent: AnyRef = null
      if (outerPairs.nonEmpty) {
        val (outermostClass, outermostObject) = outerPairs.head
        if (isClosure(outermostClass)) {
          logDebug(s" + outermost object is a closure, so we clone it: ${outerPairs.head}")
        } else if (outermostClass.getName.startsWith("$line")) {
          // SPARK-14558: if the outermost object is a REPL line object, we should clone
          // and clean it as it may carray a lot of unnecessary information,
          // e.g. hadoop conf, spark conf, etc.
          logDebug(s" + outermost object is a REPL line object, so we clone it: ${outerPairs.head}")
        } else {
          // The closure is ultimately nested inside a class; keep the object of that
          // class without cloning it since we don't want to clone the user's objects.
          // Note that we still need to keep around the outermost object itself because
          // we need it to clone its child closure later (see below).
          logDebug(" + outermost object is not a closure or REPL line object," +
            "so do not clone it: " +  outerPairs.head)
          parent = outermostObject // e.g. SparkContext
          outerPairs = outerPairs.tail
        }
      } else {
        logDebug(" + there are no enclosing objects!")
      }

      // Clone the closure objects themselves, nulling out any fields that are not
      // used in the closure we're working on or any of its inner closures.
      for ((cls, obj) <- outerPairs) {
        logDebug(s" + cloning the object $obj of class ${cls.getName}")
        // We null out these unused references by cloning each object and then filling in all
        // required fields from the original object. We need the parent here because the Java
        // language specification requires the first constructor parameter of any closure to be
        // its enclosing object.
        val clone = cloneAndSetFields(parent, obj, cls, accessedFields)

        // If transitive cleaning is enabled, we recursively clean any enclosing closure using
        // the already populated accessed fields map of the starting closure
        if (cleanTransitively && isClosure(clone.getClass)) {
          logDebug(s" + cleaning cloned closure $clone recursively (${cls.getName})")
          // No need to check serializable here for the outer closures because we're
          // only interested in the serializability of the starting closure
          clean(clone, checkSerializable = false, cleanTransitively, accessedFields)
        }
        parent = clone
      }

      // Update the parent pointer ($outer) of this closure
      if (parent != null) {
        val field = func.getClass.getDeclaredField("$outer")
        field.setAccessible(true)
        // If the starting closure doesn't actually need our enclosing object, then just null it out
        if (accessedFields.contains(func.getClass) &&
          !accessedFields(func.getClass).contains("$outer")) {
          logDebug(s" + the starting closure doesn't actually need $parent, so we null it out")
          field.set(func, null)
        } else {
          // Update this closure's parent pointer to point to our enclosing object,
          // which could either be a cloned closure or the original user object
          field.set(func, parent)
        }
      }

      logDebug(s" +++ closure $func (${func.getClass.getName}) is now cleaned +++")
    } else {
      val lambdaProxy = maybeIndylambdaProxy.get
      val implMethodName = lambdaProxy.getImplMethodName

      logDebug(s"Cleaning indylambda closure: $implMethodName")

      // capturing class is the class that declared this lambda
      val capturingClassName = lambdaProxy.getCapturingClass.replace('/', '.')
      val classLoader = func.getClass.getClassLoader // this is the safest option
      // scalastyle:off classforname
      val capturingClass = Class.forName(capturingClassName, false, classLoader)
      // scalastyle:on classforname

      // Fail fast if we detect return statements in closures
      val capturingClassReader = getClassReader(capturingClass)
      capturingClassReader.accept(new ReturnStatementFinder(Option(implMethodName)), 0)

      val isClosureDeclaredInScalaRepl = capturingClassName.startsWith("$line") &&
        capturingClassName.endsWith("$iw")
      val outerThisOpt = if (lambdaProxy.getCapturedArgCount > 0) {
        Option(lambdaProxy.getCapturedArg(0))
      } else {
        None
      }

      // only need to clean when there is an enclosing "this" captured by the closure, and it
      // should be something cleanable, i.e. a Scala REPL line object
      val needsCleaning = isClosureDeclaredInScalaRepl &&
        outerThisOpt.isDefined && outerThisOpt.get.getClass.getName == capturingClassName

      if (needsCleaning) {
        // indylambda closures do not reference enclosing closures via an `$outer` chain, so no
        // transitive cleaning on the `$outer` chain is needed.
        // Thus clean() shouldn't be recursively called with a non-empty accessedFields.
        assert(accessedFields.isEmpty)

        initAccessedFields(accessedFields, Seq(capturingClass))
        IndylambdaScalaClosures.findAccessedFields(
          lambdaProxy, classLoader, accessedFields, cleanTransitively)

        logDebug(s" + fields accessed by starting closure: ${accessedFields.size} classes")
        accessedFields.foreach { f => logDebug("     " + f) }

        if (accessedFields(capturingClass).size < capturingClass.getDeclaredFields.length) {
          // clone and clean the enclosing `this` only when there are fields to null out

          val outerThis = outerThisOpt.get

          logDebug(s" + cloning instance of REPL class $capturingClassName")
          val clonedOuterThis = cloneAndSetFields(
            parent = null, outerThis, capturingClass, accessedFields)

          val outerField = func.getClass.getDeclaredField("arg$1")
          outerField.setAccessible(true)
          outerField.set(func, clonedOuterThis)
        }
      }

      logDebug(s" +++ indylambda closure ($implMethodName) is now cleaned +++")
    }

    if (checkSerializable) {
      ensureSerializable(func)
    }
  }

  private def ensureSerializable(func: AnyRef) {
    try {
      if (SparkEnv.get != null) {
        SparkEnv.get.closureSerializer.newInstance().serialize(func)
      }
    } catch {
      case ex: Exception => throw new SparkException("Task not serializable", ex)
    }
  }

  private def instantiateClass(
      cls: Class[_],
      enclosingObject: AnyRef): AnyRef = {
    // Use reflection to instantiate object without calling constructor
    val rf = sun.reflect.ReflectionFactory.getReflectionFactory()
    val parentCtor = classOf[java.lang.Object].getDeclaredConstructor()
    val newCtor = rf.newConstructorForSerialization(cls, parentCtor)
    val obj = newCtor.newInstance().asInstanceOf[AnyRef]
    if (enclosingObject != null) {
      val field = cls.getDeclaredField("$outer")
      field.setAccessible(true)
      field.set(obj, enclosingObject)
    }
    obj
  }
}

private[spark] object IndylambdaScalaClosures extends Logging {
  private val isScala2_11 = scala.util.Properties.versionString.contains("2.11")

  // internal name of java.lang.invoke.LambdaMetafactory
  val LambdaMetafactoryClassName = "java/lang/invoke/LambdaMetafactory"
  // the method that Scala indylambda use for bootstrap method
  val LambdaMetafactoryMethodName = "altMetafactory"
  val LambdaMetafactoryMethodDesc = "(Ljava/lang/invoke/MethodHandles$Lookup;" +
    "Ljava/lang/String;Ljava/lang/invoke/MethodType;[Ljava/lang/Object;)" +
    "Ljava/lang/invoke/CallSite;"

  /**
   * Check if the given reference is a indylambda style Scala closure.
   * If so (e.g. for Scala 2.12+ closures), return a non-empty serialization proxy
   * (SerializedLambda) of the closure;
   * otherwise (e.g. for Scala 2.11 closures) return None.
   *
   * @param maybeClosure the closure to check.
   */
  def getSerializationProxy(maybeClosure: AnyRef): Option[SerializedLambda] = {
    if (isScala2_11) {
      // Keep existing behavior in Spark 2.4: assume Scala 2.11 doesn't use indylambda.
      // NOTE: It's actually possible to turn on indylambda in Scala 2.11 via delambdafy:inline,
      //       but that's not the default and we don't expect it to be in use.
      return None
    }

    def isClosureCandidate(cls: Class[_]): Boolean = {
      // TODO: maybe lift this restriction to support other functional interfaces in the future
      val implementedInterfaces = ClassUtils.getAllInterfaces(cls).asScala
      implementedInterfaces.exists(_.getName.startsWith("scala.Function"))
    }

    maybeClosure.getClass match {
      // shortcut the fast check:
      // 1. indylambda closure classes are generated by Java's LambdaMetafactory, and they're
      //    always synthetic.
      // 2. We only care about Serializable closures, so let's check that as well
      case c if !c.isSynthetic || !maybeClosure.isInstanceOf[Serializable] => None

      case c if isClosureCandidate(c) =>
        try {
          Option(inspect(maybeClosure)).filter(isIndylambdaScalaClosure)
        } catch {
          case e: Exception =>
            logDebug("The given reference is not an indylambda Scala closure.", e)
            None
        }

      case _ => None
    }
  }

  def isIndylambdaScalaClosure(lambdaProxy: SerializedLambda): Boolean = {
    lambdaProxy.getImplMethodKind == MethodHandleInfo.REF_invokeStatic &&
      lambdaProxy.getImplMethodName.contains("$anonfun$")
  }

  def inspect(closure: AnyRef): SerializedLambda = {
    val writeReplace = closure.getClass.getDeclaredMethod("writeReplace")
    writeReplace.setAccessible(true)
    writeReplace.invoke(closure).asInstanceOf[SerializedLambda]
  }

  /**
   * Check if the handle represents the LambdaMetafactory that indylambda Scala closures
   * use for creating the lambda class and getting a closure instance.
   */
  def isLambdaMetafactory(bsmHandle: Handle): Boolean = {
    bsmHandle.getOwner == LambdaMetafactoryClassName &&
      bsmHandle.getName == LambdaMetafactoryMethodName &&
      bsmHandle.getDesc == LambdaMetafactoryMethodDesc
  }

  /**
   * Check if the handle represents a target method that is:
   * - a STATIC method that implements a Scala lambda body in the indylambda style
   * - captures the enclosing `this`, i.e. the first argument is a reference to the same type as
   *   the owning class.
   * Returns true if both criteria above are met.
   */
  def isLambdaBodyCapturingOuter(handle: Handle, ownerInternalName: String): Boolean = {
    handle.getTag == H_INVOKESTATIC &&
      handle.getName.contains("$anonfun$") &&
      handle.getOwner == ownerInternalName &&
      handle.getDesc.startsWith(s"(L$ownerInternalName;")
  }

  /**
   * Check if the callee of a call site is a inner class constructor.
   * - A constructor has to be invoked via INVOKESPECIAL
   * - A constructor's internal name is "&lt;init&gt;" and the return type is "V" (void)
   * - An inner class' first argument in the signature has to be a reference to the
   *   enclosing "this", aka `$outer` in Scala.
   */
  def isInnerClassCtorCapturingOuter(
      op: Int, name: String, desc: String, callerInternalName: String): Boolean = {
    op == INVOKESPECIAL && name == "<init>" && desc.startsWith(s"(L$callerInternalName;")
  }

  /**
   * Scans an indylambda Scala closure, along with its lexically nested closures, and populate
   * the accessed fields info on which fields on the outer object are accessed.
   *
   * This is equivalent to getInnerClosureClasses() + InnerClosureFinder + FieldAccessFinder fused
   * into one for processing indylambda closures. The traversal order along the call graph is the
   * same for all three combined, so they can be fused together easily while maintaining the same
   * ordering as the existing implementation.
   *
   * Precondition: this function expects the `accessedFields` to be populated with all known
   *               outer classes and their super classes to be in the map as keys, e.g.
   *               initializing via ClosureCleaner.initAccessedFields.
   */
  // scalastyle:off line.size.limit
  // Example: run the following code snippet in a Spark Shell w/ Scala 2.12+:
  //   val topLevelValue = "someValue"; val closure = (j: Int) => {
  //     class InnerFoo {
  //       val innerClosure = (x: Int) => (1 to x).map { y => y + topLevelValue }
  //     }
  //     val innerFoo = new InnerFoo
  //     (1 to j).flatMap(innerFoo.innerClosure)
  //   }
  //   sc.parallelize(0 to 2).map(closure).collect
  //
  // produces the following trace-level logs:
  // (slightly simplified:
  //   - omitting the "ignoring ..." lines;
  //   - "$iw" is actually "$line14.$read$$iw$$iw$$iw$$iw$$iw$$iw$$iw$$iw";
  //   - "invokedynamic" lines are simplified to just show the name+desc, omitting the bsm info)
  //   Cleaning indylambda closure: $anonfun$closure$1$adapted
  //     scanning $iw.$anonfun$closure$1$adapted(L$iw;Ljava/lang/Object;)Lscala/collection/immutable/IndexedSeq;
  //       found intra class call to $iw.$anonfun$closure$1(L$iw;I)Lscala/collection/immutable/IndexedSeq;
  //     scanning $iw.$anonfun$closure$1(L$iw;I)Lscala/collection/immutable/IndexedSeq;
  //       found inner class $iw$InnerFoo$1
  //         found method innerClosure()Lscala/Function1;
  //         found method $anonfun$innerClosure$2(L$iw$InnerFoo$1;I)Ljava/lang/String;
  //         found method $anonfun$innerClosure$1(L$iw$InnerFoo$1;I)Lscala/collection/immutable/IndexedSeq;
  //         found method <init>(L$iw;)V
  //         found method $anonfun$innerClosure$2$adapted(L$iw$InnerFoo$1;Ljava/lang/Object;)Ljava/lang/String;
  //         found method $anonfun$innerClosure$1$adapted(L$iw$InnerFoo$1;Ljava/lang/Object;)Lscala/collection/immutable/IndexedSeq;
  //         found method $deserializeLambda$(Ljava/lang/invoke/SerializedLambda;)Ljava/lang/Object;
  //       found call to outer $iw$InnerFoo$1.innerClosure()Lscala/Function1;
  //     scanning $iw$InnerFoo$1.innerClosure()Lscala/Function1;
  //     scanning $iw$InnerFoo$1.$deserializeLambda$(Ljava/lang/invoke/SerializedLambda;)Ljava/lang/Object;
  //       invokedynamic: lambdaDeserialize(Ljava/lang/invoke/SerializedLambda;)Ljava/lang/Object;, bsm...)
  //     scanning $iw$InnerFoo$1.$anonfun$innerClosure$1$adapted(L$iw$InnerFoo$1;Ljava/lang/Object;)Lscala/collection/immutable/IndexedSeq;
  //       found intra class call to $iw$InnerFoo$1.$anonfun$innerClosure$1(L$iw$InnerFoo$1;I)Lscala/collection/immutable/IndexedSeq;
  //     scanning $iw$InnerFoo$1.$anonfun$innerClosure$1(L$iw$InnerFoo$1;I)Lscala/collection/immutable/IndexedSeq;
  //       invokedynamic: apply(L$iw$InnerFoo$1;)Lscala/Function1;, bsm...)
  //       found inner closure $iw$InnerFoo$1.$anonfun$innerClosure$2$adapted(L$iw$InnerFoo$1;Ljava/lang/Object;)Ljava/lang/String; (6)
  //     scanning $iw$InnerFoo$1.$anonfun$innerClosure$2$adapted(L$iw$InnerFoo$1;Ljava/lang/Object;)Ljava/lang/String;
  //       found intra class call to $iw$InnerFoo$1.$anonfun$innerClosure$2(L$iw$InnerFoo$1;I)Ljava/lang/String;
  //     scanning $iw$InnerFoo$1.$anonfun$innerClosure$2(L$iw$InnerFoo$1;I)Ljava/lang/String;
  //       found call to outer $iw.topLevelValue()Ljava/lang/String;
  //     scanning $iw.topLevelValue()Ljava/lang/String;
  //       found field access topLevelValue on $iw
  //     scanning $iw$InnerFoo$1.$anonfun$innerClosure$2$adapted(L$iw$InnerFoo$1;Ljava/lang/Object;)Ljava/lang/String;
  //       found intra class call to $iw$InnerFoo$1.$anonfun$innerClosure$2(L$iw$InnerFoo$1;I)Ljava/lang/String;
  //     scanning $iw$InnerFoo$1.<init>(L$iw;)V
  //       invokedynamic: apply(L$iw$InnerFoo$1;)Lscala/Function1;, bsm...)
  //       found inner closure $iw$InnerFoo$1.$anonfun$innerClosure$1$adapted(L$iw$InnerFoo$1;Ljava/lang/Object;)Lscala/collection/immutable/IndexedSeq; (6)
  //     scanning $iw$InnerFoo$1.$anonfun$innerClosure$1(L$iw$InnerFoo$1;I)Lscala/collection/immutable/IndexedSeq;
  //       invokedynamic: apply(L$iw$InnerFoo$1;)Lscala/Function1;, bsm...)
  //       found inner closure $iw$InnerFoo$1.$anonfun$innerClosure$2$adapted(L$iw$InnerFoo$1;Ljava/lang/Object;)Ljava/lang/String; (6)
  //     scanning $iw$InnerFoo$1.$anonfun$innerClosure$2(L$iw$InnerFoo$1;I)Ljava/lang/String;
  //       found call to outer $iw.topLevelValue()Ljava/lang/String;
  //     scanning $iw$InnerFoo$1.innerClosure()Lscala/Function1;
  //    + fields accessed by starting closure: 2 classes
  //        (class java.lang.Object,Set())
  //        (class $iw,Set(topLevelValue))
  //    + cloning instance of REPL class $iw
  //    +++ indylambda closure ($anonfun$closure$1$adapted) is now cleaned +++
  //
  // scalastyle:on line.size.limit
  def findAccessedFields(
      lambdaProxy: SerializedLambda,
      lambdaClassLoader: ClassLoader,
      accessedFields: Map[Class[_], Set[String]],
      findTransitively: Boolean): Unit = {

    // We may need to visit the same class multiple times for different methods on it, and we'll
    // need to lookup by name. So we use ASM's Tree API and cache the ClassNode/MethodNode.
    val classInfoByInternalName = Map.empty[String, (Class[_], ClassNode)]
    val methodNodeById = Map.empty[MethodIdentifier[_], MethodNode]
    def getOrUpdateClassInfo(classInternalName: String): (Class[_], ClassNode) = {
      val classInfo = classInfoByInternalName.getOrElseUpdate(classInternalName, {
        val classExternalName = classInternalName.replace('/', '.')
        // scalastyle:off classforname
        val clazz = Class.forName(classExternalName, false, lambdaClassLoader)
        // scalastyle:on classforname
        val classNode = new ClassNode()
        val classReader = ClosureCleaner.getClassReader(clazz)
        classReader.accept(classNode, 0)

        for (m <- classNode.methods.asScala) {
          methodNodeById(MethodIdentifier(clazz, m.name, m.desc)) = m
        }

        (clazz, classNode)
      })
      classInfo
    }

    val implClassInternalName = lambdaProxy.getImplClass
    val (implClass, _) = getOrUpdateClassInfo(implClassInternalName)

    val implMethodId = MethodIdentifier(
      implClass, lambdaProxy.getImplMethodName, lambdaProxy.getImplMethodSignature)

    // The set internal names of classes that we would consider following the calls into.
    // Candidates are: known outer class which happens to be the starting closure's impl class,
    // and all inner classes discovered below.
    // Note that code in an inner class can make calls to methods in any of its enclosing classes,
    // e.g.
    //   starting closure (in class T)
    //     inner class A
    //        inner class B
    //          inner closure
    // we need to track calls from "inner closure" to outer classes relative to it (class T, A, B)
    // to better find and track field accesses.
    val trackedClassInternalNames = Set[String](implClassInternalName)

    // Depth-first search for inner closures and track the fields that were accessed in them.
    // Start from the lambda body's implementation method, follow method invocations
    val visited = Set.empty[MethodIdentifier[_]]
    val stack = Stack[MethodIdentifier[_]](implMethodId)
    def pushIfNotVisited(methodId: MethodIdentifier[_]): Unit = {
      if (!visited.contains(methodId)) {
        stack.push(methodId)
      }
    }

    while (!stack.isEmpty) {
      val currentId = stack.pop
      visited += currentId

      val currentClass = currentId.cls
      val currentMethodNode = methodNodeById(currentId)
      logTrace(s"  scanning ${currentId.cls.getName}.${currentId.name}${currentId.desc}")
      currentMethodNode.accept(new MethodVisitor(ASM6) {
        val currentClassName = currentClass.getName
        val currentClassInternalName = currentClassName.replace('.', '/')

        // Find and update the accessedFields info. Only fields on known outer classes are tracked.
        // This is the FieldAccessFinder equivalent.
        override def visitFieldInsn(op: Int, owner: String, name: String, desc: String): Unit = {
          if (op == GETFIELD || op == PUTFIELD) {
            val ownerExternalName = owner.replace('/', '.')
            for (cl <- accessedFields.keys if cl.getName == ownerExternalName) {
              logTrace(s"    found field access $name on $ownerExternalName")
              accessedFields(cl) += name
            }
          }
        }

        override def visitMethodInsn(
            op: Int, owner: String, name: String, desc: String, itf: Boolean): Unit = {
          val ownerExternalName = owner.replace('/', '.')
          if (owner == currentClassInternalName) {
            logTrace(s"    found intra class call to $ownerExternalName.$name$desc")
            // could be invoking a helper method or a field accessor method, just follow it.
            pushIfNotVisited(MethodIdentifier(currentClass, name, desc))
          } else if (isInnerClassCtorCapturingOuter(op, name, desc, currentClassInternalName)) {
            // Discover inner classes.
            // This this the InnerClassFinder equivalent for inner classes, which still use the
            // `$outer` chain. So this is NOT controlled by the `findTransitively` flag.
            logDebug(s"    found inner class $ownerExternalName")
            // val innerClassInfo = getOrUpdateClassInfo(owner)
            val (innerClass, innerClassNode) = getOrUpdateClassInfo(owner)
            // val innerClass = innerClassInfo._1
            // val innerClassNode = innerClassInfo._2
            trackedClassInternalNames += owner
            // We need to visit all methods on the inner class so that we don't missing anything.
            for (m <- innerClassNode.methods.asScala) {
              logTrace(s"      found method ${m.name}${m.desc}")
              pushIfNotVisited(MethodIdentifier(innerClass, m.name, m.desc))
            }
          } else if (findTransitively && trackedClassInternalNames.contains(owner)) {
            logTrace(s"    found call to outer $ownerExternalName.$name$desc")
            val (calleeClass, _) = getOrUpdateClassInfo(owner) // make sure MethodNodes are cached
            pushIfNotVisited(MethodIdentifier(calleeClass, name, desc))
          } else {
            // keep the same behavior as the original ClosureCleaner
            logTrace(s"    ignoring call to $ownerExternalName.$name$desc")
          }
        }

        // Find the lexically nested closures
        // This is the InnerClosureFinder equivalent for indylambda nested closures
        override def visitInvokeDynamicInsn(
            name: String, desc: String, bsmHandle: Handle, bsmArgs: Object*): Unit = {
          logTrace(s"    invokedynamic: $name$desc, bsmHandle=$bsmHandle, bsmArgs=$bsmArgs")

          // fast check: we only care about Scala lambda creation
          // TODO: maybe lift this restriction and support other functional interfaces
          if (!name.startsWith("apply")) return
          if (!Type.getReturnType(desc).getDescriptor.startsWith("Lscala/Function")) return

          if (isLambdaMetafactory(bsmHandle)) {
            // OK we're in the right bootstrap method for serializable Java 8 style lambda creation
            val targetHandle = bsmArgs(1).asInstanceOf[Handle]
            if (isLambdaBodyCapturingOuter(targetHandle, currentClassInternalName)) {
              // this is a lexically nested closure that also captures the enclosing `this`
              logDebug(s"    found inner closure $targetHandle")
              val calleeMethodId =
                MethodIdentifier(currentClass, targetHandle.getName, targetHandle.getDesc)
              pushIfNotVisited(calleeMethodId)
            }
          }
        }
      })
    }
  }
}

private[spark] class ReturnStatementInClosureException
  extends SparkException("Return statements aren't allowed in Spark closures")

private class ReturnStatementFinder(targetMethodName: Option[String] = None)
  extends ClassVisitor(ASM6) {
  override def visitMethod(access: Int, name: String, desc: String,
      sig: String, exceptions: Array[String]): MethodVisitor = {

    // $anonfun$ covers indylambda closures
    if (name.contains("apply") || name.contains("$anonfun$")) {
      // A method with suffix "$adapted" will be generated in cases like
      // { _:Int => return; Seq()} but not { _:Int => return; true}
      // closure passed is $anonfun$t$1$adapted while actual code resides in $anonfun$s$1
      // visitor will see only $anonfun$s$1$adapted, so we remove the suffix, see
      // https://github.com/scala/scala-dev/issues/109
      val isTargetMethod = targetMethodName.isEmpty ||
        name == targetMethodName.get || name == targetMethodName.get.stripSuffix("$adapted")

      new MethodVisitor(ASM6) {
        override def visitTypeInsn(op: Int, tp: String) {
          if (op == NEW && tp.contains("scala/runtime/NonLocalReturnControl") && isTargetMethod) {
            throw new ReturnStatementInClosureException
          }
        }
      }
    } else {
      new MethodVisitor(ASM6) {}
    }
  }
}

/** Helper class to identify a method. */
private case class MethodIdentifier[T](cls: Class[T], name: String, desc: String)

/**
 * Find the fields accessed by a given class.
 *
 * The resulting fields are stored in the mutable map passed in through the constructor.
 * This map is assumed to have its keys already populated with the classes of interest.
 *
 * @param fields the mutable map that stores the fields to return
 * @param findTransitively if true, find fields indirectly referenced through method calls
 * @param specificMethod if not empty, visit only this specific method
 * @param visitedMethods a set of visited methods to avoid cycles
 */
private[util] class FieldAccessFinder(
    fields: Map[Class[_], Set[String]],
    findTransitively: Boolean,
    specificMethod: Option[MethodIdentifier[_]] = None,
    visitedMethods: Set[MethodIdentifier[_]] = Set.empty)
  extends ClassVisitor(ASM6) {

  override def visitMethod(
      access: Int,
      name: String,
      desc: String,
      sig: String,
      exceptions: Array[String]): MethodVisitor = {

    // If we are told to visit only a certain method and this is not the one, ignore it
    if (specificMethod.isDefined &&
        (specificMethod.get.name != name || specificMethod.get.desc != desc)) {
      return null
    }

    new MethodVisitor(ASM6) {
      override def visitFieldInsn(op: Int, owner: String, name: String, desc: String) {
        if (op == GETFIELD) {
          for (cl <- fields.keys if cl.getName == owner.replace('/', '.')) {
            fields(cl) += name
          }
        }
      }

      override def visitMethodInsn(
          op: Int, owner: String, name: String, desc: String, itf: Boolean) {
        for (cl <- fields.keys if cl.getName == owner.replace('/', '.')) {
          // Check for calls a getter method for a variable in an interpreter wrapper object.
          // This means that the corresponding field will be accessed, so we should save it.
          if (op == INVOKEVIRTUAL && owner.endsWith("$iwC") && !name.endsWith("$outer")) {
            fields(cl) += name
          }
          // Optionally visit other methods to find fields that are transitively referenced
          if (findTransitively) {
            val m = MethodIdentifier(cl, name, desc)
            if (!visitedMethods.contains(m)) {
              // Keep track of visited methods to avoid potential infinite cycles
              visitedMethods += m

              var currentClass = cl
              assert(currentClass != null, "The outer class can't be null.")

              while (currentClass != null) {
                ClosureCleaner.getClassReader(currentClass).accept(
                  new FieldAccessFinder(fields, findTransitively, Some(m), visitedMethods), 0)
                currentClass = currentClass.getSuperclass()
              }
            }
          }
        }
      }
    }
  }
}

private class InnerClosureFinder(output: Set[Class[_]]) extends ClassVisitor(ASM6) {
  var myName: String = null

  // TODO: Recursively find inner closures that we indirectly reference, e.g.
  //   val closure1 = () = { () => 1 }
  //   val closure2 = () => { (1 to 5).map(closure1) }
  // The second closure technically has two inner closures, but this finder only finds one

  override def visit(version: Int, access: Int, name: String, sig: String,
      superName: String, interfaces: Array[String]) {
    myName = name
  }

  override def visitMethod(access: Int, name: String, desc: String,
      sig: String, exceptions: Array[String]): MethodVisitor = {
    new MethodVisitor(ASM6) {
      override def visitMethodInsn(
          op: Int, owner: String, name: String, desc: String, itf: Boolean) {
        val argTypes = Type.getArgumentTypes(desc)
        if (op == INVOKESPECIAL && name == "<init>" && argTypes.length > 0
            && argTypes(0).toString.startsWith("L") // is it an object?
            && argTypes(0).getInternalName == myName) {
          // scalastyle:off classforname
          output += Class.forName(
              owner.replace('/', '.'),
              false,
              Thread.currentThread.getContextClassLoader)
          // scalastyle:on classforname
        }
      }
    }
  }
}
