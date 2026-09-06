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

package org.apache.spark.sql.catalyst.expressions.codegen

import java.io.File
import java.lang.reflect.Modifier
import java.net.{URI, URL, URLClassLoader}
import java.util.Collections
import javax.tools.{JavaFileObject, SimpleJavaFileObject, StandardLocation, ToolProvider}

import scala.jdk.CollectionConverters._

import org.codehaus.commons.compiler.CompileException
import org.mockito.Mockito.{mock, when}

import org.apache.spark.{JobArtifactSet, JobArtifactState, SparkConf, SparkEnv, SparkFunSuite}
import org.apache.spark.executor.ExecutorClassLoader
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{BoundReference, GreaterThan, LessThan, Literal}
import org.apache.spark.sql.catalyst.plans.SQLHelper
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.util.Utils

/**
 * Tests for the [[CodeCompiler]] trait, its backend selection, and behavioural
 * parity between the [[JaninoCodeCompiler]] and [[JdkCodeCompiler]] backends.
 */
class CodeCompilerSuite extends SparkFunSuite with SQLHelper {

  // A self-contained class body that exercises common shapes Spark's code generators
  // produce: an override of generate(), a nested concrete class, fields, and a
  // straightforward arithmetic method. Designed to compile under both backends with
  // no dependencies on Spark types beyond GeneratedClass.
  private val sampleClassBody: String =
    s"""
       |public java.lang.Object generate(Object[] references) {
       |  return new SpecificEvaluator(references);
       |}
       |
       |static class SpecificEvaluator {
       |  private Object[] references;
       |  private long counter = 0L;
       |
       |  public SpecificEvaluator(Object[] refs) {
       |    this.references = refs;
       |  }
       |
       |  public long evaluate(long input) {
       |    counter += 1L;
       |    return (input * 31L) + counter;
       |  }
       |}
       |""".stripMargin

  private def newCodeAndComment(body: String): CodeAndComment =
    new CodeAndComment(body, scala.collection.Map.empty[String, String])

  // ---------------- backend selection ----------------

  test("forBackend: 'janino' returns JaninoCodeCompiler") {
    assert(CodeCompiler.forBackend("janino") eq JaninoCodeCompiler)
  }

  test("forBackend: 'jdk' returns JdkCodeCompiler when available, else falls back") {
    val backend = CodeCompiler.forBackend("jdk")
    if (JdkCodeCompiler.isAvailable) {
      assert(backend eq JdkCodeCompiler)
    } else {
      assert(backend eq JaninoCodeCompiler)
    }
  }

  test("forBackend: name is case-insensitive") {
    assert(CodeCompiler.forBackend("JANINO") eq JaninoCodeCompiler)
    assert(CodeCompiler.forBackend("Janino") eq JaninoCodeCompiler)
    if (JdkCodeCompiler.isAvailable) {
      assert(CodeCompiler.forBackend("JDK") eq JdkCodeCompiler)
      assert(CodeCompiler.forBackend("Jdk") eq JdkCodeCompiler)
    }
  }

  test("forBackend: unknown name throws IllegalArgumentException") {
    val ex = intercept[IllegalArgumentException] {
      CodeCompiler.forBackend("acme-compiler")
    }
    assert(ex.getMessage.contains("acme-compiler"))
    assert(ex.getMessage.contains("janino"))
    assert(ex.getMessage.contains("jdk"))
  }

  test("active() honors SQLConf.CODEGEN_COMPILER") {
    withSQLConf(SQLConf.CODEGEN_COMPILER.key -> "janino") {
      assert(CodeCompiler.active().name == CodeCompiler.JANINO)
    }
    if (JdkCodeCompiler.isAvailable) {
      withSQLConf(SQLConf.CODEGEN_COMPILER.key -> "jdk") {
        assert(CodeCompiler.active().name == CodeCompiler.JDK)
      }
    }
  }

  test("active() routes REPL-context codegen to Janino regardless of config") {
    assume(JdkCodeCompiler.isAvailable, "javax.tools.JavaCompiler not available")
    // An ExecutorClassLoader in the active class loader chain marks REPL / interactive
    // codegen. Classes defined there carry self-inconsistent reflection metadata that the
    // JDK compiler cannot resolve, so the backend must deterministically route to Janino
    // even when `jdk` is configured. A `spark://` class URI keeps construction cheap: the
    // RPC fetch function is only referenced, never invoked, at construction time.
    val replLoader = new ExecutorClassLoader(
      new SparkConf(), null, "spark://localhost:0", getClass.getClassLoader, false)
    val childOfRepl = new URLClassLoader(Array.empty[URL], replLoader)
    val prev = Thread.currentThread().getContextClassLoader
    try {
      withSQLConf(SQLConf.CODEGEN_COMPILER.key -> "jdk") {
        // Baseline: an ordinary (non-REPL) loader honors the configured `jdk` backend.
        Thread.currentThread().setContextClassLoader(prev)
        assert(CodeCompiler.active() eq JdkCodeCompiler)
        // An ExecutorClassLoader at the head of the chain forces Janino.
        Thread.currentThread().setContextClassLoader(replLoader)
        assert(CodeCompiler.active() eq JaninoCodeCompiler)
        // An ExecutorClassLoader anywhere in the parent chain forces Janino too.
        Thread.currentThread().setContextClassLoader(childOfRepl)
        assert(CodeCompiler.active() eq JaninoCodeCompiler)
      }
    } finally {
      Thread.currentThread().setContextClassLoader(prev)
    }
  }

  test("active() routes artifact/REPL-session codegen (replClassDirUri) to Janino") {
    assume(JdkCodeCompiler.isAvailable, "javax.tools.JavaCompiler not available")
    withSQLConf(SQLConf.CODEGEN_COMPILER.key -> "jdk") {
      // Without an artifact class-dir URI, the configured `jdk` backend is honored.
      assert(CodeCompiler.active() eq JdkCodeCompiler)
      // A session/job carrying a `replClassDirUri` (Spark Connect per-session artifacts,
      // spark-shell) must route to Janino, including driver-side codegen where no
      // ExecutorClassLoader is in the loader chain (e.g. a Connect UDF over a local
      // relation referencing an Ammonite `$sess` class).
      JobArtifactSet.withActiveJobArtifactState(
        JobArtifactState("test-uuid", Some("spark://localhost:0/classes"))) {
        assert(CodeCompiler.active() eq JaninoCodeCompiler)
      }
    }
  }

  test("active() routes spark-shell codegen (spark.repl.class.uri in SparkEnv conf) to Janino") {
    assume(JdkCodeCompiler.isAvailable, "javax.tools.JavaCompiler not available")
    // spark-shell publishes the REPL class URI in the SparkEnv conf. That signal alone -
    // no ExecutorClassLoader in the chain, no artifact state - must route to Janino.
    val env = mock(classOf[SparkEnv])
    when(env.conf).thenReturn(
      new SparkConf().set("spark.repl.class.uri", "spark://localhost:0/classes"))
    val prevEnv = SparkEnv.get
    withSQLConf(SQLConf.CODEGEN_COMPILER.key -> "jdk") {
      // Baseline: without the REPL conf signal, the configured backend is honored.
      assert(CodeCompiler.active() eq JdkCodeCompiler)
      SparkEnv.set(env)
      try {
        assert(CodeCompiler.active() eq JaninoCodeCompiler)
      } finally {
        SparkEnv.set(prevEnv)
      }
    }
  }

  test("active(code) routes code referencing a package-object class to Janino") {
    assume(JdkCodeCompiler.isAvailable, "javax.tools.JavaCompiler not available")
    withSQLConf(SQLConf.CODEGEN_COMPILER.key -> "jdk") {
      // Ordinary code honors the configured jdk backend.
      assert(CodeCompiler.active(newCodeAndComment("int x = 1;")) eq JdkCodeCompiler)
      // A reference to a Scala `package object`'s nested class (`...package$Inner`) - whose
      // `package` segment the JDK compiler can name in no form - must route to Janino, the
      // same always-Janino bucket as REPL classes javac cannot name.
      val pkgObjBody = "org.apache.spark.sql.foo.package$Inner v = " +
        "(org.apache.spark.sql.foo.package$Inner) references[0];"
      assert(CodeCompiler.active(newCodeAndComment(pkgObjBody)) eq JaninoCodeCompiler)
      // A legal identifier that merely contains the text "package" is unaffected.
      assert(CodeCompiler.active(newCodeAndComment("com.mypackage.Inner v;")) eq JdkCodeCompiler)
    }
  }

  test("SQLConf rejects invalid backend names at set time") {
    val ex = intercept[IllegalArgumentException] {
      withSQLConf(SQLConf.CODEGEN_COMPILER.key -> "acme") {}
    }
    // SQLConf checkValues surfaces the allowed set in the message
    val msg = ex.getMessage.toLowerCase(java.util.Locale.ROOT)
    assert(msg.contains("janino") || msg.contains("jdk"))
  }

  // ---------------- compilation parity ----------------

  /**
   * Invoke `evaluate(long)` on the object returned by `generate()`. The nested
   * `SpecificEvaluator` class lives inside the generated `GeneratedClass`, and the
   * generated `GeneratedClass` is loaded by a different classloader than this test
   * suite. Even though the method is `public`, reflection requires explicit access
   * because the test cannot statically reach the enclosing class.
   */
  private def invokeEvaluate(result: Any, input: Long): Any = {
    val m = result.getClass.getMethod("evaluate", classOf[Long])
    m.setAccessible(true)
    m.invoke(result, Long.box(input))
  }

  test("Janino backend compiles a simple class body and produces working bytecode") {
    val (generated, stats) = JaninoCodeCompiler.compile(newCodeAndComment(sampleClassBody))
    assert(generated != null)
    val result = generated.generate(Array.empty[Any])
    assert(invokeEvaluate(result, 10L) == 311L)  // 10*31 + 1
    assert(invokeEvaluate(result, 20L) == 622L)  // 20*31 + 2
    assert(stats.maxMethodCodeSize > 0)
    assert(stats.maxConstPoolSize > 0)
  }

  test("JDK backend compiles the same class body and produces equivalent results") {
    assume(JdkCodeCompiler.isAvailable, "javax.tools.JavaCompiler not available")
    val (generated, stats) = JdkCodeCompiler.compile(newCodeAndComment(sampleClassBody))
    assert(generated != null)
    val result = generated.generate(Array.empty[Any])
    assert(invokeEvaluate(result, 10L) == 311L)
    assert(invokeEvaluate(result, 20L) == 622L)
    assert(stats.maxMethodCodeSize > 0)
    assert(stats.maxConstPoolSize > 0)
  }

  test("Both backends produce class bytecodes the ByteCodeStats parser accepts") {
    assume(JdkCodeCompiler.isAvailable, "javax.tools.JavaCompiler not available")
    val code = newCodeAndComment(sampleClassBody)
    val (_, janinoStats) = JaninoCodeCompiler.compile(code)
    val (_, jdkStats) = JdkCodeCompiler.compile(code)
    // Bytecode size will not be identical - the two compilers emit different instruction
    // sequences for the same source - but both must be non-trivial and non-error.
    assert(janinoStats.maxMethodCodeSize > 0)
    assert(jdkStats.maxMethodCodeSize > 0)
    assert(janinoStats.maxConstPoolSize > 0)
    assert(jdkStats.maxConstPoolSize > 0)
    // numInnerClasses must agree between backends. Both wrap the same body in a single
    // outer class declaration, so for K nested classes the formula `size - 2` yields the
    // same K-1 value under either backend.
    assert(janinoStats.numInnerClasses == jdkStats.numInnerClasses,
      s"numInnerClasses divergence: Janino=${janinoStats.numInnerClasses}, " +
        s"JDK=${jdkStats.numInnerClasses}")
  }

  test("Both backends compile a reference to a class nested in a Scala object") {
    assume(JdkCodeCompiler.isAvailable, "javax.tools.JavaCompiler not available")
    // End-to-end check of the mllib legacy save/load shape: `NewInstance` emits the
    // binary class name (CodeGenerator.javaSourceName == Class#getName). For a class
    // nested in a Scala object that name carries module `$`s (Outer$SaveLoadV1$Leaf),
    // and only that binary form resolves under javac - the dotted canonical form makes
    // javac reconstruct a non-existent Outer$SaveLoadV1$$Leaf. Both backends must
    // accept the same generated source.
    val binary = CodeGenerator.javaSourceName(classOf[CodeCompilerSuite.SaveLoadV1.Leaf])
    assert(binary.contains("$SaveLoadV1$"), s"fixture is not object-nested: $binary")
    val body =
      s"""
         |public java.lang.Object generate(Object[] references) {
         |  $binary leaf = new $binary(7);
         |  return Integer.valueOf(leaf.x());
         |}
         |""".stripMargin
    assert(JaninoCodeCompiler.compile(newCodeAndComment(body))._1 != null)
    val (generated, _) = JdkCodeCompiler.compile(newCodeAndComment(body))
    assert(generated.generate(Array.empty[Any]) === Integer.valueOf(7))
  }

  test("JDK backend resolves a class available only via the context classloader") {
    assume(JdkCodeCompiler.isAvailable, "javax.tools.JavaCompiler not available")
    // dyn.DynHelper is compiled into a temp dir that is NOT on java.class.path and is
    // served by a custom (non-URLClassLoader) loader - the shape of REPL-generated and
    // Spark Connect session-artifact classes, which live only on a runtime loader. The
    // JDK backend must resolve it the way Janino does (via the classloader), not via a
    // file-based -classpath.
    val dir = compileDynHelper()
    // DirClassLoader is deliberately a plain ClassLoader, not a URLClassLoader, so the
    // retired -classpath harvesting would never have found its classes.
    val loader = new DirClassLoader(dir, getClass.getClassLoader)
    assert(loader.loadClass("dyn.DynHelper") != null)

    val body =
      """
        |public java.lang.Object generate(Object[] references) {
        |  return Integer.valueOf(dyn.DynHelper.magic());
        |}
        |""".stripMargin
    val prev = Thread.currentThread().getContextClassLoader
    try {
      Thread.currentThread().setContextClassLoader(loader)
      // Both backends must resolve dyn.DynHelper through the context classloader.
      assert(JaninoCodeCompiler.compile(newCodeAndComment(body))._1 != null)
      val (generated, _) = JdkCodeCompiler.compile(newCodeAndComment(body))
      assert(generated.generate(Array.empty[Any]) === Integer.valueOf(4242))
    } finally {
      Thread.currentThread().setContextClassLoader(prev)
    }
  }

  test("JDK backend resolves a class from a non-enumerable (REPL-style) classloader") {
    assume(JdkCodeCompiler.isAvailable, "javax.tools.JavaCompiler not available")
    // A loader that serves classes by name (getResource / getResourceAsStream) but does
    // NOT support package enumeration (getResources) - the shape of the Scala REPL and
    // Spark Connect session loaders, which hold generated classes only in memory.
    // Resolution must fall back to the source's referenced names, the way Janino does.
    val dir = compileDynHelper()
    val loader = new NonEnumerableDirClassLoader(dir, getClass.getClassLoader)
    assert(!loader.getResources("dyn").hasMoreElements,
      "fixture loader must not support package enumeration")
    assert(loader.loadClass("dyn.DynHelper") != null)

    val body =
      """
        |public java.lang.Object generate(Object[] references) {
        |  return Integer.valueOf(dyn.DynHelper.magic());
        |}
        |""".stripMargin
    val prev = Thread.currentThread().getContextClassLoader
    try {
      Thread.currentThread().setContextClassLoader(loader)
      assert(JaninoCodeCompiler.compile(newCodeAndComment(body))._1 != null)
      val (generated, _) = JdkCodeCompiler.compile(newCodeAndComment(body))
      assert(generated.generate(Array.empty[Any]) === Integer.valueOf(4242))
    } finally {
      Thread.currentThread().setContextClassLoader(prev)
    }
  }

  test("JDK backend resolves a class from a getResourceAsStream-only classloader") {
    assume(JdkCodeCompiler.isAvailable, "javax.tools.JavaCompiler not available")
    // A loader that serves .class bytes only via getResourceAsStream and exposes no
    // resource URL (getResource is null) and no enumeration - the shape of the Scala
    // REPL / Ammonite (`ammonite.$sess`) and similar in-memory loaders. Resolution must
    // probe with getResourceAsStream, the way Janino does, not getResource.
    val dir = compileDynHelper()
    val loader = new StreamOnlyDirClassLoader(dir, getClass.getClassLoader)
    assert(loader.getResource("dyn/DynHelper.class") == null,
      "fixture loader must expose no resource URL")
    assert(loader.getResourceAsStream("dyn/DynHelper.class") != null)
    assert(!loader.getResources("dyn").hasMoreElements)

    val body =
      """
        |public java.lang.Object generate(Object[] references) {
        |  return Integer.valueOf(dyn.DynHelper.magic());
        |}
        |""".stripMargin
    val prev = Thread.currentThread().getContextClassLoader
    try {
      Thread.currentThread().setContextClassLoader(loader)
      assert(JaninoCodeCompiler.compile(newCodeAndComment(body))._1 != null)
      val (generated, _) = JdkCodeCompiler.compile(newCodeAndComment(body))
      assert(generated.generate(Array.empty[Any]) === Integer.valueOf(4242))
    } finally {
      Thread.currentThread().setContextClassLoader(prev)
    }
  }

  test("JDK backend ignores a phantom class served for a package path") {
    assume(JdkCodeCompiler.isAvailable, "javax.tools.JavaCompiler not available")
    // Some artifact / session loaders return a non-null, non-class stream for a
    // package-shaped resource path. The generated unit's own package is
    // org.apache.spark.sql.catalyst.expressions; if such a phantom were treated as a
    // class, javac would fail with "package ... clashes with class of same name".
    // Resolution must validate the class-file magic and ignore the phantom.
    val loader = new PhantomPackageClassLoader(getClass.getClassLoader)
    val prev = Thread.currentThread().getContextClassLoader
    try {
      Thread.currentThread().setContextClassLoader(loader)
      val (generated, _) = JdkCodeCompiler.compile(newCodeAndComment(sampleClassBody))
      assert(generated != null)
      assert(invokeEvaluate(generated.generate(Array.empty[Any]), 10L) == 311L)
    } finally {
      Thread.currentThread().setContextClassLoader(prev)
    }
  }

  test("JDK backend resolves an object-nested class + enclosing chain (non-enumerable loader)") {
    assume(JdkCodeCompiler.isAvailable, "javax.tools.JavaCompiler not available")
    // SaveLoadV1.Leaf is object-nested (binary ...CodeCompilerSuite$SaveLoadV1$Leaf,
    // canonical carries `$`), the same shape as a Scala REPL class
    // ($line.$read$$iw$X). Served by a loader that resolves classes by name but does
    // NOT enumerate packages, javac needs the leaf AND its enclosing classes; the
    // by-name fallback must add the whole `$`-prefix chain.
    val binary = classOf[CodeCompilerSuite.SaveLoadV1.Leaf].getName
    val loader = new NonEnumerableWrapper(getClass.getClassLoader)
    val pkgPath = binary.substring(0, binary.lastIndexOf('.')).replace('.', '/')
    assert(!loader.getResources(pkgPath).hasMoreElements, "loader must not enumerate")
    assert(loader.getResourceAsStream(binary.replace('.', '/') + ".class") != null)

    val body =
      s"""
         |public java.lang.Object generate(Object[] references) {
         |  return new $binary(7);
         |}
         |""".stripMargin
    val prev = Thread.currentThread().getContextClassLoader
    try {
      Thread.currentThread().setContextClassLoader(loader)
      assert(JaninoCodeCompiler.compile(newCodeAndComment(body))._1 != null)
      val (generated, _) = JdkCodeCompiler.compile(newCodeAndComment(body))
      assert(generated.generate(Array.empty[Any]) != null)
    } finally {
      Thread.currentThread().setContextClassLoader(prev)
    }
  }

  // A wrapper that delegates class/resource loading to its parent but refuses to
  // enumerate packages (getResources returns empty), forcing the by-name fallback -
  // the shape of an in-memory REPL / session loader that serves getResourceAsStream
  // but not getResources.
  private class NonEnumerableWrapper(parent: ClassLoader) extends ClassLoader(parent) {
    override def getResources(name: String): java.util.Enumeration[URL] =
      Collections.emptyEnumeration()
  }

  // Compile `dyn.DynHelper` into a fresh temp dir using the system Java compiler, so
  // the class exists only under that dir (never on java.class.path).
  test("reflection that raises a LinkageError does not route the unit or escape") {
    assume(JdkCodeCompiler.isAvailable, "javax.tools.JavaCompiler not available")
    // A partial or shaded jar can leave a class loadable while its enclosing class is not.
    // `getCanonicalName` and `getEnclosingClass` both throw NoClassDefFoundError then, and
    // `NonFatal` does not cover a LinkageError, so an escaping Error would bypass the
    // codegen fallbacks. The token cannot be evaluated, which is no evidence that narrowing
    // it is unsafe, so the unit must stay on the configured backend rather than gain a
    // permanent Janino arm.
    val dir = compileLinkageFixture()
    // Drop the enclosing class, keeping the anonymous one that references it.
    assert(new File(dir, "lnk/Holder$Mid.class").delete(), "fixture setup: enclosing class")
    val loader = new DirClassLoader(dir, getClass.getClassLoader)
    val anon = loader.loadClass("lnk.Holder$Mid$1")
    // Precondition: the reflective calls the predicate makes really do throw here.
    intercept[LinkageError](anon.getCanonicalName)
    intercept[LinkageError](anon.getEnclosingClass)

    val body = s"${anon.getName} v = (${anon.getName}) references[0];"
    val prev = Thread.currentThread().getContextClassLoader
    try {
      Thread.currentThread().setContextClassLoader(loader)
      assert(!JdkCodeCompiler.referencesUnnarrowableClass(body),
        "an unevaluable token must not route the unit to Janino")
      withSQLConf(SQLConf.CODEGEN_COMPILER.key -> "jdk") {
        assert(CodeCompiler.active(newCodeAndComment(body)) eq JdkCodeCompiler)
      }
      // The rewrite degrades to the binary name instead of propagating the Error.
      assert(JdkCodeCompiler.rewriteInnerClassRefs(body, loader) === body)
    } finally {
      Thread.currentThread().setContextClassLoader(prev)
    }
  }

  test("reflection that fails only on member enumeration does not narrow the reference") {
    assume(JdkCodeCompiler.isAvailable, "javax.tools.JavaCompiler not available")
    // The dangerous asymmetry: a class can be missing a type named in one of its method
    // signatures, so the supertype climb succeeds while `getMethods` throws. Narrowing then
    // looks safe, since the emitted supertype name compiles, even though no member was ever
    // checked, and an overload collision would bind to the supertype's method. The verdict
    // has to keep the binary name instead, which javac rejects and Spark falls back from.
    val dir = compileMemberLinkageFixture()
    val loader = new DirClassLoader(dir, getClass.getClassLoader)
    val anon = loader.loadClass("lnk2.Holder$1")
    val body = s"${anon.getName} v = (${anon.getName}) references[0];"

    // Positive control: while the classpath is complete the reference IS narrowed, which
    // proves the token reaches the verdict rather than being inert in this body.
    assert(JdkCodeCompiler.rewriteInnerClassRefs(body, loader) ===
      "lnk2.Foo v = (lnk2.Foo) references[0];",
      "with a complete classpath the reference must narrow to the interface")

    // Now break only the method signature's parameter type. A fresh loader is needed because
    // the one above has already resolved members for this class.
    assert(new File(dir, "lnk2/Missing.class").delete(), "fixture setup: signature type")
    val brokenLoader = new DirClassLoader(dir, getClass.getClassLoader)
    val brokenAnon = brokenLoader.loadClass("lnk2.Holder$1")
    // Preconditions: the climb reads fine, only member enumeration throws.
    assert(brokenAnon.getCanonicalName == null, "fixture must be unnameable")
    assert(brokenAnon.getInterfaces.map(_.getName).contains("lnk2.Foo"), "climb input must read")
    intercept[LinkageError](brokenAnon.getMethods)

    val prev = Thread.currentThread().getContextClassLoader
    try {
      Thread.currentThread().setContextClassLoader(brokenLoader)
      // Not routed: an unevaluable class is no evidence that narrowing is unsafe.
      assert(!JdkCodeCompiler.referencesUnnarrowableClass(body),
        "an unevaluable token must not route the unit to Janino")
      // And not narrowed either: narrowing to lnk2.Foo would compile and hide the risk.
      assert(JdkCodeCompiler.rewriteInnerClassRefs(body, brokenLoader) === body,
        "an unevaluable class must keep its binary name rather than be narrowed")
      withSQLConf(SQLConf.CODEGEN_COMPILER.key -> "jdk") {
        assert(CodeCompiler.active(newCodeAndComment(body)) eq JdkCodeCompiler)
      }
    } finally {
      Thread.currentThread().setContextClassLoader(prev)
    }
  }

  test("reflection that fails with a non-LinkageError does not narrow the reference") {
    assume(JdkCodeCompiler.isAvailable, "javax.tools.JavaCompiler not available")
    // The other half of the same failure: a classloader is user code, so it may reject a
    // class with an ordinary RuntimeException instead of ClassNotFoundException, as a
    // relocating or artifact loader can, and the JVM propagates that out of `getMethods`
    // unwrapped rather than as a LinkageError. The verdict has to treat it the same way,
    // which is why its catch does not stop at LinkageError.
    val dir = compileMemberLinkageFixture()
    val loader = new DirClassLoader(dir, getClass.getClassLoader) {
      override def findClass(name: String): Class[_] =
        if (name == "lnk2.Missing") throw new IllegalStateException("relocated away")
        else super.findClass(name)
    }
    val anon = loader.loadClass("lnk2.Holder$1")
    // Preconditions. What matters is not the exact type but that it is NOT a LinkageError,
    // since that is the property the verdict's `NonFatal` arm exists for; the enumeration
    // that throws is the one over `lnk2.Foo`, which declares the missing parameter type.
    assert(anon.getCanonicalName == null, "fixture must be unnameable")
    assert(anon.getInterfaces.map(_.getName).contains("lnk2.Foo"), "climb input must read")
    intercept[IllegalStateException](anon.getMethods)

    val body = s"${anon.getName} v = (${anon.getName}) references[0];"
    val prev = Thread.currentThread().getContextClassLoader
    try {
      Thread.currentThread().setContextClassLoader(loader)
      assert(!JdkCodeCompiler.referencesUnnarrowableClass(body),
        "an unevaluable token must not route the unit to Janino")
      assert(JdkCodeCompiler.rewriteInnerClassRefs(body, loader) === body,
        "an unevaluable class must keep its binary name rather than be narrowed")
    } finally {
      Thread.currentThread().setContextClassLoader(prev)
    }
  }

  /**
   * Compile `nrw.Holder`, whose local class holds a named inner class extending `ArrayList`.
   * `Holder$<n>Local$Inner` is a member class of a local class: neither anonymous nor local
   * itself, with a null canonical name. javac's outer reference `this$0` is package-private
   * and comes with no accessor method, so `getFields`/`getMethods` report nothing beyond
   * `ArrayList`'s and the class is narrowable.
   */
  private def compileNestedLocalFixture(): File = {
    val dir = Utils.createTempDir()
    val src =
      """package nrw;
        |public class Holder {
        |  public static Object make() {
        |    class Local {
        |      public class Inner extends java.util.ArrayList<String> { }
        |    }
        |    return new Local().new Inner();
        |  }
        |}
        |""".stripMargin
    compileJavaFixtures(dir, Seq("nrw/Holder.java" -> src))
    dir
  }

  /**
   * Compile `shd.Holder`, whose anonymous subclass redeclares its supertype's public field.
   * `getFields` reports both, so a field check that compares names rather than declaring
   * classes accepts the pair, and narrowing then reads the supertype's value, since field
   * access is resolved statically. Java, not Scala: a Scala `val` compiles to a private
   * field plus an accessor, so it cannot shadow a public field.
   *
   * `makePublic` carries the same shadowing pair on a member class of a local class. javac
   * strips `ACC_PUBLIC` from an anonymous class, which Janino then refuses to reference from
   * the generated unit's package, so an end-to-end test needs this shape instead, for the same
   * reason [[compileStaticHiderFixture]] uses one.
   */
  private def compileShadowedFieldFixture(): File = {
    val dir = Utils.createTempDir()
    val src =
      """package shd;
        |public class Holder {
        |  public static class Base { public int shadowed = 1; }
        |  public static Object make() { return new Base() { public int shadowed = 99; }; }
        |  public static Object makePublic() {
        |    class Local {
        |      public class Inner extends Base { public int shadowed = 99; }
        |    }
        |    return new Local().new Inner();
        |  }
        |}
        |""".stripMargin
    compileJavaFixtures(dir, Seq("shd/Holder.java" -> src))
    dir
  }

  /**
   * Compile `sth.Holder`, whose member-of-local class hides its supertype's public static
   * method. `getMethods` reports both, with identical erased signatures, so a signature-based
   * check accepts the pair, and a narrowed call binds the supertype's method, since a static
   * call is bound statically. A member class of a local class is used rather than an anonymous
   * one because only the former keeps `ACC_PUBLIC`, which the Janino path needs.
   */
  private def compileStaticHiderFixture(): File = {
    val dir = Utils.createTempDir()
    val src =
      """package sth;
        |public class Holder {
        |  public static class Base { public static int hidden() { return 1; } }
        |  public static Object make() {
        |    class Local {
        |      public class Inner extends Base { public static int hidden() { return 99; } }
        |    }
        |    return new Local().new Inner();
        |  }
        |  public static Object makePlain() {
        |    class Local2 { public class Inner extends Base { } }
        |    return new Local2().new Inner();
        |  }
        |}
        |""".stripMargin
    compileJavaFixtures(dir, Seq("sth/Holder.java" -> src))
    dir
  }

  /**
   * Compile `lnk2.Holder`, whose anonymous `Foo` declares a method taking `lnk2.Missing`.
   * Deleting `Missing.class` afterwards leaves the anonymous class loadable with a readable
   * supertype while `getMethods` throws: the asymmetric partial-jar shape.
   */
  private def compileMemberLinkageFixture(): File = {
    val dir = Utils.createTempDir()
    compileJavaFixtures(dir, Seq(
      "lnk2/Missing.java" -> "package lnk2; public class Missing {}",
      "lnk2/Foo.java" -> "package lnk2; public interface Foo { String extra(Missing m); }",
      "lnk2/Holder.java" ->
        """package lnk2;
          |public class Holder {
          |  public static Object make() {
          |    return new Foo() { public String extra(Missing m) { return "e"; } };
          |  }
          |}
          |""".stripMargin))
    dir
  }

  /** Compile Java source strings, given as (path, source) pairs, into `dir`. */
  private def compileJavaFixtures(dir: File, sources: Seq[(String, String)]): Unit = {
    val compiler = ToolProvider.getSystemJavaCompiler
    val fm = compiler.getStandardFileManager(null, null, null)
    try {
      fm.setLocation(StandardLocation.CLASS_OUTPUT, Collections.singletonList(dir))
      val files = sources.map { case (path, src) =>
        new SimpleJavaFileObject(URI.create(s"string:///$path"), JavaFileObject.Kind.SOURCE) {
          override def getCharContent(ignoreEncodingErrors: Boolean): CharSequence = src
        }.asInstanceOf[JavaFileObject]
      }
      assert(compiler.getTask(null, fm, null, null, null, files.asJava).call(),
        s"failed to compile fixture: ${sources.map(_._1).mkString(", ")}")
    } finally {
      fm.close()
    }
  }

  /**
   * Compile `lnk.Holder`, whose nested `Mid` holds an anonymous `Runnable`. Deleting
   * `Holder$Mid.class` afterwards leaves `Holder$Mid$1` loadable but makes reflection over
   * its enclosing class throw, which is the partial-jar shape the guards have to survive.
   */
  private def compileLinkageFixture(): File = {
    val dir = Utils.createTempDir()
    val src =
      """package lnk;
        |public class Holder {
        |  public static class Mid {
        |    public static Object make() { return new Runnable() { public void run() {} }; }
        |  }
        |}
        |""".stripMargin
    compileJavaFixtures(dir, Seq("lnk/Holder.java" -> src))
    dir
  }

  private def compileDynHelper(): File = {
    val dir = Utils.createTempDir()
    val src =
      "package dyn; public class DynHelper { public static int magic() { return 4242; } }"
    compileJavaFixtures(dir, Seq("dyn/DynHelper.java" -> src))
    dir
  }

  // A loader that is NOT a URLClassLoader (so the retired -classpath harvesting would
  // miss it) but loads classes and serves their resources from `dir` - the shape of the
  // loaders Spark uses for REPL-generated / Connect session-artifact classes.
  private class DirClassLoader(dir: File, parent: ClassLoader) extends ClassLoader(parent) {
    override def findClass(name: String): Class[_] = {
      val f = new File(dir, name.replace('.', '/') + ".class")
      if (!f.isFile) throw new ClassNotFoundException(name)
      val bytes = java.nio.file.Files.readAllBytes(f.toPath)
      defineClass(name, bytes, 0, bytes.length)
    }
    override def findResource(name: String): URL = {
      val f = new File(dir, name)
      if (f.exists) f.toURI.toURL else null
    }
    override def findResources(name: String): java.util.Enumeration[URL] = {
      val f = new File(dir, name)
      if (f.exists) Collections.enumeration(Collections.singletonList(f.toURI.toURL))
      else Collections.emptyEnumeration()
    }
  }

  // Like DirClassLoader but serves only individual .class resources by name and does
  // NOT implement findResources, so getResources(package) yields nothing - the shape of
  // an in-memory REPL / Connect session loader that cannot enumerate its packages.
  private class NonEnumerableDirClassLoader(dir: File, parent: ClassLoader)
      extends ClassLoader(parent) {
    override def findClass(name: String): Class[_] = {
      val f = new File(dir, name.replace('.', '/') + ".class")
      if (!f.isFile) throw new ClassNotFoundException(name)
      val bytes = java.nio.file.Files.readAllBytes(f.toPath)
      defineClass(name, bytes, 0, bytes.length)
    }
    override def findResource(name: String): URL = {
      val f = new File(dir, name)
      if (name.endsWith(".class") && f.isFile) f.toURI.toURL else null
    }
  }

  // Serves .class bytes ONLY via getResourceAsStream, with no resource URL
  // (getResource is null) and no enumeration - the shape of an in-memory REPL /
  // Ammonite loader. The class is still loadable (findClass) and readable as a stream.
  private class StreamOnlyDirClassLoader(dir: File, parent: ClassLoader)
      extends ClassLoader(parent) {
    override def findClass(name: String): Class[_] = {
      val f = new File(dir, name.replace('.', '/') + ".class")
      if (!f.isFile) throw new ClassNotFoundException(name)
      val bytes = java.nio.file.Files.readAllBytes(f.toPath)
      defineClass(name, bytes, 0, bytes.length)
    }
    override def getResourceAsStream(name: String): java.io.InputStream = {
      val f = new File(dir, name)
      if (name.endsWith(".class") && f.isFile) new java.io.FileInputStream(f)
      else super.getResourceAsStream(name)
    }
  }

  // Returns a non-null, non-class stream for the package-shaped path
  // `org/apache/spark/sql/catalyst/expressions.class` (the generated unit's own
  // package), reproducing the artifact-loader behaviour that surfaced a phantom class
  // clashing with that package. All other resources delegate to the parent.
  private class PhantomPackageClassLoader(parent: ClassLoader) extends ClassLoader(parent) {
    override def getResourceAsStream(name: String): java.io.InputStream = {
      if (name == "org/apache/spark/sql/catalyst/expressions.class") {
        new java.io.ByteArrayInputStream(Array[Byte]('n', 'o', 't'))
      } else {
        super.getResourceAsStream(name)
      }
    }
  }

  // ---------------- error paths ----------------

  test("Janino backend surfaces compile errors as QueryExecutionErrors.compilerError") {
    val malformedBody =
      "public boolean evaluate() { return missing_identifier; }"
    val ex = intercept[CompileException] {
      JaninoCodeCompiler.compile(newCodeAndComment(malformedBody))
    }
    // The "Failed to compile:" prefix is what QueryExecutionErrors.compilerError adds;
    // a raw Janino CompileException would not carry it.
    assert(ex.getMessage.contains("Failed to compile:"))
  }

  test("JDK backend surfaces compile errors with the same exception type as Janino") {
    assume(JdkCodeCompiler.isAvailable, "javax.tools.JavaCompiler not available")
    val malformedBody =
      "public boolean evaluate() { return missing_identifier; }"
    val janinoEx = intercept[Exception] {
      JaninoCodeCompiler.compile(newCodeAndComment(malformedBody))
    }
    val jdkEx = intercept[Exception] {
      JdkCodeCompiler.compile(newCodeAndComment(malformedBody))
    }
    // Both backends surface source errors through QueryExecutionErrors.compilerError, so
    // callers matching on the exception type behave identically whichever is active.
    assert(jdkEx.getClass === janinoEx.getClass,
      s"backend exception types diverge: janino=${janinoEx.getClass}, jdk=${jdkEx.getClass}")
    assert(jdkEx.isInstanceOf[CompileException])
    // The diagnostic names the offending identifier so users can locate the failure.
    assert(jdkEx.getMessage.contains("missing_identifier"),
      s"diagnostic lost the offending symbol:\n${jdkEx.getMessage}")
  }

  // ---------------- end-to-end through GeneratePredicate ----------------
  //
  // These exercise a real Spark code generator under each backend. They smoke-test
  // that each backend is compatible with code shapes Spark generators actually
  // produce (nested classes, references arrays, multiple methods, the GeneratedClass
  // contract). The backend cache key includes the backend, so withSQLConf
  // guarantees a fresh compilation regardless of prior in-JVM state.

  test("end-to-end: GeneratePredicate with Janino backend evaluates correctly") {
    withSQLConf(SQLConf.CODEGEN_COMPILER.key -> CodeCompiler.JANINO) {
      val predicate = GeneratePredicate.generate(
        GreaterThan(BoundReference(0, IntegerType, nullable = false), Literal(5)))
      assert(predicate.eval(InternalRow(10)) === true)
      assert(predicate.eval(InternalRow(3)) === false)
    }
  }

  test("end-to-end: GeneratePredicate with JDK backend evaluates correctly") {
    assume(JdkCodeCompiler.isAvailable, "javax.tools.JavaCompiler not available")
    withSQLConf(SQLConf.CODEGEN_COMPILER.key -> CodeCompiler.JDK) {
      val predicate = GeneratePredicate.generate(
        LessThan(BoundReference(0, IntegerType, nullable = false), Literal(5)))
      assert(predicate.eval(InternalRow(3)) === true)
      assert(predicate.eval(InternalRow(10)) === false)
    }
  }

  test("CodeGenerator.compile caches the same source separately per backend") {
    assume(JdkCodeCompiler.isAvailable, "javax.tools.JavaCompiler not available")
    // The backend is part of the cache key; if it were dropped, flipping the config
    // would silently serve the other backend's cached class for identical source.
    val code = newCodeAndComment(sampleClassBody)
    val (janinoGenerated, _) = withSQLConf(SQLConf.CODEGEN_COMPILER.key -> CodeCompiler.JANINO) {
      CodeGenerator.compile(code)
    }
    val (jdkGenerated, _) = withSQLConf(SQLConf.CODEGEN_COMPILER.key -> CodeCompiler.JDK) {
      CodeGenerator.compile(code)
    }
    assert(janinoGenerated.getClass.getClassLoader ne jdkGenerated.getClass.getClassLoader,
      "each backend must produce (and cache) its own compilation of the same source")
  }

  // ---------------- wrapAsCompilationUnit shape ----------------

  test("JDK backend's wrapAsCompilationUnit produces a well-formed source unit") {
    assume(JdkCodeCompiler.isAvailable, "javax.tools.JavaCompiler not available")
    val wrapped = JdkCodeCompiler.wrapAsCompilationUnit(sampleClassBody, getClass.getClassLoader)
    assert(wrapped.startsWith("package org.apache.spark.sql.catalyst.expressions;"),
      s"missing package declaration:\n$wrapped")
    assert(wrapped.contains("import org.apache.spark.unsafe.Platform;"),
      s"missing default imports:\n$wrapped")
    assert(wrapped.contains(
      "public class GeneratedClass extends " +
        "org.apache.spark.sql.catalyst.expressions.codegen.GeneratedClass"),
      s"missing class declaration:\n$wrapped")
  }

  // ---------------- rewriteInnerClassRefs ----------------

  // Classloader used to resolve candidate type references in the tests below.
  private val rewriteLoader: ClassLoader = getClass.getClassLoader
  private def rewrite(body: String): String =
    JdkCodeCompiler.rewriteInnerClassRefs(body, rewriteLoader)

  test("rewriteInnerClassRefs: converts binary inner-class refs to dotted form") {
    assume(JdkCodeCompiler.isAvailable, "javax.tools.JavaCompiler not available")
    // Unresolvable synthetic names fall back to the conservative regex.
    assert(rewrite("a.b.Outer$Inner x;") === "a.b.Outer.Inner x;")
    // Doubly-nested names rewrite every separator.
    assert(rewrite("Outer$Mid$Inner") === "Outer.Mid.Inner")
  }

  test("rewriteInnerClassRefs: restores the trailing dot of a line-wrapped member access") {
    assume(JdkCodeCompiler.isAvailable, "javax.tools.JavaCompiler not available")
    // A member access wrapped onto the next line scans as a token ending in '.', which
    // `split('.')` would otherwise drop along with the trailing empty segment.
    assert(rewrite("java.util.Map$Entry.\ncomparingByKey()") ===
      "java.util.Map.Entry.\ncomparingByKey()")
  }

  test("rewriteInnerClassRefs: dots regular nested classes resolved via reflection") {
    assume(JdkCodeCompiler.isAvailable, "javax.tools.JavaCompiler not available")
    // java.util.Map$Entry is a real class; its canonical name java.util.Map.Entry
    // is a plain dotted name, so the reflection path emits the dotted form. A
    // trailing member access is preserved.
    assert(rewrite("java.util.Map$Entry e;") === "java.util.Map.Entry e;")
    assert(rewrite("java.util.Map$Entry.class") === "java.util.Map.Entry.class")
  }

  test("rewriteInnerClassRefs: preserves binary names of object-nested classes") {
    assume(JdkCodeCompiler.isAvailable, "javax.tools.JavaCompiler not available")
    // A case class nested inside a Scala object has a canonical name carrying a
    // module `$` (Outer.SaveLoadV1$.Leaf) that the JDK compiler cannot resolve;
    // the binary name must be kept verbatim.
    val binary = classOf[CodeCompilerSuite.SaveLoadV1.Leaf].getName
    assert(binary.count(_ == '$') === 2, s"unexpected binary form: $binary")
    assert(rewrite(s"$binary x = null;") === s"$binary x = null;")
    // Companion-object access on such a class is also preserved (longest loadable
    // prefix is the companion class, trailing MODULE$.apply is kept).
    assert(rewrite(s"$binary$$.MODULE$$.apply(1)") === s"$binary$$.MODULE$$.apply(1)")
  }

  test("rewriteInnerClassRefs: preserves Scala companion and mangled names") {
    assume(JdkCodeCompiler.isAvailable, "javax.tools.JavaCompiler not available")
    // `$` followed by `.` (companion access) on an unresolvable name must not be touched.
    assert(rewrite("Foo$.MODULE$.apply()") === "Foo$.MODULE$.apply()")
    // A real top-level operator-named class: canonical is `scala.collection.immutable.::`
    // which is not a valid Java identifier, so the binary name is kept.
    assert(rewrite("scala.collection.immutable.$colon$colon") ===
      "scala.collection.immutable.$colon$colon")
  }

  test("rewriteInnerClassRefs: does not corrupt string or char literals or comments") {
    assume(JdkCodeCompiler.isAvailable, "javax.tools.JavaCompiler not available")
    // A `$Upper` sequence inside a string literal (e.g. a column name) is preserved.
    assert(rewrite("""String s = "col$Name";""") === """String s = "col$Name";""")
    // Escaped quote inside the string does not end it early.
    assert(rewrite("""x = "a\"b$Cd"; Outer$Inner y;""") === """x = "a\"b$Cd"; Outer.Inner y;""")
    // Char literal preserved.
    assert(rewrite("""char c = '$'; Outer$Inner z;""") === """char c = '$'; Outer.Inner z;""")
    // Line and block comments preserved.
    assert(rewrite("// see Foo$Bar\nOuter$Inner w;") === "// see Foo$Bar\nOuter.Inner w;")
    assert(rewrite("/* Foo$Bar */ Outer$Inner w;") === "/* Foo$Bar */ Outer.Inner w;")
  }

  test("rewriteInnerClassRefs: rewrites anonymous-class refs to a nameable supertype") {
    assume(JdkCodeCompiler.isAvailable, "javax.tools.JavaCompiler not available")
    // A Scala `new HashMap() {...}` compiles to an anonymous class (`...$$anon$N`) that
    // cannot be named in Java source; the JDK compiler rejects a qualified reference to
    // it even with the bytecode present. It must be rewritten to its nearest nameable
    // supertype (java.util.HashMap), which is a sound cast target for the generated code.
    val anon = new java.util.HashMap[String, String]() {}
    val anonName = anon.getClass.getName
    assert(anon.getClass.isAnonymousClass, s"expected an anonymous class, got: $anonName")
    assert(rewrite(s"$anonName m = ($anonName) references[0];") ===
      "java.util.HashMap m = (java.util.HashMap) references[0];")
  }

  test("rewriteInnerClassRefs: anonymous interface impl rewrites to the interface") {
    assume(JdkCodeCompiler.isAvailable, "javax.tools.JavaCompiler not available")
    // An anonymous class implementing only an interface has Object as its superclass;
    // the nameable-supertype climb must pick the interface, not Object.
    val anon = new java.util.Comparator[String] {
      override def compare(a: String, b: String): Int = a.compareTo(b)
    }
    val anonName = anon.getClass.getName
    assert(anon.getClass.isAnonymousClass || anon.getClass.isLocalClass,
      s"expected an anonymous/local class, got: $anonName")
    assert(rewrite(s"$anonName c = ($anonName) references[0];") ===
      "java.util.Comparator c = (java.util.Comparator) references[0];")
  }

  test("JDK backend resolves an anonymous class reference via its nameable supertype") {
    assume(JdkCodeCompiler.isAvailable, "javax.tools.JavaCompiler not available")
    // Mirrors SPARK-23589 (ExternalMapToCatalyst over an anonymous java.util.HashMap):
    // codegen casts the literal object to its anonymous binary name. Janino loads that
    // name directly; the JDK backend must rewrite it to a nameable supertype and still
    // produce working bytecode.
    val anon = new java.util.HashMap[String, String]() {
      put("k", "v")
    }
    val anonName = anon.getClass.getName
    val body =
      s"""
         |public java.lang.Object generate(Object[] references) {
         |  $anonName m = ($anonName) references[0];
         |  return m.get("k");
         |}
       """.stripMargin
    assert(JaninoCodeCompiler.compile(newCodeAndComment(body))._1 != null)
    val (generated, _) = JdkCodeCompiler.compile(newCodeAndComment(body))
    assert(generated.generate(Array[Any](anon)) === "v")
  }

  // ---------------- unnarrowable anonymous/local classes ----------------

  test("containsDollarDigit gates the scan on names Java cannot spell") {
    // The `$`-digit gate must admit every unnameable shape and reject the nameable `$`
    // forms the rewrite handles, or the scan either misses a class or runs needlessly.
    for (name <- Seq("Outer$1", "Outer$1Local", "Outer$$anon$1", "Outer$1$Inner",
        "a.b.Outer$$anon$12")) {
      assert(JdkCodeCompiler.containsDollarDigit(name), s"expected $name to be gated in")
    }
    for (name <- Seq("", "$", "a$", "java.util.Map$Entry", "Foo$", "Model$SaveLoad$Leaf",
        "scala.Function1$mcII$sp", "scala.collection.immutable.$colon$colon",
        "pkg.package$Inner", "$line21.$read$$iw$T")) {
      assert(!JdkCodeCompiler.containsDollarDigit(name), s"expected $name to be gated out")
    }
  }

  test("referencesUnnarrowableClass: false when the supertype offers every member") {
    // The shapes codegen actually produces: the anonymous or local class only overrides
    // methods the supertype already declares, so narrowing the reference loses nothing.
    // `specializedPrimitiveOverride` is the boxing case - scalac specializes the override to
    // `apply(int)` while the bridge that reaches it takes `Object`. `anonWithLambdaBody` is
    // the synthetic-static case: a closure in the body adds a `public static final
    // $anonfun$...` helper javac cannot name, which must not make the class unnarrowable.
    val anonSubclass = new java.util.HashMap[String, String]() { put("k", "v") }
    val anonInterface = new java.util.Comparator[String] {
      override def compare(a: String, b: String): Int = a.compareTo(b)
    }
    val narrowable = Seq[Any](anonSubclass, anonInterface, CodeCompilerSuite.plainLocal,
      CodeCompilerSuite.specializedPrimitiveOverride, CodeCompilerSuite.anonWithLambdaBody)
    for (o <- narrowable) {
      val cls = o.getClass
      // Guard the fixture's own precondition: a nameable class would pass the assertion
      // below for the wrong reason.
      assert(cls.getCanonicalName == null, s"expected an unnameable class, got: ${cls.getName}")
      val name = cls.getName
      assert(!JdkCodeCompiler.referencesUnnarrowableClass(s"$name v = ($name) references[0];"),
        s"expected $name to be narrowable")
    }
  }

  test("referencesUnnarrowableClass: a synthetic static does not make a class unnarrowable") {
    // Pins the fixture's own shape, so the case above cannot pass by accident if a future
    // scalac stops emitting the helper as a public static on the anonymous class.
    val cls = CodeCompilerSuite.anonWithLambdaBody.getClass
    val statics = cls.getMethods.filter(m => Modifier.isStatic(m.getModifiers))
    assert(statics.nonEmpty, s"fixture must carry a public static, got none on ${cls.getName}")
    assert(statics.forall(_.isSynthetic),
      s"fixture's statics must all be synthetic, got: ${statics.mkString(", ")}")
    assert(statics.exists(_.getDeclaringClass eq cls),
      s"at least one static must be declared by the anonymous class itself, got: " +
        statics.map(_.getDeclaringClass.getName).mkString(", "))
  }

  test("referencesUnnarrowableClass: true when narrowing would lose access") {
    // An extra public method, public fields inherited from a second interface, a member on
    // a second interface, an overload that shadows nothing, a local class with an extra
    // method, a bridged override sharing its name and arity with an unrelated overload, and
    // an instance method whose only counterpart on the supertype is static: each puts
    // something out of reach of the nearest nameable supertype.
    val unnarrowable = Seq[Any](
      CodeCompilerSuite.anonWithExtraMethod,
      CodeCompilerSuite.anonWithPublicFields,
      CodeCompilerSuite.anonWithSecondInterface,
      CodeCompilerSuite.anonWithOverload,
      CodeCompilerSuite.localWithExtraMethod,
      CodeCompilerSuite.anonWithBridgeAndPrimitiveOverload,
      CodeCompilerSuite.anonWithBridgeAndReferenceOverload,
      CodeCompilerSuite.anonOverStaticClash)
    for (o <- unnarrowable) {
      val cls = o.getClass
      assert(cls.getCanonicalName == null, s"expected an unnameable class, got: ${cls.getName}")
      val name = cls.getName
      assert(JdkCodeCompiler.referencesUnnarrowableClass(s"$name v = ($name) references[0];"),
        s"expected $name to be rejected")
    }
  }

  test("referencesUnnarrowableClass: a bridge covers only the method it forwards to") {
    // Guards the fixture shapes behind the two bridge tests: both classes carry a genuine
    // bridge for the generic override, so a check that excused the whole name/arity group
    // would let the unrelated overload ride along on it.
    for (o <- Seq[Any](CodeCompilerSuite.anonWithBridgeAndPrimitiveOverload,
        CodeCompilerSuite.anonWithBridgeAndReferenceOverload)) {
      val compares = o.getClass.getMethods.filter(_.getName == "compare")
      assert(compares.exists(_.isBridge), "fixture precondition: expected a bridge method")
      assert(compares.count(m => !m.isBridge && m.getParameterCount == 2) === 2,
        "fixture precondition: expected the override and the overload to share name and arity")
    }
  }

  test("referencesUnnarrowableClass: true when the supertype itself cannot be named") {
    // Members line up here, but the nearest nameable supertype is a private nested class
    // (scala.collection.mutable.HashSet$HashSetIterator), so javac could not write the
    // narrowed cast at all.
    val cls = scala.collection.mutable.HashSet("a").iterator.getClass
    assert(cls.getCanonicalName == null, s"expected an unnameable class, got: ${cls.getName}")
    val name = cls.getName
    assert(JdkCodeCompiler.referencesUnnarrowableClass(s"$name v = ($name) references[0];"),
      s"expected $name to be rejected for an unnameable supertype")
  }

  test("referencesUnnarrowableClass: a field shadowing the supertype's cannot be narrowed") {
    assume(JdkCodeCompiler.isAvailable, "javax.tools.JavaCompiler not available")
    // The one silent-wrong-answer shape that methods do not have. `getFields` reports both
    // the shadowing and the shadowed field, so comparing names would accept the pair, but
    // field access is resolved statically: the narrowed reference would read the supertype's
    // 1 instead of the anonymous class's 99, compiling cleanly the whole way.
    val dir = compileShadowedFieldFixture()
    val loader = new DirClassLoader(dir, getClass.getClassLoader)
    val anon = loader.loadClass("shd.Holder").getMethod("make").invoke(null).getClass
    assert(anon.getCanonicalName == null, s"expected an unnameable class, got: ${anon.getName}")
    // Fixture preconditions: the shadowing pair is exactly what a name check would miss.
    val vs = anon.getFields.filter(_.getName == "shadowed")
    assert(vs.length === 2, s"expected two public `shadowed` fields: ${vs.mkString(", ")}")
    assert(vs.exists(_.getDeclaringClass eq anon),
      "one `shadowed` must be declared by the anonymous class")

    val body = s"${anon.getName} v = (${anon.getName}) references[0];"
    val prev = Thread.currentThread().getContextClassLoader
    try {
      Thread.currentThread().setContextClassLoader(loader)
      assert(JdkCodeCompiler.referencesUnnarrowableClass(body),
        "a shadowed field must make the class unnarrowable")
      assert(JdkCodeCompiler.rewriteInnerClassRefs(body, loader) === body,
        "an unnarrowable class must keep its binary name")
    } finally {
      Thread.currentThread().setContextClassLoader(prev)
    }
  }

  test("referencesUnnarrowableClass: a static method hiding the supertype's cannot be narrowed") {
    assume(JdkCodeCompiler.isAvailable, "javax.tools.JavaCompiler not available")
    // The method counterpart of the field case: `hidden()` erases identically on both
    // classes, so a signature check accepts the pair, but a static call is bound statically:
    // the narrowed reference would call the supertype's `hidden()` for 1 instead of 99, and
    // Java permits the instance-qualified form so even a plain cast rebinds.
    val dir = compileStaticHiderFixture()
    val loader = new DirClassLoader(dir, getClass.getClassLoader)
    val inner = loader.loadClass("sth.Holder").getMethod("make").invoke(null).getClass
    assert(inner.getCanonicalName == null, s"expected an unnameable class, got: ${inner.getName}")
    // Fixture preconditions: `f` is static and declared here, while the target declares an
    // identically-erased static of its own: the pair a declaring-class check must reject and
    // a signature check would accept. (`getMethods` reports only the hiding one; a static is
    // hidden, not inherited.)
    val fs = inner.getMethods.filter(_.getName == "hidden")
    assert(fs.length === 1, s"expected one visible `f`, got: ${fs.mkString(", ")}")
    assert((fs.head.getDeclaringClass eq inner) && Modifier.isStatic(fs.head.getModifiers),
      s"`f` must be a static declared by the member class, got: ${fs.head}")
    val base = loader.loadClass("sth.Holder$Base")
    assert(base.getMethods.exists(m =>
      m.getName == "hidden" && m.getParameterCount == 0 && Modifier.isStatic(m.getModifiers)),
      "the target must declare the same static signature, or nothing would be hidden")
    assert(inner.getSuperclass eq base, s"expected Base as superclass, got ${inner.getSuperclass}")
    // And the complement, so both branches of the declaring-class check are pinned: a static
    // the class merely INHERITS from the target stays narrowable.
    val plain = loader.loadClass("sth.Holder").getMethod("makePlain").invoke(null).getClass
    assert(plain.getCanonicalName == null, s"expected an unnameable class, got: ${plain.getName}")
    assert(plain.getMethods.exists(m =>
      m.getName == "hidden" && Modifier.isStatic(m.getModifiers) &&
        (m.getDeclaringClass eq base)),
      "the complement fixture must inherit the target's static rather than hide it")

    val body = s"${inner.getName} v = (${inner.getName}) references[0];"
    val prev = Thread.currentThread().getContextClassLoader
    try {
      Thread.currentThread().setContextClassLoader(loader)
      assert(JdkCodeCompiler.referencesUnnarrowableClass(body),
        "a hidden static method must make the class unnarrowable")
      assert(JdkCodeCompiler.rewriteInnerClassRefs(body, loader) === body,
        "an unnarrowable class must keep its binary name")
      val plainBody = s"${plain.getName} v = (${plain.getName}) references[0];"
      assert(!JdkCodeCompiler.referencesUnnarrowableClass(plainBody),
        "merely inheriting the target's static must stay narrowable")
      assert(JdkCodeCompiler.rewriteInnerClassRefs(plainBody, loader) ===
        s"sth.Holder.Base v = (sth.Holder.Base) references[0];",
        "the complement fixture must narrow to the target")
    } finally {
      Thread.currentThread().setContextClassLoader(prev)
    }
  }

  test("rewriteInnerClassRefs: narrows a class nested inside a local class") {
    assume(JdkCodeCompiler.isAvailable, "javax.tools.JavaCompiler not available")
    // A named class declared inside a local class is neither anonymous nor local, so the
    // climb has to key off `getCanonicalName == null` to catch it. Its outer reference is
    // reachable from neither side: javac's `this$0` is package-private with no accessor, and
    // scalac's `$outer` pair is synthetic, so the member check sees nothing beyond
    // ArrayList's either way (the two tests below cover the Scala shape).
    val dir = compileNestedLocalFixture()
    val loader = new DirClassLoader(dir, getClass.getClassLoader)
    // Derive the binary name rather than hardcoding it: the JLS fixes the shape
    // (`Holder$<digits>Local$Inner`) but leaves the digit sequence to the compiler.
    val cls = loader.loadClass("nrw.Holder")
      .getMethod("make").invoke(null).getClass
    assert(!cls.isAnonymousClass && !cls.isLocalClass && cls.isMemberClass,
      s"expected a member class of a local class, got: ${cls.getName}")
    assert(cls.getCanonicalName == null, s"expected no canonical name for ${cls.getName}")
    val name = cls.getName
    val body = s"$name v = ($name) references[0];"
    // Fixture guard: this shape must stay on the JDK backend, or the rewrite is moot. The
    // context loader has to be swapped because the fixture lives only in a temp dir.
    val prev = Thread.currentThread().getContextClassLoader
    try {
      Thread.currentThread().setContextClassLoader(loader)
      assert(!JdkCodeCompiler.referencesUnnarrowableClass(body),
        "fixture must be narrowable, otherwise it would route to Janino instead")
    } finally {
      Thread.currentThread().setContextClassLoader(prev)
    }
    assert(JdkCodeCompiler.rewriteInnerClassRefs(body, loader) ===
      "java.util.ArrayList v = (java.util.ArrayList) references[0];")
  }

  test("rewriteInnerClassRefs: keeps the binary name of an unnarrowable nested local class") {
    assume(JdkCodeCompiler.isAvailable, "javax.tools.JavaCompiler not available")
    // The complement of the test above: the same shape plus a non-synthetic public method
    // `ArrayList` does not have, so narrowing would put it out of reach. The rewrite refuses
    // rather than emitting a name that compiles but drops the member. The `$outer` field and
    // accessor are NOT what makes this unnarrowable: being synthetic, they are unreachable
    // from generated source either way. No context-loader swap is needed here: this fixture
    // is on the suite's own classpath, unlike the temp-dir one above.
    val cls = CodeCompilerSuite.memberOfLocalClassWithExtra.getClass
    assert(cls.getCanonicalName == null, s"expected no canonical name for ${cls.getName}")
    assert(cls.getMethods.exists(m => m.getName == "extra" && !m.isSynthetic),
      "fixture must carry a non-synthetic public method absent from ArrayList")
    val name = cls.getName
    val body = s"$name v = ($name) references[0];"
    assert(JdkCodeCompiler.referencesUnnarrowableClass(body),
      "a member class with an extra public method must be reported unnarrowable")
    assert(rewrite(body) === body, "an unnarrowable class must keep its binary name")
  }

  test("referencesUnnarrowableClass: synthetic outer accessors do not block narrowing") {
    // `memberOfLocalClass` carries scalac's public `$outer` field and accessor, both synthetic
    // and therefore invisible to javac's source lookup ("cannot find symbol"), so nothing a
    // generator emits can reach them and narrowing to `ArrayList` loses nothing.
    val cls = CodeCompilerSuite.memberOfLocalClass.getClass
    assert(cls.getCanonicalName == null, s"expected no canonical name for ${cls.getName}")
    val outerFields = cls.getFields.filter(_.getName.contains("outer"))
    assert(outerFields.nonEmpty && outerFields.forall(_.isSynthetic),
      s"fixture must carry a synthetic public outer field, got: ${outerFields.mkString(", ")}")
    val extraMethods = cls.getMethods.filterNot(m =>
      classOf[java.util.ArrayList[_]].getMethods.exists(_.getName == m.getName))
    assert(extraMethods.nonEmpty && extraMethods.forall(_.isSynthetic),
      s"fixture's extra methods must all be synthetic, got: ${extraMethods.mkString(", ")}")
    val name = cls.getName
    assert(!JdkCodeCompiler.referencesUnnarrowableClass(s"$name v = ($name) references[0];"),
      "synthetic-only extras must not make a class unnarrowable")
  }

  test("active(code) routes an unnarrowable class reference to Janino") {
    assume(JdkCodeCompiler.isAvailable, "javax.tools.JavaCompiler not available")
    val narrowableName = (new java.util.HashMap[String, String]() { put("k", "v") })
      .getClass.getName
    val unnarrowableName = CodeCompilerSuite.anonWithExtraMethod.getClass.getName
    withSQLConf(SQLConf.CODEGEN_COMPILER.key -> "jdk") {
      assert(CodeCompiler.active(newCodeAndComment(s"$narrowableName v;")) eq JdkCodeCompiler)
      assert(CodeCompiler.active(newCodeAndComment(s"$unnarrowableName v;")) eq JaninoCodeCompiler)
    }
    withSQLConf(SQLConf.CODEGEN_COMPILER.key -> "janino") {
      assert(CodeCompiler.active(newCodeAndComment(s"$unnarrowableName v;")) eq JaninoCodeCompiler)
    }
  }

  test("JDK backend cannot compile a member access that narrowing drops") {
    assume(JdkCodeCompiler.isAvailable, "javax.tools.JavaCompiler not available")
    // Both halves of the routing decision: javac rejects the rewritten unit (the reference
    // narrows to java.util.HashMap, which has no `extra()`), and `active` therefore hands
    // the unit to Janino, which compiles it and produces the right answer.
    val anon = CodeCompilerSuite.anonWithExtraMethod
    val anonName = anon.getClass.getName
    val code = newCodeAndComment(
      s"""
         |public java.lang.Object generate(Object[] references) {
         |  $anonName m = ($anonName) references[0];
         |  return m.extra();
         |}
       """.stripMargin)
    intercept[CompileException] {
      JdkCodeCompiler.compile(code)
    }
    withSQLConf(SQLConf.CODEGEN_COMPILER.key -> "jdk") {
      assert(CodeCompiler.active(code) eq JaninoCodeCompiler)
      val (generated, _) = CodeGenerator.compile(code)
      assert(generated.generate(Array[Any](anon)) === "extra")
    }
  }

  test("JDK backend compiles a call that narrowing preserves through a bridge") {
    assume(JdkCodeCompiler.isAvailable, "javax.tools.JavaCompiler not available")
    // The counterpart to the test above: an override of a generic method erases narrower
    // than the interface declares, but the compiler-emitted bridge keeps dispatch correct,
    // so this must stay on the JDK backend rather than being routed away.
    val anon = new java.util.Comparator[String] {
      override def compare(a: String, b: String): Int = a.compareTo(b)
    }
    val anonName = anon.getClass.getName
    val code = newCodeAndComment(
      s"""
         |public java.lang.Object generate(Object[] references) {
         |  $anonName c = ($anonName) references[0];
         |  return Integer.valueOf(c.compare("a", "b"));
         |}
       """.stripMargin)
    withSQLConf(SQLConf.CODEGEN_COMPILER.key -> "jdk") {
      assert(CodeCompiler.active(code) eq JdkCodeCompiler)
    }
    val (generated, _) = JdkCodeCompiler.compile(code)
    assert(generated.generate(Array[Any](anon)) === -1)
  }

  test("end-to-end: a shadowing field reads the class's own value, not the target's") {
    assume(JdkCodeCompiler.isAvailable, "javax.tools.JavaCompiler not available")
    // The verdict tests pin the decision; this pins the outcome. `Inner` redeclares `Base`'s
    // public `shadowed`, and a field read is bound statically, so the two names answer
    // differently at run time: 99 through the class, 1 through the supertype. Routing keeps 99.
    val dir = compileShadowedFieldFixture()
    val loader = new DirClassLoader(dir, getClass.getClassLoader)
    val value = loader.loadClass("shd.Holder").getMethod("makePublic").invoke(null)
    val name = value.getClass.getName
    assert(value.getClass.getCanonicalName == null, s"expected an unnameable class: $name")
    val hazard = newCodeAndComment(
      s"""
         |public java.lang.Object generate(Object[] references) {
         |  $name v = ($name) references[0];
         |  return Integer.valueOf(v.shadowed);
         |}
       """.stripMargin)
    // The counterfactual: the same read spelled with the name narrowing would emit. Janino
    // compiles it too, and it answers 1, which is exactly why it may not be emitted.
    val narrowed = newCodeAndComment(
      """
        |public java.lang.Object generate(Object[] references) {
        |  shd.Holder.Base v = (shd.Holder.Base) references[0];
        |  return Integer.valueOf(v.shadowed);
        |}
      """.stripMargin)
    val prev = Thread.currentThread().getContextClassLoader
    try {
      Thread.currentThread().setContextClassLoader(loader)
      // Janino names the class itself, so it binds the class's own field.
      assert(JaninoCodeCompiler.compile(hazard)._1.generate(Array[Any](value)) === 99)
      withSQLConf(SQLConf.CODEGEN_COMPILER.key -> "jdk") {
        assert(CodeCompiler.active(hazard) eq JaninoCodeCompiler)
        val (generated, _) = CodeGenerator.compile(hazard)
        assert(generated.generate(Array[Any](value)) === 99,
          "routing must preserve the shadowing field's value")
      }
      assert(JaninoCodeCompiler.compile(narrowed)._1.generate(Array[Any](value)) === 1,
        "counterfactual: the narrowed name reads the supertype's field")
    } finally {
      Thread.currentThread().setContextClassLoader(prev)
    }
  }

  test("end-to-end: a hidden static call reaches the class's own method, not the target's") {
    assume(JdkCodeCompiler.isAvailable, "javax.tools.JavaCompiler not available")
    // The method counterpart. `Inner.hidden()` hides `Base.hidden()` with an identical
    // erasure, and a static call is bound statically even in the instance-qualified form, so
    // the narrowed reference would answer 1 where the class answers 99.
    val dir = compileStaticHiderFixture()
    val loader = new DirClassLoader(dir, getClass.getClassLoader)
    val value = loader.loadClass("sth.Holder").getMethod("make").invoke(null)
    val name = value.getClass.getName
    assert(value.getClass.getCanonicalName == null, s"expected an unnameable class: $name")
    val hazard = newCodeAndComment(
      s"""
         |public java.lang.Object generate(Object[] references) {
         |  $name v = ($name) references[0];
         |  return Integer.valueOf(v.hidden());
         |}
       """.stripMargin)
    val narrowed = newCodeAndComment(
      """
        |public java.lang.Object generate(Object[] references) {
        |  sth.Holder.Base v = (sth.Holder.Base) references[0];
        |  return Integer.valueOf(v.hidden());
        |}
      """.stripMargin)
    val prev = Thread.currentThread().getContextClassLoader
    try {
      Thread.currentThread().setContextClassLoader(loader)
      assert(JaninoCodeCompiler.compile(hazard)._1.generate(Array[Any](value)) === 99)
      withSQLConf(SQLConf.CODEGEN_COMPILER.key -> "jdk") {
        assert(CodeCompiler.active(hazard) eq JaninoCodeCompiler)
        val (generated, _) = CodeGenerator.compile(hazard)
        assert(generated.generate(Array[Any](value)) === 99,
          "routing must preserve the hiding static's value")
      }
      assert(JaninoCodeCompiler.compile(narrowed)._1.generate(Array[Any](value)) === 1,
        "counterfactual: the narrowed name calls the supertype's static")
    } finally {
      Thread.currentThread().setContextClassLoader(prev)
    }
  }

  test("end-to-end: an instance method clashing with a static keeps the class's value") {
    assume(JdkCodeCompiler.isAvailable, "javax.tools.JavaCompiler not available")
    // The third shape: `value()` is an INSTANCE method on the anonymous class whose only
    // counterpart on `StaticClashBase` is STATIC. Java permits the instance-qualified form
    // for a static, so the narrowed call compiles and binds the target's `value()`, giving 1
    // instead of 7. Both fixtures are on the suite's own classpath, so no loader swap here.
    val anon = CodeCompilerSuite.anonOverStaticClash
    val name = anon.getClass.getName
    assert(anon.getClass.getCanonicalName == null, s"expected an unnameable class: $name")
    val target = classOf[StaticClashBase].getName
    val hazard = newCodeAndComment(
      s"""
         |public java.lang.Object generate(Object[] references) {
         |  $name v = ($name) references[0];
         |  return Integer.valueOf(v.value());
         |}
       """.stripMargin)
    val narrowed = newCodeAndComment(
      s"""
         |public java.lang.Object generate(Object[] references) {
         |  $target v = ($target) references[0];
         |  return Integer.valueOf(v.value());
         |}
       """.stripMargin)
    assert(JaninoCodeCompiler.compile(hazard)._1.generate(Array[Any](anon)) === 7)
    withSQLConf(SQLConf.CODEGEN_COMPILER.key -> "jdk") {
      assert(CodeCompiler.active(hazard) eq JaninoCodeCompiler)
      val (generated, _) = CodeGenerator.compile(hazard)
      assert(generated.generate(Array[Any](anon)) === 7,
        "routing must preserve the instance method's value")
    }
    assert(JdkCodeCompiler.compile(narrowed)._1.generate(Array[Any](anon)) === 1,
      "counterfactual: the narrowed name calls the target's static")
  }

  // ---------------- Function1 apply(Object) bridge ----------------

  test("stripFunction1ApplyBridges removes the bridge but keeps apply(InternalRow)") {
    val src =
      s"""${CodeGenerator.function1ApplyBridge("i")}
         |public UnsafeRow apply(InternalRow i) { return null; }""".stripMargin
    assert(src.contains("apply(java.lang.Object"))
    val stripped = JdkCodeCompiler.stripFunction1ApplyBridges(src)
    assert(!stripped.contains("apply(java.lang.Object"),
      s"bridge not stripped:\n$stripped")
    assert(stripped.contains("apply(InternalRow i)"), s"typed apply lost:\n$stripped")
  }

  test("both backends compile a projection with the Function1 apply(Object) bridge") {
    assume(JdkCodeCompiler.isAvailable, "javax.tools.JavaCompiler not available")
    // A class extending a Scala `InternalRow => *` must carry an explicit apply(Object)
    // bridge for Janino (which does not synthesize it) but not for javac (which does, and
    // rejects an explicit duplicate). Generators emit the bridge; the JDK backend strips
    // it. The same generated source must compile under both backends.
    val body =
      s"""
         |public java.lang.Object generate(Object[] references) {
         |  return new SpecificProj();
         |}
         |static class SpecificProj
         |    extends org.apache.spark.sql.catalyst.expressions.UnsafeProjection {
         |  ${CodeGenerator.function1ApplyBridge("i")}
         |  public UnsafeRow apply(InternalRow i) { return null; }
         |  public void initialize(int partitionIndex) {}
         |}
       """.stripMargin
    assert(JaninoCodeCompiler.compile(newCodeAndComment(body))._1 != null)
    assert(JdkCodeCompiler.compile(newCodeAndComment(body))._1 != null)
  }

  // ---------------- extractLeadingImports ----------------

  test("extractLeadingImports: hoists leading imports and leaves the rest") {
    assume(JdkCodeCompiler.isAvailable, "javax.tools.JavaCompiler not available")
    val body =
      """import a.B;
        |
        |import c.D;
        |public void foo() {}
        |import e.F;""".stripMargin
    val (imports, rest) = JdkCodeCompiler.extractLeadingImports(body)
    // The blank line between the leading imports is consumed and not preserved -
    // intentional: blank lines are cosmetic in the import block.
    assert(imports === "import a.B;\nimport c.D;\n")
    // An import after the first non-import line stays in the body.
    assert(rest === "public void foo() {}\nimport e.F;")
  }

  test("extractLeadingImports: no leading imports leaves body unchanged") {
    assume(JdkCodeCompiler.isAvailable, "javax.tools.JavaCompiler not available")
    val body = "public void foo() {}\n"
    val (imports, rest) = JdkCodeCompiler.extractLeadingImports(body)
    assert(imports === "")
    assert(rest === body)
  }

  test("JDK backend hoists leading imports and compiles (GenerateColumnAccessor shape)") {
    assume(JdkCodeCompiler.isAvailable, "javax.tools.JavaCompiler not available")
    // GenerateColumnAccessor emits `import` lines at the top of the class body. Janino
    // accepts them there; javac only accepts them at the compilation-unit level, so
    // wrapAsCompilationUnit must hoist them - and the result must actually compile.
    val body =
      """import java.util.ArrayList;
        |
        |public java.lang.Object generate(Object[] references) {
        |  ArrayList list = new ArrayList();
        |  list.add("ok");
        |  return list.get(0);
        |}""".stripMargin
    assert(JaninoCodeCompiler.compile(newCodeAndComment(body))._1 != null)
    val (generated, _) = JdkCodeCompiler.compile(newCodeAndComment(body))
    assert(generated.generate(Array.empty[Any]) === "ok")
  }

  // ---------------- interrupt isolation ----------------

  test("JDK backend compiles successfully even if the calling thread is interrupted") {
    // javac reads classpath jars via interruptible NIO channels; running on a Spark
    // task thread whose interrupt flag is set must NOT break compilation. The compile
    // runs on a dedicated worker thread, so the caller's interrupt does not reach the
    // jar reads. The caller's interrupt status must be preserved on return.
    assume(JdkCodeCompiler.isAvailable, "javax.tools.JavaCompiler not available")
    Thread.currentThread().interrupt()
    try {
      val (generated, _) = JdkCodeCompiler.compile(newCodeAndComment(sampleClassBody))
      assert(generated != null)
      assert(Thread.currentThread().isInterrupted,
        "caller's interrupt status should be preserved")
    } finally {
      // Clear the interrupt flag so it does not leak into subsequent tests.
      Thread.interrupted()
    }
  }
}

object CodeCompilerSuite {
  // Mirrors the mllib legacy save/load shape (a case class nested inside a Scala
  // `object`), whose binary name uses `$` as both the module suffix and the
  // nesting separator (CodeCompilerSuite$SaveLoadV1$Leaf) - the case the
  // reflection-based rewrite must preserve verbatim.
  object SaveLoadV1 {
    case class Leaf(x: Int)
  }

  private[codegen] trait Greeter {
    def hello(): String
  }

  private[codegen] abstract class Converter {
    def convert(o: Any): String = "base"
  }

  // Anonymous and local classes for the narrowing-soundness tests. They are held in vals
  // rather than built inline in the tests because scalac keeps an anonymous class's extra
  // members `public` only when the binding's type is inferred as the refined type; giving
  // the val an explicit type, or passing the expression as `Any`, makes them private.
  val anonWithExtraMethod = new java.util.HashMap[String, String]() {
    def extra(): String = "extra"
  }

  // Mixing in a Java constants interface is what actually yields public FIELDS: a Scala
  // `val` compiles to a private field plus an accessor, which only exercises the method
  // check. ObjectStreamConstants contributes 30 public static final fields and no method
  // beyond Comparator's, so this isolates the field clause of `narrowingVerdict`.
  val anonWithPublicFields = new java.util.Comparator[String] with java.io.ObjectStreamConstants {
    override def compare(a: String, b: String): Int = a.compareTo(b)
  }

  val anonWithSecondInterface = new java.util.Comparator[String] with Greeter {
    override def compare(a: String, b: String): Int = a.compareTo(b)
    override def hello(): String = "hi"
  }

  // An INSTANCE method whose only counterpart on the supertype is STATIC. scalac allows it,
  // since it does not treat a Java static as an inherited member, so `value()` is not an
  // override; javac rejects the pair outright, which is why `StaticClashBase` is written in
  // Java. A narrowed call binds the supertype's static `value()`, returning 1 rather than 7.
  val anonOverStaticClash = new StaticClashBase {
    def value(): Int = 7
  }

  // An OVERLOAD, not an override: `convert(String)` does not implement `convert(Any)`, so
  // no bridge is emitted. Narrowing would silently bind the call to the supertype's method.
  val anonWithOverload = new Converter {
    def convert(s: String): String = "anon"
  }

  // A bridged generic override alongside an unrelated overload of the same name and arity.
  // `compare(String, String)` is reached through a `compare(Object, Object)` bridge, but
  // `compare(Int, Int)` has no bridge of its own: after narrowing to `Comparator`, an
  // integer call would bind to `compare(Object, Object)` and land in the String-casting
  // bridge instead of the overload.
  val anonWithBridgeAndPrimitiveOverload = new java.util.Comparator[String] {
    override def compare(a: String, b: String): Int = a.compareTo(b)
    def compare(a: Int, b: Int): Int = a - b
  }

  // The same collision with a reference-typed overload. Boxing-blind parameter matching
  // would accept this one, since the bridge's `Object` parameters do accept `Integer`.
  val anonWithBridgeAndReferenceOverload = new java.util.Comparator[String] {
    override def compare(a: String, b: String): Int = a.compareTo(b)
    def compare(a: Integer, b: Integer): Int = a - b
  }

  // Sound counterpart of the two above, and the reason bridge matching treats boxing as
  // equivalence: scalac specializes this override to `apply(int)`, which is reached through
  // an `apply(Object)` bridge. Requiring the bridge parameter to accept the override's
  // parameter without boxing would reject it, since `Object` is not assignable from `int`.
  val specializedPrimitiveOverride = new scala.runtime.AbstractFunction1[Int, Boolean] {
    def apply(i: Int): Boolean = i > 0
  }

  // A lambda in the body makes scalac emit a `public static final $anonfun$...` helper on the
  // anonymous class. It is synthetic, so javac cannot name it ("cannot find symbol") and no
  // generated reference can reach it, so narrowing loses nothing and the static check has to
  // excuse it or every closure-carrying anonymous class would route to Janino.
  val anonWithLambdaBody = new java.util.Comparator[String] {
    override def compare(a: String, b: String): Int = {
      val len: String => Int = _.length
      len(a) - len(b)
    }
  }

  val plainLocal: java.util.ArrayList[String] = {
    class PlainLocal extends java.util.ArrayList[String]
    new PlainLocal
  }

  val localWithExtraMethod = {
    class LocalWithExtra extends java.util.ArrayList[String] {
      def extra(): String = "extra"
    }
    new LocalWithExtra
  }

  // A named class declared inside a local class: neither anonymous nor local itself
  // (`isMemberClass` is true), yet Java cannot name it either. Its supertype offers every
  // member, so only the canonical-name test keeps it from being narrowed to a binary name
  // javac would reject. The `$outer` field and accessor scalac adds are synthetic, hence
  // unreachable from generated source and no obstacle to narrowing.
  val memberOfLocalClass: Any = {
    class Holder {
      class Inner extends java.util.ArrayList[String]
      def make(): Any = new Inner
    }
    new Holder().make()
  }

  // The same shape plus one NON-synthetic public method, which is what actually puts a member
  // out of reach of `ArrayList`. Used where the test needs an unnarrowable member-of-local.
  val memberOfLocalClassWithExtra: Any = {
    class Holder {
      class Inner extends java.util.ArrayList[String] {
        def extra(): String = "e"
      }
      def make(): Any = new Inner
    }
    new Holder().make()
  }
}
