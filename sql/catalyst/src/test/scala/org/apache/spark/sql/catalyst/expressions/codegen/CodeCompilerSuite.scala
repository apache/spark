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
import java.net.{URI, URL, URLClassLoader}
import java.util.Collections
import javax.tools.{JavaFileObject, SimpleJavaFileObject, StandardLocation, ToolProvider}

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
  private def compileDynHelper(): File = {
    val dir = Utils.createTempDir()
    val src =
      "package dyn; public class DynHelper { public static int magic() { return 4242; } }"
    val compiler = ToolProvider.getSystemJavaCompiler
    val fm = compiler.getStandardFileManager(null, null, null)
    try {
      fm.setLocation(StandardLocation.CLASS_OUTPUT, Collections.singletonList(dir))
      val srcFile = new SimpleJavaFileObject(
          URI.create("string:///dyn/DynHelper.java"), JavaFileObject.Kind.SOURCE) {
        override def getCharContent(ignoreEncodingErrors: Boolean): CharSequence = src
      }
      assert(
        compiler.getTask(null, fm, null, null, null, Collections.singletonList(srcFile)).call(),
        "failed to compile dyn.DynHelper fixture")
    } finally {
      fm.close()
    }
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
}
