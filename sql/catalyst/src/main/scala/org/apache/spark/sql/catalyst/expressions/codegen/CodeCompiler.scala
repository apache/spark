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

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, InputStream, IOException, StringWriter}
import java.lang.reflect.{Member, Method, Modifier}
import java.net.{JarURLConnection, URI, URL}
import java.util.Locale
import java.util.concurrent.{Callable, ExecutionException, ExecutorService}
import javax.tools.{Diagnostic, DiagnosticCollector, FileObject, ForwardingJavaFileManager, JavaCompiler, JavaFileManager, JavaFileObject, SimpleJavaFileObject, StandardJavaFileManager, StandardLocation, ToolProvider}

import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

import com.google.common.cache.{Cache, CacheBuilder}
import com.google.common.util.concurrent.Uninterruptibles
import org.codehaus.commons.compiler.{CompileException, InternalCompilerException}
import org.codehaus.janino.ClassBodyEvaluator
import org.codehaus.janino.util.ClassFile
import org.codehaus.janino.util.ClassFile.CodeAttribute

import org.apache.spark.{JobArtifactSet, SparkEnv, TaskContext, TaskKilledException}
import org.apache.spark.executor.{ExecutorClassLoader, InputMetrics}
import org.apache.spark.internal.{Logging, LogKeys}
import org.apache.spark.metrics.source.CodegenMetrics
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Expression, UnsafeArrayData, UnsafeMapData, UnsafeRow}
import org.apache.spark.sql.catalyst.util.{ArrayData, CollationAwareUTF8String, CollationFactory, CollationSupport, MapData}
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.Decimal
import org.apache.spark.unsafe.Platform
import org.apache.spark.unsafe.types.{BinaryView, CalendarInterval, TimestampNanosVal, UTF8String, VariantVal}
import org.apache.spark.util.{ParentClassLoader, ThreadUtils, Utils}

/**
 * Backend used to compile generator-produced Java source into a [[GeneratedClass]].
 *
 * Two implementations are provided:
 *   - [[JaninoCodeCompiler]]: the default, uses Janino's `ClassBodyEvaluator`. Very fast.
 *   - [[JdkCodeCompiler]]: uses `javax.tools.JavaCompiler` from the JDK. Slower (~5x for
 *     large generated units, 30-300x for small ones), but maintained on the JDK
 *     release cadence and not subject to Janino's unmaintained-upstream risk.
 *
 * The backend is selected at compile time via [[SQLConf.CODEGEN_COMPILER]].
 */
trait CodeCompiler {
  /** Backend name as used in `spark.sql.codegen.compiler`. */
  def name: String

  /**
   * Compile a generator-produced class body into an instance of the
   * [[GeneratedClass]] subclass it defines.
   *
   * @return the instantiated generated class along with bytecode statistics.
   */
  def compile(code: CodeAndComment): (GeneratedClass, ByteCodeStats)
}

object CodeCompiler extends Logging {

  // Emit log messages under CodeGenerator's logger name: operators and tests
  // (SPARK-25113 / SPARK-51527) subscribe to that exact logger for codegen
  // compilation events, and the backends are implementation details of
  // `CodeGenerator.compile`, so their logs belong under its name.
  override protected def logName: String = classOf[CodeGenerator[_, _]].getName

  val JANINO: String = "janino"
  val JDK: String = "jdk"

  /**
   * Fully-qualified imports made available to generated code by both backends.
   *
   * For Janino these are passed to `ClassBodyEvaluator.setDefaultImports`.
   * For the JDK backend they are rendered into `import` statements inside the
   * synthesized compilation unit.
   *
   * This is the single shared list - anything added here automatically applies to
   * both backends. It intentionally excludes
   * `org.apache.spark.sql.catalyst.expressions.codegen.GeneratedClass` to avoid
   * a name collision with the generated subclass `GeneratedClass`; the extends
   * clause uses the fully-qualified name instead.
   *
   * When adding an entry, keep its SIMPLE name distinct from any `import` line a
   * generator emits at the top of a class body (currently only GenerateColumnAccessor
   * does this): javac rejects two single-type imports sharing a simple name
   * (JLS 7.5.1) while Janino resolves them leniently, so a collision would fail only
   * under the JDK backend.
   */
  val DefaultImports: Seq[String] = Seq(
    classOf[Platform].getName,
    classOf[InternalRow].getName,
    classOf[UnsafeRow].getName,
    classOf[BinaryView].getName,
    classOf[UTF8String].getName,
    classOf[Decimal].getName,
    classOf[CalendarInterval].getName,
    classOf[TimestampNanosVal].getName,
    classOf[VariantVal].getName,
    classOf[ArrayData].getName,
    classOf[UnsafeArrayData].getName,
    classOf[MapData].getName,
    classOf[UnsafeMapData].getName,
    classOf[Expression].getName,
    classOf[TaskContext].getName,
    classOf[TaskKilledException].getName,
    classOf[InputMetrics].getName,
    classOf[CollationAwareUTF8String].getName,
    classOf[CollationFactory].getName,
    classOf[CollationSupport].getName,
    QueryExecutionErrors.getClass.getName.stripSuffix("$")
  )

  /**
   * FQN of the generated class. Must NOT be under the `codegen` package or Janino
   * fails with `java.lang.InstantiationException`. The same name is used for both
   * backends so generated source, logs, and diagnostics name the same class
   * whichever backend compiles it. (Compiled results are NOT shared across
   * backends: the compile cache key includes the backend.)
   */
  val GeneratedClassName: String =
    "org.apache.spark.sql.catalyst.expressions.GeneratedClass"

  def active(): CodeCompiler = active(null)

  /**
   * Resolve the active backend for the given generated unit.
   *
   * The configured backend ([[SQLConf.CODEGEN_COMPILER]]) governs ordinary codegen. The
   * exception is codegen the JDK compiler is fundamentally *incapable* of compiling - not
   * merely slower at compiling - which is always routed to Janino regardless of the
   * configured backend. This is deterministic routing decided up front from the execution
   * context and the generated source; it is never a fallback after a failed compile. Three
   * such cases, all involving classes the JDK compiler cannot name that Janino's lenient
   * loader/lexer accepts:
   *
   *   - REPL / interactive sessions (spark-shell `$line*` wrappers, Spark Connect /
   *     Ammonite session artifacts): reachable only through a runtime class loader and
   *     carrying self-inconsistent reflection metadata the JDK compiler cannot resolve.
   *     This arm is context-wide by design: ALL codegen in such a session routes to
   *     Janino, whether or not the unit references a REPL class, because the reference
   *     cannot be told from the source text up front. See [[isReplContext]].
   *   - A reference to a class nested in a Scala `package object` (binary name
   *     `a.b.package$Inner`): `package` is a Java reserved word that cannot be spelled as
   *     an identifier in any form - Java has no backtick/escape, unlike Scala - so javac
   *     can neither parse `a.b.package.Inner` nor resolve the flat `a.b.package$Inner`.
   *     See [[requiresJaninoSource]].
   *   - A reference to a class the Java language forbids naming, i.e. an anonymous or local
   *     class (`a.b.Outer$1`, `a.b.Outer$$anon$1`) or a class nested inside one
   *     (`a.b.Outer$1$Inner`), that is shown to be unnarrowable. The JDK backend rewrites
   *     such a reference to the nearest nameable supertype, which works only while that
   *     supertype is itself referenceable and offers every member the generated code could
   *     access; this arm covers the classes for which reflection positively reports it does
   *     not. A class whose verdict reflection cannot determine is not routed: a reference to
   *     it in code position keeps its binary name, which javac rejects for these shapes.
   *     See [[JdkCodeCompiler.referencesUnnarrowableClass]].
   */
  def active(code: CodeAndComment): CodeCompiler = {
    // Resolve the configured backend first: when it is already Janino - by configuration or
    // because javac is absent - none of the overrides below can change the outcome, and
    // their source scans would be pure overhead.
    val configured = forBackend(SQLConf.get.codegenCompiler)
    if (configured eq JaninoCodeCompiler) {
      configured
    } else if (isReplContext) {
      logReplRoutingOnce()
      JaninoCodeCompiler
    } else if (requiresJaninoSource(code)) {
      logPackageObjectRoutingOnce()
      JaninoCodeCompiler
    } else if (code != null && JdkCodeCompiler.referencesUnnarrowableClass(code.body)) {
      logUnnarrowableClassRoutingOnce()
      JaninoCodeCompiler
    } else {
      configured
    }
  }

  // One-time visibility for the deterministic routing above: an operator who set
  // `jdk` should be able to tell from the logs why Janino still shows up.
  private val replRoutingLogged = new java.util.concurrent.atomic.AtomicBoolean(false)
  private def logReplRoutingOnce(): Unit = {
    if (replRoutingLogged.compareAndSet(false, true)) {
      logInfo(log"REPL / interactive session context detected; codegen is routed to " +
        log"Janino although ${MDC(LogKeys.CONFIG, SQLConf.CODEGEN_COMPILER.key)} " +
        log"requests another backend (the JDK compiler cannot resolve REPL-defined " +
        log"classes). This notice is logged once per JVM.")
    }
  }
  private val packageObjectRoutingLogged = new java.util.concurrent.atomic.AtomicBoolean(false)
  private def logPackageObjectRoutingOnce(): Unit = {
    if (packageObjectRoutingLogged.compareAndSet(false, true)) {
      logInfo(log"Generated code references a Scala package-object class; that unit is " +
        log"routed to Janino although ${MDC(LogKeys.CONFIG, SQLConf.CODEGEN_COMPILER.key)} " +
        log"requests another backend (`package` is a Java reserved word the JDK compiler " +
        log"cannot name). This notice is logged once per JVM.")
    }
  }
  private val unnarrowableClassRoutingLogged = new java.util.concurrent.atomic.AtomicBoolean(false)
  private def logUnnarrowableClassRoutingOnce(): Unit = {
    if (unnarrowableClassRoutingLogged.compareAndSet(false, true)) {
      logInfo(log"Generated code references a class Java cannot name (anonymous, local, or " +
        log"nested in one) that cannot be narrowed to a nameable supertype; that unit is " +
        log"routed to Janino although " +
        log"${MDC(LogKeys.CONFIG, SQLConf.CODEGEN_COMPILER.key)} requests another backend. " +
        log"This notice is logged once per JVM.")
    }
  }

  // A `package` segment in a qualified/binary class name - a Scala `package object`'s nested
  // class such as `a.b.package$Inner`. `package` is a Java reserved word the JDK compiler can
  // name in no form (parse error as `package.Inner`; unresolvable as the flat `package$Inner`),
  // whereas Janino's lexer scans `package$Inner` as one identifier.
  //
  // `package` is the only keyword scanned for, by design. It is the only Java keyword the
  // Scala compiler ever produces in a generated name (from `package object`); a class named
  // after any other keyword (`class int`) requires pathological user code. It is also the only
  // keyword that is *safe* to scan for: the rest (`int`, `new`, `this`, `return`, `switch`, ...)
  // occur as legitimate tokens throughout the generated Java, so matching them would route
  // almost all codegen to Janino, whereas a `package` token never appears in a generated class
  // body except as such a class reference. (A fully general check would inspect the resolved
  // class names rather than the source text, but that information is only available during
  // compilation, i.e. after the backend is already chosen.) The lookbehind keeps a legal
  // identifier like `mypackage$Inner` from matching; a false positive (e.g. text inside a string
  // literal) is harmless - it only picks Janino, a superset of what javac accepts.
  private val UnnameablePackageObjectClass = """(?<![\w$])package[.$]""".r
  private def requiresJaninoSource(code: CodeAndComment): Boolean = {
    // This runs on every compile() call (the result is part of the cache key), so gate
    // the regex scan behind an intrinsified substring search: generated bodies almost
    // never contain the literal `package` at all, and the regex runs only when they do.
    code != null && code.body.contains("package") &&
      UnnameablePackageObjectClass.findFirstIn(code.body).isDefined
  }

  /**
   * True when codegen is running in a REPL / interactive context, detected via the three
   * mechanisms Spark uses to ship such classes:
   *   - the active job/session carries a REPL or artifact class-dir URI
   *     ([[JobArtifactSet.getCurrentJobArtifactState]]'s `replClassDirUri`). This is the
   *     canonical signal: Spark Connect sets it per session and spark-shell falls back to
   *     it from `spark.repl.class.uri`. It is a thread-local set around both driver-side
   *     and executor-side work, so it catches driver-side codegen where no
   *     `ExecutorClassLoader` is in the loader chain (e.g. a Connect UDF over a local
   *     relation referencing an Ammonite `$sess` class); or
   *   - `spark.repl.class.uri` set in the active conf (spark-shell sets this globally); or
   *   - an [[org.apache.spark.executor.ExecutorClassLoader]] somewhere in the active
   *     class loader chain (created on executors when a session has such a class URI).
   *
   * The default (non-REPL) job state has no `replClassDirUri`, so ordinary codegen is not
   * affected. Any lookup failure conservatively reports `false`, which preserves the
   * configured backend.
   */
  private def isReplContext: Boolean = {
    def hasArtifactReplUri =
      try JobArtifactSet.getCurrentJobArtifactState.exists(_.replClassDirUri.isDefined)
      catch { case NonFatal(_) => false }
    def confHasReplUri =
      try Option(SparkEnv.get).exists(_.conf.contains("spark.repl.class.uri"))
      catch { case NonFatal(_) => false }
    def eclInChain =
      try {
        var loader = Utils.getContextOrSparkClassLoader
        var found = false
        while (loader != null && !found) {
          if (loader.isInstanceOf[ExecutorClassLoader]) found = true
          loader = loader.getParent
        }
        found
      } catch {
        case NonFatal(_) => false
      }
    hasArtifactReplUri || confHasReplUri || eclInChain
  }

  /**
   * Get the backend by name. SQLConf already validates the value via `checkValues`
   * at config-set time, so unknown names should not reach here in normal use;
   * tests may call this directly. When `jdk` is requested but the JDK compiler is
   * not present at runtime (a JRE-only image), this logs a warning once and falls
   * back to Janino so the query does not fail.
   */
  private[codegen] def forBackend(requested: String): CodeCompiler = {
    requested.toLowerCase(Locale.ROOT) match {
      case JANINO => JaninoCodeCompiler
      case JDK if JdkCodeCompiler.isAvailable => JdkCodeCompiler
      case JDK =>
        logJdkUnavailableOnce()
        JaninoCodeCompiler
      case other =>
        throw new IllegalArgumentException(
          s"Unknown ${SQLConf.CODEGEN_COMPILER.key} backend: $other " +
            s"(supported: ${Seq(JANINO, JDK).mkString(", ")})")
    }
  }

  private val jdkUnavailableWarned = new java.util.concurrent.atomic.AtomicBoolean(false)
  private def logJdkUnavailableOnce(): Unit = {
    if (jdkUnavailableWarned.compareAndSet(false, true)) {
      logWarning(log"${MDC(LogKeys.CONFIG, SQLConf.CODEGEN_COMPILER.key)}=jdk requested " +
        log"but javax.tools.JavaCompiler is not available on this runtime " +
        log"(JRE-only image?). Falling back to Janino for this JVM.")
    }
  }

  /**
   * Compute bytecode statistics for a set of compiled classes. Both backends
   * produce the same map shape (className -> classfile bytes), so the analysis is
   * shared. This is the only piece of code that depends on Janino's
   * `ClassFile` parser (from the `janino` artifact); it can be swapped for ASM
   * later without touching either backend.
   */
  private[codegen] def computeByteCodeStats(
      classBytecodes: Iterable[(String, Array[Byte])]): ByteCodeStats = {
    val perClass = classBytecodes.map { case (_, classBytes) =>
      val classCodeSize = classBytes.length
      CodegenMetrics.METRIC_GENERATED_CLASS_BYTECODE_SIZE.update(classCodeSize)
      try {
        val cf = new ClassFile(new ByteArrayInputStream(classBytes))
        val constPoolSize = cf.getConstantPoolSize
        val methodCodeSizes = cf.methodInfos.asScala.flatMap { method =>
          method.getAttributes.collect { case attr: CodeAttribute =>
            val byteCodeSize = attr.code.length
            CodegenMetrics.METRIC_GENERATED_METHOD_BYTECODE_SIZE.update(byteCodeSize)
            if (byteCodeSize > CodeGenerator.DEFAULT_JVM_HUGE_METHOD_LIMIT) {
              logInfo(log"Generated method too long to be JIT compiled: " +
                log"${MDC(LogKeys.CLASS_NAME, cf.getThisClassName)}." +
                log"${MDC(LogKeys.METHOD_NAME, method.getName)} is " +
                log"${MDC(LogKeys.BYTECODE_SIZE, byteCodeSize)} bytes")
            }
            byteCodeSize
          }
        }
        // Use `maxOption` to handle classes with no methods (e.g., a synthetic
        // module-info-style class). The original Janino-only code would have raised
        // UnsupportedOperationException there; we degrade gracefully to -1 instead.
        (methodCodeSizes.maxOption.getOrElse(-1), constPoolSize)
      } catch {
        case NonFatal(e) =>
          logWarning("Error calculating stats of compiled class.", e)
          (-1, -1)
      }
    }

    val (maxMethodSizes, constPoolSize) = perClass.unzip
    ByteCodeStats(
      maxMethodCodeSize = maxMethodSizes.maxOption.getOrElse(-1),
      maxConstPoolSize = constPoolSize.maxOption.getOrElse(-1),
      // Minus 2 for `GeneratedClass` and an outer-most generated class.
      // Both backends wrap the class body in a single outer declaration, so the
      // emitted class count has the same shape (1 outer wrapper + K user-declared
      // classes) and the offset yields the same value under either backend.
      // `max(0, ...)` keeps an unexpected emit shape from going negative.
      numInnerClasses = math.max(0, classBytecodes.size - 2))
  }

  /**
   * Log the generated source on a compilation failure. Behaviour matches the
   * original [[CodeGenerator]] implementation. `maxLines` (the session's
   * `loggingMaxLinesForCodegen`) is captured by the CALLER: the JDK backend invokes
   * this from its compile worker thread, where `SQLConf.get` would silently return
   * the default conf instead of the calling session's.
   */
  private[codegen] def logGeneratedCodeOnFailure(code: CodeAndComment, maxLines: Int): Unit = {
    val formatted = s"\n${CodeFormatter.format(code, maxLines)}"
    if (Utils.isTesting) {
      logError(formatted)
    } else {
      logInfo(formatted)
    }
  }
}

/**
 * The default backend using Janino's `ClassBodyEvaluator`.
 *
 * This lifts the previous body of `CodeGenerator.doCompile`, with the only
 * changes being: imports moved to `CodeCompiler.DefaultImports`, stats
 * computation moved to `CodeCompiler.computeByteCodeStats` (which degrades to
 * `-1` for a class with no methods instead of throwing; see its comment).
 * Behaviour is otherwise preserved, including the [[ParentClassLoader]] wrapping
 * (workaround for the Janino `findIClass` behaviour described in SPARK-15622 /
 * SPARK-11636).
 */
object JaninoCodeCompiler extends CodeCompiler with Logging {

  override val name: String = CodeCompiler.JANINO

  // Route source-code/debug log emissions under CodeGenerator's logger name.
  override protected def logName: String = classOf[CodeGenerator[_, _]].getName

  override def compile(code: CodeAndComment): (GeneratedClass, ByteCodeStats) = {
    val evaluator = new ClassBodyEvaluator()

    // See SPARK-15622 / SPARK-11636 for why this wrapping is required.
    val parentClassLoader = new ParentClassLoader(Utils.getContextOrSparkClassLoader)
    evaluator.setParentClassLoader(parentClassLoader)
    evaluator.setClassName(CodeCompiler.GeneratedClassName)
    evaluator.setDefaultImports(CodeCompiler.DefaultImports: _*)
    evaluator.setExtendedClass(classOf[GeneratedClass])

    logBasedOnLevel(SQLConf.get.codegenLogLevel) {
      // Only add extra debugging info to byte code when we are going to print the source code.
      evaluator.setDebuggingInformation(true, true, false)
      log"\n${MDC(LogKeys.CODE, CodeFormatter.format(code))}"
    }

    val codeStats =
      try {
        evaluator.cook("generated.java", code.body)
        CodeCompiler.computeByteCodeStats(evaluator.getBytecodes.asScala)
      } catch {
        case e: InternalCompilerException =>
          logError("Failed to compile the generated Java code.", e)
          CodeCompiler.logGeneratedCodeOnFailure(code, SQLConf.get.loggingMaxLinesForCodegen)
          throw QueryExecutionErrors.internalCompilerError(e)
        case e: CompileException =>
          logError("Failed to compile the generated Java code.", e)
          CodeCompiler.logGeneratedCodeOnFailure(code, SQLConf.get.loggingMaxLinesForCodegen)
          throw QueryExecutionErrors.compilerError(e)
      }

    (evaluator.getClazz().getConstructor().newInstance().asInstanceOf[GeneratedClass], codeStats)
  }
}

/**
 * Alternative backend using the JDK's `javax.tools.JavaCompiler`.
 *
 * Wraps the generator-produced class body in a synthesized compilation unit
 * (package + imports + `public class GeneratedClass extends ...`) before
 * handing it to the compiler. Compiled classes are captured in memory and
 * loaded through a private [[ClassLoader]] that mirrors the Janino backend's
 * [[ParentClassLoader]] wrapping (SPARK-15622 / SPARK-11636) so behaviour on
 * containerised deployments stays consistent.
 *
 * Class resolution: referenced classes are resolved through the task's context
 * [[ClassLoader]] (see [[ClassLoaderFileManager]]) rather than a file-based
 * `-classpath`, mirroring how Janino resolves them. This lets the JDK compiler see
 * classes that exist only on a runtime loader - REPL-generated, Spark Connect
 * session artifacts - and avoids handing javac a giant classpath to index.
 *
 * Threading: the actual javac invocation runs on a dedicated single-threaded
 * executor (see `compileExecutor`). This is required for correctness on Spark
 * task threads (jar reads through interruptible NIO channels vs. task
 * interruption), and it also confines the shared, not-thread-safe
 * [[StandardJavaFileManager]] (used for platform-class lookups) to one
 * thread, so no extra locking is needed. Caller threads only build the source and
 * capture the context classloader, then await the result.
 *
 * Resource lifecycle: the shared [[StandardJavaFileManager]] is intentionally
 * never closed. Like the compile executor and the per-jar package index, it is
 * JVM-lifetime state rather than a per-compile resource, so this is not a leak.
 *
 * Performance is roughly 5x slower than Janino for large generated units and
 * 30-300x slower for small ones.
 * The benefit is decoupling Spark from Janino's release cadence.
 */
object JdkCodeCompiler extends CodeCompiler with Logging {

  override val name: String = CodeCompiler.JDK

  // Route source-code/debug log emissions under CodeGenerator's logger name.
  override protected def logName: String = classOf[CodeGenerator[_, _]].getName

  /** True if `javax.tools.JavaCompiler` is available on this runtime. */
  lazy val isAvailable: Boolean = ToolProvider.getSystemJavaCompiler != null

  private lazy val compiler: JavaCompiler = {
    val c = ToolProvider.getSystemJavaCompiler
    require(c != null,
      "javax.tools.JavaCompiler is not available; check isAvailable before use")
    c
  }

  /**
   * Shared file manager. `StandardJavaFileManager` is not thread-safe; reuse is safe
   * here because every javac invocation runs on the single-threaded [[compileExecutor]].
   */
  private lazy val sharedFileManager: StandardJavaFileManager =
    compiler.getStandardFileManager(null, null, null)

  /**
   * Compiler options applied to every compilation.
   *
   * There is deliberately no `--release`/`-source`/`-target`: the compiled class is
   * loaded only into the same JVM that produced it (the cache is in-memory; generated
   * code travels between JVMs as SOURCE), so the emitted class-file version is always
   * consistent with the running runtime and there is no cross-compilation target to
   * pin. Pinning `--release` would only reroute the delegate file manager's
   * platform-class lookups through `ct.sym`, adding overhead without benefit.
   *
   * Note there is no `-classpath`: the [[ClassLoaderFileManager]] resolves the
   * `CLASS_PATH` location through the compile's parent [[ClassLoader]] (the task's
   * context classloader) rather than a file-based classpath. This mirrors how
   * Janino resolves referenced classes, so the JDK backend sees exactly what
   * Janino would - including classes that exist only on a runtime classloader
   * (REPL-generated, Spark Connect session artifacts) and never on
   * `java.class.path`. It also avoids handing javac a giant `-classpath` to index,
   * which both bloats compiler memory and is brittle to harvest correctly across
   * driver / executor / Connect deployments.
   */
  private val compileOptions: java.util.List[String] = Seq(
    "-proc:none",      // skip annotation processing
    "-g:none",         // skip debug info
    "-nowarn",         // suppress warnings
    "-implicit:none",  // do not compile referenced source files
    "-Xlint:none"      // disable lints
  ).asJava

  /** Source-position package name and simple class name derived once. */
  private val packageName: String =
    CodeCompiler.GeneratedClassName.substring(0, CodeCompiler.GeneratedClassName.lastIndexOf('.'))
  private val simpleName: String =
    CodeCompiler.GeneratedClassName.substring(CodeCompiler.GeneratedClassName.lastIndexOf('.') + 1)

  /** Rendered import block, computed once. */
  private val importBlock: String =
    CodeCompiler.DefaultImports.map(i => s"import $i;").mkString("\n")

  /** FQN of the abstract base used in the extends clause (avoids import collision). */
  private val extendsFqn: String = classOf[GeneratedClass].getName

  /**
   * Wrap a generator-produced class body in a full compilation unit. Class name
   * matches Janino's output so logs and diagnostics name the same class whichever
   * backend compiled it. `classLoader` resolves the candidate inner-class
   * references for the `$`-rewrite (see [[rewriteInnerClassRefs]]); it must be the
   * same loader the compile resolves classes through.
   *
   * Hoisted imports are deliberately left un-rewritten: an `import` requires a canonical
   * name, so a binary inner-class name there is a javac error whether or not it is
   * narrowed.
   */
  private[codegen] def wrapAsCompilationUnit(body: String, classLoader: ClassLoader): String = {
    val (extraImports, cleanedBody) = extractLeadingImports(body)
    val javacBody = stripFunction1ApplyBridges(cleanedBody)
    // Built by plain concatenation, NOT a stripMargin template: stripMargin
    // post-processes the final interpolated string, so a generated-body line whose
    // first non-blank character is `|` (e.g. a line-wrapped `||` condition) would
    // lose that character. No current generator emits such a line, but the unit
    // must not depend on that.
    s"package $packageName;\n" +
      s"$importBlock\n" +
      s"$extraImports\n" +
      s"public class $simpleName extends $extendsFqn {\n" +
      s"${rewriteInnerClassRefs(javacBody, classLoader)}\n" +
      "}\n"
  }

  // The explicit `scala.Function1` `apply(Object)` bridge that projection codegen emits
  // for the Janino backend (see `CodeGenerator.function1ApplyBridge`). javac synthesizes
  // this bridge itself for the typed `apply(InternalRow)` override and rejects an explicit
  // duplicate with a "name clash" error, so it must be removed before compiling with the
  // JDK backend. This pattern matches exactly the shape `function1ApplyBridge` emits
  // (whitespace tolerant, `\1` ties the cast operand to the parameter); keep them in sync.
  private val Function1ApplyBridgePattern =
    ("""(?s)public\s+java\.lang\.Object\s+apply\(\s*java\.lang\.Object\s+(\w+)\s*\)\s*""" +
      """\{\s*return\s+apply\(\(\s*InternalRow\s*\)\s*\1\s*\)\s*;\s*\}""").r

  /** Remove the Janino-only Function1 `apply(Object)` bridges so javac does not clash. */
  private[codegen] def stripFunction1ApplyBridges(body: String): String =
    if (body.contains("apply(java.lang.Object")) {
      Function1ApplyBridgePattern.replaceAllIn(body, "")
    } else {
      body
    }

  /**
   * Some generators (e.g. GenerateColumnAccessor) emit `import` statements at
   * the top of the class body. Janino's ClassBodyEvaluator treats those as
   * imports for the synthesized class, but the JDK compiler rejects imports
   * inside a class declaration ("illegal start of type"). Extract any leading
   * `import` lines from the body so they can be hoisted into the compilation
   * unit header.
   */
  private[codegen] def extractLeadingImports(body: String): (String, String) = {
    // Fast path: no leading `import` line (every generator but GenerateColumnAccessor).
    // Skips the full line-split allocation.
    var p = 0
    while (p < body.length && Character.isWhitespace(body.charAt(p))) p += 1
    if (!body.startsWith("import ", p)) return ("", body)
    // The `-1` limit keeps trailing empty lines so the reconstruction is faithful.
    val lines = body.split("\n", -1)
    val imports = new StringBuilder
    var i = 0
    var scanning = true
    while (scanning && i < lines.length) {
      val trimmed = lines(i).trim
      if (trimmed.startsWith("import ")) {
        imports.append(lines(i)).append('\n')
        i += 1
      } else if (trimmed.isEmpty) {
        i += 1
      } else {
        scanning = false
      }
    }
    if (i == 0) {
      ("", body)
    } else {
      (imports.toString, lines.drop(i).mkString("\n"))
    }
  }

  /**
   * Rewrite JVM-binary inner-class references into the Java-source form the JDK
   * compiler accepts. Spark generators emit class names via `Class#getName` in
   * many places; for nested classes that yields the binary form (`Outer$Inner`),
   * which Janino accepts as a source-level identifier but the JDK compiler does
   * not.
   *
   * The correct source form depends on HOW the class is nested, and that cannot
   * be told from the text alone:
   *   - a regular nested class `Outer$Inner` must be written `Outer.Inner`;
   *   - a class nested inside a Scala `object` has a binary name whose `$`
   *     separators include the module suffix (e.g. `Model$SaveLoad$Leaf` where
   *     `SaveLoad` is an object), and the JDK compiler resolves it ONLY via the
   *     raw binary name - the dotted canonical form `Model.SaveLoad$.Leaf` makes
   *     javac reconstruct a non-existent `Model$SaveLoad$$Leaf`.
   * Textually `Model$SaveLoad$Leaf` (object-nested) is indistinguishable from
   * `A$B$C` (three regular classes) yet they need opposite treatment, so the
   * decision is made by resolving each candidate against the compile classpath
   * and consulting reflection: `getCanonicalName` is the right source form when
   * it is free of `$`, otherwise the binary name is.
   *
   * For each maximal qualified-name token that contains `$`, we find the longest
   * dot-delimited prefix that loads as a class and replace it with that
   * reflection-derived name, leaving any trailing member access untouched
   * (so `Foo$.MODULE$.apply` and `List$.MODULE$.newBuilder()` resolve correctly).
   * Tokens whose prefixes do not resolve - notably references to the
   * not-yet-compiled inner classes of the generated unit itself - fall back to a
   * conservative regex that dots `$`-before-uppercase, matching the historical
   * behaviour for those.
   *
   * The rewrite is applied only to actual code spans: string literals, char
   * literals, and `//` / block comments are copied verbatim so that a `$Upper`
   * sequence inside generated string data (e.g. a column name or error message)
   * is never corrupted.
   */
  private[codegen] def rewriteInnerClassRefs(body: String, classLoader: ClassLoader): String = {
    val out = new java.lang.StringBuilder(body.length + 16)
    val code = new java.lang.StringBuilder()
    val n = body.length
    // A token's rewritten form is stable for a given classloader; memoize within
    // this call so repeated type references resolve at most once.
    val memo = mutable.HashMap.empty[String, String]

    def flushCode(): Unit = {
      if (code.length > 0) {
        out.append(rewriteCodeSpan(code.toString, classLoader, memo))
        code.setLength(0)
      }
    }

    // Copy a quoted literal (string or char) verbatim, honoring backslash escapes.
    def copyQuoted(start: Int, quote: Char): Int = {
      out.append(quote)
      var j = start + 1
      var closed = false
      while (j < n && !closed) {
        val ch = body.charAt(j)
        if (ch == '\\' && j + 1 < n) {
          out.append(ch).append(body.charAt(j + 1))
          j += 2
        } else {
          out.append(ch)
          j += 1
          if (ch == quote) closed = true
        }
      }
      j
    }

    var i = 0
    while (i < n) {
      val c = body.charAt(i)
      if (c == '"' || c == '\'') {
        flushCode()
        i = copyQuoted(i, c)
      } else if (c == '/' && i + 1 < n && body.charAt(i + 1) == '/') {
        flushCode()
        while (i < n && body.charAt(i) != '\n') { out.append(body.charAt(i)); i += 1 }
      } else if (c == '/' && i + 1 < n && body.charAt(i + 1) == '*') {
        flushCode()
        out.append("/*")
        i += 2
        while (i < n && !(body.charAt(i) == '*' && i + 1 < n && body.charAt(i + 1) == '/')) {
          out.append(body.charAt(i)); i += 1
        }
        // The scan exits either at the `*/` terminator (then i + 1 < n holds by the
        // loop condition) or at end-of-body for an unterminated comment, whose
        // characters the loop already copied verbatim.
        if (i + 1 < n) { out.append("*/"); i += 2 }
      } else {
        code.append(c)
        i += 1
      }
    }
    flushCode()
    out.toString
  }

  /**
   * Rewrite the qualified-name tokens of a code span (no literals or comments).
   * Runs of `[A-Za-z0-9_$.]` are treated as candidate qualified names; only
   * those containing `$` are resolved (others cannot be binary inner-class
   * references), and everything else is copied verbatim so whitespace and
   * punctuation are preserved exactly.
   */
  private def rewriteCodeSpan(
      span: String,
      classLoader: ClassLoader,
      memo: mutable.Map[String, String]): String = {
    val sb = new java.lang.StringBuilder(span.length + 16)
    val n = span.length
    var i = 0
    while (i < n) {
      val c = span.charAt(i)
      if (isNameStart(c)) {
        val start = i
        i += 1
        while (i < n && isNamePart(span.charAt(i))) i += 1
        val token = span.substring(start, i)
        if (token.indexOf('$') < 0) {
          sb.append(token)
        } else {
          sb.append(memo.getOrElseUpdate(token, rewriteQualifiedName(token, classLoader)))
        }
      } else {
        sb.append(c)
        i += 1
      }
    }
    sb.toString
  }

  private def isNameStart(c: Char): Boolean =
    (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || c == '_' || c == '$'

  private def isNamePart(c: Char): Boolean =
    isNameStart(c) || (c >= '0' && c <= '9') || c == '.'

  /**
   * Resolve a `$`-containing qualified token to a JDK-compiler-acceptable form by
   * finding the longest dot-delimited prefix that loads as a class and replacing
   * it with its reflection-derived source name, keeping any trailing member
   * access. Falls back to the conservative `$`-before-uppercase regex when no
   * prefix resolves (e.g. inner classes of the not-yet-compiled generated unit).
   */
  private def rewriteQualifiedName(token: String, classLoader: ClassLoader): String = {
    loadLongestPrefix(token, classLoader) match {
      case Some((cls, rest)) =>
        val sourceName = narrowedSourceName(cls)
        val resolved = if (rest.isEmpty) sourceName else s"$sourceName.$rest"
        // `split('.')` drops a trailing empty segment, so a token that ends in `.`
        // (member access wrapped onto the next line) must get its dot restored.
        if (token.endsWith(".")) resolved + "." else resolved
      case None =>
        InnerClassRefPattern.replaceAllIn(token, ".")
    }
  }

  /**
   * The source name to emit for `cls`: the name of the nearest class Java can name, in the
   * form javac accepts.
   *
   * Narrowing happens only on a positive [[Narrowable]] verdict; anything else keeps the
   * binary name. Consulting the verdict is the load-bearing part, because the reflective
   * failures are not symmetric: the supertype climb can succeed while member enumeration
   * throws, so climbing here without it would narrow a class whose members were never
   * checked, to a public supertype javac happily accepts. Taking the target from the verdict
   * rather than re-deriving it costs one climb instead of two. Keeping the binary name of a
   * class Java cannot name yields a name javac rejects, so the unit fails to compile rather
   * than compiling into a wrong answer.
   */
  private def narrowedSourceName(cls: Class[_]): String = narrowingVerdict(cls) match {
    case Narrowable(target) => sourceNameOf(target)
    case Unnarrowable => cls.getName
    case Unknown(cause) =>
      // Logged from here, not from the verdict: this is the one call site that knows the
      // token really is a type reference in code position, so the message's claim that the
      // unit will fail to compile actually holds. The routing scan reads the raw body and
      // can evaluate a token inside a literal, where the same verdict costs nothing.
      logUnevaluableClassOnce(cls, cause)
      cls.getName
  }

  /**
   * Find the longest dot-delimited prefix of `token` that loads as a class, and return it
   * with the remaining member access. Only a prefix containing `$` is tried, since only
   * such a prefix can be a binary inner-class name. Returns None when nothing loads (e.g.
   * an inner class of the not-yet-compiled generated unit).
   */
  private def loadLongestPrefix(
      token: String,
      classLoader: ClassLoader): Option[(Class[_], String)] = {
    val parts = token.split('.')
    var k = parts.length
    while (k >= 1) {
      val prefix = parts.iterator.take(k).mkString(".")
      if (prefix.indexOf('$') >= 0) {
        loadWithoutInit(prefix, classLoader) match {
          case Some(cls) => return Some((cls, parts.iterator.drop(k).mkString(".")))
          case None => // try a shorter prefix
        }
      }
      k -= 1
    }
    None
  }

  /** Load `binaryName` without initializing it; None when it is not a loadable class. */
  private def loadWithoutInit(binaryName: String, classLoader: ClassLoader): Option[Class[_]] = {
    try {
      // scalastyle:off classforname
      // Load with the exact loader passed in (the task's context loader), not the Spark
      // class loader, so the JDK compiler sees what the runtime would; Utils.classForName
      // cannot target an arbitrary loader.
      Some(Class.forName(binaryName, false, classLoader))
      // scalastyle:on classforname
    } catch {
      case _: ClassNotFoundException | _: LinkageError => None
      case NonFatal(_) => None
    }
  }

  /**
   * The source name the JDK compiler accepts for `cls`: the canonical name when it is a
   * plain dotted identifier name (regular nesting, e.g. `java.util.Map.Entry`), otherwise
   * the binary name. The binary name is required for classes nested in Scala objects and
   * for Scala companion-object classes, whose canonical form carries a module `$` that
   * javac cannot resolve, and for Scala operator-named classes (e.g.
   * `scala.collection.immutable.::`) whose canonical form is not a valid Java identifier -
   * in all those cases the binary name is itself a legal Java type reference.
   *
   * The canonical name is also rejected when it is not a faithful rename of the binary
   * name - it must keep the same package. Scala REPL classes (e.g.
   * `$line21.$read$$iw$TestCaseClass`) report a misleading `getCanonicalName` that drops
   * the package and returns just the simple name (`TestCaseClass`); using it would corrupt
   * the reference into an unqualified one javac cannot resolve.
   *
   * Reflection here can raise a `LinkageError` when the class loaded but its enclosing
   * class did not (a partial or shaded jar); `NonFatal` does not cover that, so it is
   * caught explicitly and the binary name used, matching what an unresolvable prefix
   * yields in [[loadLongestPrefix]].
   */
  private def sourceNameOf(cls: Class[_]): String = {
    val canonical =
      try cls.getCanonicalName
      catch {
        case _: LinkageError => null
        case NonFatal(_) => null
      }
    val pkg = cls.getPackageName
    val usableCanonical = canonical != null && isPlainDottedName(canonical) &&
      (pkg.isEmpty || canonical.startsWith(pkg + "."))
    if (usableCanonical) canonical else cls.getName
  }

  /**
   * Climb to the nearest class that can be named in Java source. A class whose
   * `getCanonicalName` is null has no source-referenceable name: the JDK compiler rejects
   * a qualified reference to it even when the `.class` file is on the classpath, because
   * the Java language forbids naming it. That covers anonymous classes (a Scala
   * `new HashMap[..]() {...}` compiles to `Outer$$anon$1`), local classes, and classes
   * nested inside either of those (`Outer$1$Inner`), which are themselves neither
   * anonymous nor local. Janino does not care - it resolves any class by its runtime
   * binary name - so this only matters for the JDK backend. For an anonymous class
   * implementing an interface (`new Comparator() {...}`, whose superclass is `Object`),
   * the implemented interface is preferred over `Object`.
   *
   * Narrowing a reference this way is only sound while every member the generated code
   * could access remains reachable through the replacement type; a unit referencing a class
   * for which reflection shows that does not hold is routed to Janino instead of being
   * rewritten. When reflection cannot tell, the unit stays on the JDK backend and the
   * reference keeps its binary name, which javac rejects, so the unit fails to compile
   * rather than compiling into a wrong answer (see [[referencesUnnarrowableClass]] and
   * [[CodeCompiler.active]]).
   */
  private def nameableSupertype(start: Class[_]): Class[_] = {
    var c: Class[_] = start
    while (c != null && c.getCanonicalName == null) {
      val sup: Class[_] = c.getSuperclass
      c =
        if (sup != null && (sup ne classOf[Object])) sup
        else c.getInterfaces.headOption.getOrElse(sup)
    }
    if (c == null) classOf[Object] else c
  }

  /**
   * True when `body` references a class that [[nameableSupertype]] cannot narrow soundly,
   * i.e. the class carries a public member that the replacement type does not offer, or
   * the replacement type is itself one javac cannot reference. Such a unit must go to
   * Janino: rewriting the reference would either drop the member or emit a type name javac
   * rejects.
   *
   * Called from [[CodeCompiler.active]] on every compile, so it is gated behind a scan for
   * `$` followed by a digit. Every class the Java language forbids naming carries that in
   * its binary name (`Outer$1`, `Outer$1Local`, Scala's `Outer$$anon$1`, and their nested
   * members `Outer$1$Inner`). The other `$` forms the rewrite handles do not: regular
   * nesting (`Map$Entry`), Scala modules (`Foo$`, `Model$Load$Leaf`), package objects
   * (`pkg$Inner`), specialized (`Function1$mcII$sp`) and operator-named (`$colon$colon`)
   * classes. Runtime-synthesized classes need not carry it at all: a lambda is
   * `Outer$$Lambda$14/0x...` on JDK 17 but `Outer$$Lambda/0x...` on 21 and later, and a
   * hidden class is `Host$Named/0x...`. Neither routes on its own name: the tokenizer stops
   * at `/`, and the truncated remainder either resolves to nothing (a lambda) or to the
   * ordinary class the hidden class was defined from, whose own verdict is then the one that
   * answers. On JDK 17 the lambda shape does pass the gate, at the cost of one failed load.
   *
   * The scan adds one linear pass over the body ahead of the compile-cache lookup, behind
   * the intrinsified `$`-digit gate that ordinary generated code fails immediately.
   *
   * The scan reads the raw body, so a `$`-digit sequence outside code position (inside a
   * string literal, say a regex the optimizer folded in, or inside a comment) can trigger
   * the resolution attempt. That is harmless: an unloadable token is ignored, and a loadable
   * one only ever picks Janino, which accepts a superset of what javac does.
   */
  private[codegen] def referencesUnnarrowableClass(body: String): Boolean = {
    if (!containsDollarDigit(body)) return false
    val classLoader = Utils.getContextOrSparkClassLoader
    val checked = mutable.HashSet.empty[String]
    var i = 0
    val n = body.length
    while (i < n) {
      if (isNameStart(body.charAt(i))) {
        val start = i
        i += 1
        while (i < n && isNamePart(body.charAt(i))) i += 1
        val token = body.substring(start, i)
        if (containsDollarDigit(token) && checked.add(token) &&
            routesOnToken(token, classLoader)) {
          return true
        }
      } else {
        i += 1
      }
    }
    false
  }

  /**
   * True when `token` resolves to a class whose reference has to go to Janino, i.e. one
   * [[narrowingVerdict]] positively reports as [[Unnarrowable]].
   *
   * An [[Unknown]] verdict does not route: it is no evidence that narrowing is unsafe, only
   * that reflection could not tell. The token then contributes nothing to the decision, so
   * unless some other token in the same body is [[Unnarrowable]] the unit stays on the
   * configured backend, where a reference in code position keeps its binary name and javac
   * rejects it, so the unit fails to compile rather than compiling into a wrong answer.
   * Routing it to Janino would compile it instead, which is the better outcome for this one
   * unit, but it would also let a truncated classpath quietly move work off the configured
   * backend. The routing arms exist for source Spark knows javac cannot express, not for a
   * broken deployment.
   */
  private def routesOnToken(token: String, classLoader: ClassLoader): Boolean = {
    loadLongestPrefix(token, classLoader).exists {
      case (cls, _) => narrowingVerdict(cls) match {
        case Unnarrowable => true
        case Narrowable(_) | Unknown(_) => false
      }
    }
  }

  /** True iff `s` holds a `$` immediately followed by an ASCII digit. */
  private[codegen] def containsDollarDigit(s: String): Boolean = {
    var i = s.indexOf('$')
    while (i >= 0 && i < s.length - 1) {
      val next = s.charAt(i + 1)
      if (next >= '0' && next <= '9') return true
      i = s.indexOf('$', i + 1)
    }
    false
  }

  /**
   * Whether a reference to a class can be replaced by the nearest class Java can name.
   *
   * [[Narrowable]] carries the replacement type, so the decision and the name emitted for it
   * come from one evaluation. The other two both keep the JDK backend from rewriting a
   * reference, but they differ in what a caller may conclude: [[Unnarrowable]] is positive
   * evidence that narrowing loses access, so the unit is routed to Janino, while [[Unknown]]
   * means reflection could not answer (a partial or shaded jar) and is no evidence either
   * way.
   */
  private sealed trait NarrowingVerdict
  private case class Narrowable(target: Class[_]) extends NarrowingVerdict
  private case object Unnarrowable extends NarrowingVerdict
  private case class Unknown(cause: Throwable) extends NarrowingVerdict

  /**
   * The verdict for replacing a reference to `cls` with [[nameableSupertype]]. A class that
   * is already nameable needs no narrowing, and only has to pass the accessibility check
   * below on itself.
   *
   * Otherwise two things must hold. First, the replacement type must be one the generated
   * unit can reference: it and every enclosing class must be public. Same-package is NOT
   * sufficient even though javac would accept it - the generated class is defined into
   * `org.apache.spark.sql.catalyst.expressions` but loaded by [[InMemoryClassLoader]], so
   * its runtime package differs from the same-named package on the app loader and a
   * package-private access would fail with `IllegalAccessError` at execution time instead
   * of at compile time. Second, every public member of the concrete class - including
   * inherited ones, since the generated code may access any of them - must be reachable on
   * the replacement type.
   *
   * A member is matched by its exact erased signature, with one allowance for bridges: an
   * override of a generic method has a narrower erasure than the supertype declaration it
   * implements (`compare(String, String)` against `Comparator.compare(Object, Object)`),
   * and the compiler emits a bridge carrying the supertype's signature. Such a method is
   * safe to narrow because `invokevirtual` on the supertype signature still dispatches to
   * the override. An overload has no bridge of its own, so it is rejected - and it must be,
   * because narrowing binds the call to the supertype's method instead: `Invoke` codegen
   * always wraps the call in an explicit cast, which would hide the type mismatch from
   * javac and silently produce the wrong result rather than fail to compile.
   *
   * Telling the two apart needs the bridge to be matched to the specific method it forwards
   * to, not merely to some method of the same name and arity. A class can hold both shapes
   * at once - an anonymous `Comparator[String]` that also declares `compare(int, int)` has
   * a bridge for the override and an unrelated overload sharing its name and arity - and
   * excusing the whole name/arity group would let the overload through. So a bridge covers
   * a method only when their parameter types line up (boxing counts as equivalent, since a
   * specialized primitive override such as `apply(int)` is reached through an
   * `apply(Object)` bridge), and only when it is the group's sole non-bridge method: with
   * two of them the bridge cannot forward to both, so at least one becomes unreachable
   * after narrowing.
   *
   * Fields and static methods get no such allowance. Both are bound statically, so they are
   * matched by declaring class rather than by signature: a class that redeclares an inherited
   * public field, or hides an inherited public static method, exposes it under a name the
   * replacement type also answers to, and the narrowed reference would silently reach the
   * replacement type's member instead. `getFields`/`getMethods` report the hiding and the
   * hidden member both, so a name- or signature-based check would accept the pair. There is
   * no counterpart to `invokevirtual`'s re-dispatch here, so the only safe such member is one
   * the replacement type declares or inherits itself.
   *
   * Reflection over either class can raise a `LinkageError` when a signature or an
   * enclosing class names something the loader cannot find (a partial or shaded jar), and
   * the failure is not symmetric: the climb can succeed while member enumeration throws, so
   * a verdict of [[Narrowable]] would let the rewrite emit a supertype name javac accepts
   * for a class whose members were never checked. Such a class is reported [[Unknown]]
   * instead, which keeps both the routing decision and the rewrite from acting on it.
   *
   * A non-`LinkageError` failure is caught the same way, because this file already treats
   * this reflection as fallible in both directions: [[sourceNameOf]] guards
   * `getCanonicalName` with `NonFatal`, as did the `resolveSourceName` that it and
   * [[loadWithoutInit]] were split out of, over its whole load-climb-name sequence. Catching
   * both is also what lets the routing scan read the raw body safely. A token there need not
   * be a type reference in this unit at all: a class name embedded in a generated
   * error-message literal reaches this method exactly as a real reference does, and an
   * escaping exception would cost the unit its compile over one, at the routing step before
   * any compile is attempted (`CodeGenerator.compile` only unwraps cache exceptions).
   */
  private def narrowingVerdict(cls: Class[_]): NarrowingVerdict = {
    try {
      // An array class cannot arrive from the tokenizer: `javaType` renders an array as
      // `component[]` and neither `[` nor `;` is a name character, so `foo.Bar$1[]` yields
      // the component token, which is the one that needs the verdict. A future caller could
      // still pass one, and it would narrow silently: an array of an unnameable component
      // type has a null canonical name and only Object's members, so the climb lands on
      // `Cloneable` and the member check passes. Reporting it unnarrowable routes the unit
      // to Janino, which needs no rewrite for the component name it would find there.
      if (cls.isArray) return Unnarrowable
      val target = nameableSupertype(cls)
      // A nameable class needs no narrowing, only a check that the generated unit can reach
      // it at run time. That is the class's own modifier, NOT [[isPubliclyNameable]]'s walk
      // over the enclosing chain: for a nested class `getModifiers` reports the source-level
      // modifier from the InnerClasses attribute, while the JVM checks the class file's own
      // `ACC_PUBLIC`, and a public class nested in a package-private one has it and is
      // reachable across loaders. The walk is the right test for the climbed target below,
      // where rejecting means routing to Janino. Here it would mean emitting this class's own
      // binary name, which javac resolves for no nested class at all, turning a reference
      // that compiled and ran into a compile error. The two arms answer different questions
      // and disagree on this shape by design.
      if (cls eq target) {
        return if (Modifier.isPublic(cls.getModifiers)) Narrowable(cls) else Unnarrowable
      }
      // The climbed target is emitted by canonical name, so here nameability is what matters:
      // a target javac cannot name from the generated unit's package must not be narrowed to.
      if (!isPubliclyNameable(target)) return Unnarrowable
      val reachable: Seq[Class[_]] = Seq(target, classOf[Object])
      // Statics are excluded: they are matched by declaring class below, and letting one
      // satisfy the signature set would accept an INSTANCE method of `cls` whose only
      // counterpart on the target is static, a call the narrowed reference would bind
      // statically to the target's method. scalac produces that pair, since it does not
      // treat a Java static as an inherited member.
      val targetSignatures = reachable.flatMap(_.getMethods)
        .filterNot(m => Modifier.isStatic(m.getModifiers)).map(erasedSignature).toSet
      val targetStaticSignatures = reachable.flatMap(_.getMethods)
        .filter(m => Modifier.isStatic(m.getModifiers)).map(erasedSignature).toSet
      val methods = cls.getMethods
      val bridges = methods.filter(m => m.isBridge && targetSignatures.contains(erasedSignature(m)))
      val nonBridgeCount = methods.iterator.filterNot(_.isBridge)
        .foldLeft(Map.empty[(String, Int), Int]) { (counts, m) =>
          val key = (m.getName, m.getParameterCount)
          counts.updated(key, counts.getOrElse(key, 0) + 1)
        }
      def coveredByBridge(m: Method): Boolean =
        nonBridgeCount.getOrElse((m.getName, m.getParameterCount), 0) <= 1 &&
          bridges.exists(b => forwardsTo(b, m))
      def declaredOnTarget(m: Member): Boolean = m.getDeclaringClass.isAssignableFrom(target)
      val targetFieldNames = Seq(target, classOf[Object]).flatMap(_.getFields).map(_.getName).toSet
      // A synthetic member is invisible to javac's source-level lookup: referencing one is
      // "cannot find symbol" even though `getMethods`/`getFields` report it, so no generated
      // reference can reach it and its loss to narrowing costs nothing. scalac emits several:
      // a lambda body in the class becomes a `public static final $anonfun$...`, and an inner
      // class carries a public `$outer` field and accessor. The exemption is withheld when the
      // target answers to the same name and kind, because the JVM ignores `ACC_SYNTHETIC` when
      // it resolves a statically-bound member. There the synthetic one shadows the target's,
      // and narrowing would change which is read.
      def exemptSynthetic(m: Method): Boolean = m.isSynthetic && {
        val shadowed =
          if (Modifier.isStatic(m.getModifiers)) targetStaticSignatures else targetSignatures
        !shadowed.contains(erasedSignature(m))
      }
      val membersLineUp = methods.forall { m =>
        if (Modifier.isStatic(m.getModifiers)) declaredOnTarget(m) || exemptSynthetic(m)
        else targetSignatures.contains(erasedSignature(m)) || coveredByBridge(m) ||
          exemptSynthetic(m)
      } && cls.getFields.forall { f =>
        declaredOnTarget(f) || (f.isSynthetic && !targetFieldNames.contains(f.getName))
      }
      if (membersLineUp) Narrowable(target) else Unnarrowable
    } catch {
      case e: LinkageError => Unknown(e)
      case NonFatal(e) => Unknown(e)
    }
  }

  private val unevaluableClassLogged = new java.util.concurrent.atomic.AtomicBoolean(false)
  // The one outcome an operator cannot diagnose from the routing logs: reflection over a
  // referenced class threw, so Spark cannot tell whether narrowing it is safe. The unit stays
  // on the configured backend with the reference spelled as a binary name javac rejects, which
  // surfaces as a compile error that looks like a codegen bug rather than a classpath one, and
  // repeats, since the compile cache does not retain failures. Called only from
  // [[narrowedSourceName]], so the once-per-JVM budget is spent on a real type reference.
  private def logUnevaluableClassOnce(cls: Class[_], e: Throwable): Unit = {
    if (unevaluableClassLogged.compareAndSet(false, true)) {
      logWarning(log"Reflection over ${MDC(LogKeys.CLASS_NAME, cls.getName)} failed, so " +
        log"Spark cannot tell whether a reference to it can be narrowed to a name the JDK " +
        log"compiler accepts; the reference keeps its binary name, which that compiler " +
        log"rejects. This usually means a partial or shaded jar on the classpath. Setting " +
        log"${MDC(LogKeys.CONFIG, SQLConf.CODEGEN_COMPILER.key)} to 'janino' compiles such " +
        log"units anyway. This notice is logged once per JVM.", e)
    }
  }

  /**
   * True when `bridge` can be the bridge the compiler emitted for `impl`: same name and
   * arity, and every bridge parameter accepts the corresponding one of `impl`. Boxing is
   * treated as equivalence so that a specialized primitive override (`apply(int)` reached
   * through an `apply(Object)` bridge) matches.
   */
  private def forwardsTo(bridge: Method, impl: Method): Boolean =
    bridge.getName == impl.getName &&
      bridge.getParameterCount == impl.getParameterCount &&
      bridge.getParameterTypes.zip(impl.getParameterTypes).forall {
        case (bridgeParam, implParam) => boxed(bridgeParam).isAssignableFrom(boxed(implParam))
      }

  /** The wrapper type for a primitive, or `cls` itself when it is already a reference type. */
  private def boxed(cls: Class[_]): Class[_] = cls match {
    case Integer.TYPE => classOf[java.lang.Integer]
    case java.lang.Long.TYPE => classOf[java.lang.Long]
    case java.lang.Double.TYPE => classOf[java.lang.Double]
    case java.lang.Float.TYPE => classOf[java.lang.Float]
    case java.lang.Short.TYPE => classOf[java.lang.Short]
    case java.lang.Byte.TYPE => classOf[java.lang.Byte]
    case Character.TYPE => classOf[java.lang.Character]
    case java.lang.Boolean.TYPE => classOf[java.lang.Boolean]
    case other => other
  }

  private def erasedSignature(m: Method): (String, Seq[Class[_]]) =
    (m.getName, m.getParameterTypes.toSeq)

  /** True iff `cls` and every class enclosing it are public, i.e. javac can name it. */
  private def isPubliclyNameable(cls: Class[_]): Boolean = {
    var c = cls
    while (c != null) {
      if (!Modifier.isPublic(c.getModifiers)) return false
      c = c.getEnclosingClass
    }
    true
  }

  /** True iff `s` contains only `[A-Za-z0-9_.]` - a dotted Java identifier path. */
  private def isPlainDottedName(s: String): Boolean = {
    var i = 0
    while (i < s.length) {
      val c = s.charAt(i)
      val ok = (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') ||
        (c >= '0' && c <= '9') || c == '_' || c == '.'
      if (!ok) return false
      i += 1
    }
    true
  }

  // `$` preceded by an identifier char and followed by an uppercase letter.
  private val InnerClassRefPattern = """(?<=[A-Za-z0-9_])\$(?=[A-Z])""".r

  // A dotted, qualified Java name (at least one `.`), used to recover the class
  // references in a generated unit when resolving classes from a non-enumerable
  // classloader (see `ClassLoaderFileManager.resolveReferencedClasses`).
  private val QualifiedNamePattern = """[A-Za-z_$][\w$]*(?:\.[A-Za-z_$][\w$]*)+""".r

  /**
   * Dedicated single-threaded executor on which the actual javac invocation runs.
   *
   * Reading jars can go through interruptible NIO file channels (both javac's
   * platform-class reads and the JDK's jar handling). Spark runs codegen on task
   * threads whose interrupt status may be set (task cancellation / cleanup); an
   * interrupt during a jar read raises `ClosedByInterruptException` (surfaced as
   * "bad class file ... unable to access file") and poisons the JDK's process-wide
   * cached zip filesystem for that jar, so later compiles fail too. Janino is immune
   * because it resolves classes via `ClassLoader.loadClass`, which reads jar bytes
   * through `ZipFile`'s native path rather than an interruptible NIO channel.
   *
   * Running every compile on this never-interrupted thread keeps those channels
   * open and valid. The single worker also confines the shared
   * [[StandardJavaFileManager]] to one thread, so no additional locking is needed.
   *
   * The single worker serializes JDK-backend compiles JVM-wide: when many sessions or
   * streaming queries trigger first-time codegen at once, their javac runs queue
   * behind one another - a throughput cliff the Janino backend (which compiles on the
   * calling threads) does not have. This is an accepted trade-off: each distinct unit
   * compiles once per JVM and is then served from the cache, and the worker must be a
   * dedicated never-interrupted thread regardless; a small pool with per-thread file
   * managers could lift the limit later if profiles ever demand it. Note that the
   * compilation-time metric measures caller-observed wall clock, so under contention
   * it includes time spent queued behind other compiles.
   */
  private lazy val compileExecutor: ExecutorService =
    ThreadUtils.newDaemonSingleThreadExecutor("jdk-code-compiler")

  override def compile(code: CodeAndComment): (GeneratedClass, ByteCodeStats) = {
    // Capture the CALLING thread's context classloader: it differs from the compile
    // worker's, and both class resolution (ClassLoaderFileManager) and loading of the
    // compiled output must see the task's classes - executor jars, and classes that
    // live only on a runtime loader such as REPL-generated or Spark Connect session
    // artifacts. `wrapAsCompilationUnit` is pure given the loader. The failure-log
    // line budget is likewise session-bound, so it too is read on the calling thread
    // (the compile worker has no session SQLConf attached).
    val resolveLoader = Utils.getContextOrSparkClassLoader
    val source = wrapAsCompilationUnit(code.body, resolveLoader)
    val failureLogMaxLines = SQLConf.get.loggingMaxLinesForCodegen

    logBasedOnLevel(SQLConf.get.codegenLogLevel) {
      log"\n${MDC(LogKeys.CODE, CodeFormatter.format(code))}"
    }

    // Wrap the parent in ParentClassLoader for the same SPARK-15622 / SPARK-11636
    // reason the Janino backend does: a ClassNotFoundException from the parent must
    // not carry a cause, or downstream resolution can fail in non-local deployments.
    val parentLoader = new ParentClassLoader(resolveLoader)

    val future = compileExecutor.submit(new Callable[(GeneratedClass, ByteCodeStats)] {
      override def call(): (GeneratedClass, ByteCodeStats) =
        doCompile(code, source, resolveLoader, parentLoader, failureLogMaxLines)
    })
    try {
      // Await uninterruptibly: the result is cached and the worker must finish its
      // jar reads without interruption (see compileExecutor). The caller's interrupt
      // status is preserved for Spark to act on after this returns. Worst case a
      // killed/speculated task holds its slot for one javac run before Spark observes
      // the kill; the work is never wasted because the result lands in the cache.
      Uninterruptibles.getUninterruptibly(future)
    } catch {
      case e: ExecutionException =>
        // Surface the root cause: a QueryExecutionErrors throwable for compile
        // failures, or a raw reflection/linkage exception when loading or
        // instantiating the compiled class fails - the same exceptions the Janino
        // path's unguarded instantiation surfaces.
        throw Option(e.getCause).getOrElse(e)
    }
  }

  /** The actual compilation; always runs on [[compileExecutor]]. */
  private def doCompile(
      code: CodeAndComment,
      source: String,
      resolveLoader: ClassLoader,
      parentLoader: ClassLoader,
      failureLogMaxLines: Int): (GeneratedClass, ByteCodeStats) = {
    val fileObject = new InMemorySourceFile(CodeCompiler.GeneratedClassName, source)
    val diagnostics = new DiagnosticCollector[JavaFileObject]()
    val fileManager = new ClassLoaderFileManager(sharedFileManager, resolveLoader, source)
    // Captures anything javac writes outside the diagnostics listener (internal
    // failures can bypass it); folded into the error message below instead of being
    // silently dropped on `System.err`.
    val compilerOut = new StringWriter()

    val task = compiler.getTask(
      /* out             = */ compilerOut,
      /* fileManager     = */ fileManager,
      /* diagnostics     = */ diagnostics,
      /* options         = */ compileOptions,
      /* classes         = */ null,
      /* compilationUnits = */ java.util.Collections.singletonList(fileObject))

    // On failure, dump the full compilation unit rather than the raw class body: the
    // line numbers in javac diagnostics refer to the wrapped unit (header + adapted
    // body), so this keeps the dump's `/* NNN */` markers aligned with `line NNN`.
    def logSourceOnFailure(): Unit = CodeCompiler.logGeneratedCodeOnFailure(
      new CodeAndComment(source, code.comment), failureLogMaxLines)

    val success = try {
      task.call().booleanValue()
    } catch {
      case NonFatal(e) =>
        logError("Failed to compile the generated Java code.", e)
        logSourceOnFailure()
        throw QueryExecutionErrors.internalCompilerError(
          new InternalCompilerException(e.getMessage, e))
    }

    if (!success) {
      val errors = diagnostics.getDiagnostics.asScala
        .filter(_.getKind == Diagnostic.Kind.ERROR)
        .map(formatDiagnostic)
        .mkString("\n")
      // javac can report failure without ERROR diagnostics (internal conditions may
      // surface through other kinds or the output writer); never raise an
      // empty-message exception.
      val message = Seq(errors, compilerOut.toString.trim).filter(_.nonEmpty) match {
        case Seq() => "the JDK compiler returned failure without diagnostics"
        case parts => parts.mkString("\n")
      }
      val ex = new CompileException(message, null)
      logError("Failed to compile the generated Java code.", ex)
      logSourceOnFailure()
      throw QueryExecutionErrors.compilerError(ex)
    }

    val classBytecodes = fileManager.snapshot()
    val codeStats = CodeCompiler.computeByteCodeStats(classBytecodes)
    val loader = new InMemoryClassLoader(classBytecodes.toMap, parentLoader)

    // `getConstructor` (public-only), matching the Janino path's instantiation.
    val generated = loader.loadClass(CodeCompiler.GeneratedClassName)
      .getConstructor()
      .newInstance()
      .asInstanceOf[GeneratedClass]

    (generated, codeStats)
  }

  private def formatDiagnostic(d: Diagnostic[_ <: JavaFileObject]): String = {
    val line = if (d.getLineNumber > 0) s"line ${d.getLineNumber}: " else ""
    s"$line${d.getMessage(Locale.ROOT)}"
  }

  // --- in-memory plumbing ---

  private class InMemorySourceFile(className: String, code: String)
    extends SimpleJavaFileObject(
      URI.create("string:///" + className.replace('.', '/') +
        JavaFileObject.Kind.SOURCE.extension),
      JavaFileObject.Kind.SOURCE) {
    override def getCharContent(ignoreEncodingErrors: Boolean): CharSequence = code
  }

  private class InMemoryClassFile(className: String)
    extends SimpleJavaFileObject(
      URI.create("bytes:///" + className.replace('.', '/') +
        JavaFileObject.Kind.CLASS.extension),
      JavaFileObject.Kind.CLASS) {
    val bytes = new ByteArrayOutputStream()
    override def openOutputStream(): java.io.OutputStream = bytes
    def toBytes: Array[Byte] = bytes.toByteArray
  }

  /**
   * Per-jar index of `package path -> class binary names`, built once per jar and
   * shared across compilations. Jars are immutable, so caching their contents is
   * safe and avoids re-scanning a jar's full entry list on every `list()` call
   * (the expensive part of classloader-based enumeration). Directories are NOT
   * cached - they may gain classes at runtime (Spark Connect session artifacts,
   * REPL output) - so those are always enumerated fresh. A jar path cannot serve
   * changed content within a JVM either: Spark refuses to overwrite an added jar,
   * and the JDK's own JarURLConnection cache (`setUseCaches(true)` below) already
   * assumes that immutability.
   *
   * The cache is size-bounded so a long-running driver (e.g. a Spark Connect server
   * whose sessions add distinct jars for years) cannot grow it without limit; the
   * bound is far above the handful of jars generated code actually references, so
   * eviction is rare and costs only a one-off re-index of that jar.
   */
  private val jarPackageIndex: Cache[String, Map[String, Seq[String]]] =
    CacheBuilder.newBuilder()
      .maximumSize(2048)
      .build[String, Map[String, Seq[String]]]()

  /**
   * A [[JavaFileManager]] that resolves the `CLASS_PATH` location through a runtime
   * [[ClassLoader]] (the task's context classloader) instead of a file-based
   * classpath, mirroring how Janino resolves referenced classes. This lets the JDK
   * compiler see exactly what Janino would - including classes that exist only on a
   * runtime loader (REPL-generated, Spark Connect session artifacts) and never on
   * `java.class.path`. Platform-class lookups (`java.*`, `jdk.*`) still go through
   * the wrapped [[StandardJavaFileManager]]; compiled output is captured in
   * [[InMemoryClassFile]] objects and never reaches it.
   */
  private class ClassLoaderFileManager(
      delegate: StandardJavaFileManager,
      classLoader: ClassLoader,
      source: String)
    extends ForwardingJavaFileManager[StandardJavaFileManager](delegate) {

    // Qualified names referenced by the generated source, used only to resolve
    // classes in packages the classloader cannot enumerate (see `list`). Computed
    // lazily, so a normal file-classpath compile (every package enumerable) never
    // pays for it. False positives from string/comment text are harmless - they
    // simply fail the class-file probe in `resolveReferencedClasses`.
    private lazy val referencedNames: Set[String] =
      QualifiedNamePattern.findAllIn(source).toSet

    private val classFiles = new mutable.HashMap[String, InMemoryClassFile]()

    override def getJavaFileForOutput(
        location: JavaFileManager.Location,
        className: String,
        kind: JavaFileObject.Kind,
        sibling: FileObject): JavaFileObject = {
      val out = new InMemoryClassFile(className)
      classFiles.put(className, out)
      out
    }

    def snapshot(): Iterable[(String, Array[Byte])] =
      classFiles.iterator.map { case (name, file) => (name, file.toBytes) }.toVector

    override def list(
        location: JavaFileManager.Location,
        packageName: String,
        kinds: java.util.Set[JavaFileObject.Kind],
        recurse: Boolean): java.lang.Iterable[JavaFileObject] = {
      if (location == StandardLocation.CLASS_PATH &&
          kinds.contains(JavaFileObject.Kind.CLASS) && packageName.nonEmpty) {
        val out = mutable.ArrayBuffer.empty[JavaFileObject]
        val seen = mutable.HashSet.empty[String]
        // First, enumerate the package via getResources (file dirs / jar entries).
        listFromClassLoader(packageName, recurse, out, seen)
        // Then add any classes the source references that enumeration missed. A
        // classloader can serve a class by name (getResourceAsStream / loadClass) yet
        // not enumerate it (getResources) - the Scala REPL and Spark Connect session
        // loaders hold generated classes only in memory. This also covers split
        // packages (e.g. `org.apache.spark.sql.connect`), where some classes are on
        // the classpath and others - test/session artifacts - are only in memory.
        // Janino resolves all of these by name; mirror that here.
        resolveReferencedClasses(packageName, out, seen)
        out.asJava
      } else if (location == StandardLocation.CLASS_PATH &&
          kinds.contains(JavaFileObject.Kind.CLASS)) {
        // Root package: never needed (the generated unit and its references live in
        // named packages), and enumerating it would scan every classpath root.
        java.util.Collections.emptyList[JavaFileObject]()
      } else {
        super.list(location, packageName, kinds, recurse)
      }
    }

    override def inferBinaryName(
        location: JavaFileManager.Location, file: JavaFileObject): String = file match {
      case f: ClassLoaderFileObject => f.binaryName
      case _ => super.inferBinaryName(location, file)
    }

    /** Enumerate the classes of `packageName` visible to the parent classloader. */
    private def listFromClassLoader(
        packageName: String,
        recurse: Boolean,
        out: mutable.ArrayBuffer[JavaFileObject],
        seen: mutable.HashSet[String]): Unit = {
      val path = packageName.replace('.', '/')
      val urls =
        try classLoader.getResources(path).asScala.toSeq
        catch { case NonFatal(_) => Seq.empty[URL] }
      urls.foreach { url =>
        try {
          url.getProtocol match {
            case "file" =>
              collectFromDir(new java.io.File(url.toURI), packageName, recurse, out, seen)
            case "jar" => collectFromJar(url, path, packageName, recurse, out, seen)
            case _ => // custom protocols cannot be enumerated; skip
          }
        } catch {
          case NonFatal(_) => () // a bad classpath entry must not fail the compile
        }
      }
    }

    /**
     * Add classes of `packageName` that the generated source references but that
     * enumeration did not surface, resolving each referenced child by name. The
     * existence probe uses `getResourceAsStream` (not `getResource`): in-memory
     * loaders such as the Scala REPL and Spark Connect session loaders override
     * `getResourceAsStream` to return bytes but have no resource URL, so `getResource`
     * would return null even though the class is loadable - exactly the path Janino
     * uses. Already-seen classes (from enumeration) are skipped, so split packages add
     * only their in-memory extras.
     *
     * When the referenced child is itself a nested class (e.g. the Scala REPL's
     * `$read$$iw$TestCaseClass`), javac needs not only that class file but its
     * enclosing classes (`$read$$iw`, `$read$`) to resolve the inner-class chain.
     * Those enclosing classes live in the same package but are not referenced by the
     * source, so each `$`-boundary prefix of the child is probed and added too.
     *
     * Only the first dot-component after the package is probed: a dotted member-class
     * reference (`pkg.Outer.Inner`) relies on `Outer$Inner` surfacing through package
     * enumeration, which holds for every enumerable loader. The known non-enumerable
     * loaders (REPL / Connect session artifacts) are deterministically routed to
     * Janino up front (see `CodeCompiler.active`), and their class references are flat
     * binary names rather than dotted member forms in any case.
     */
    private def resolveReferencedClasses(
        packageName: String,
        out: mutable.ArrayBuffer[JavaFileObject],
        seen: mutable.HashSet[String]): Unit = {
      val prefix = packageName + "."
      referencedNames.foreach { name =>
        if (name.startsWith(prefix) && name.length > prefix.length) {
          val rest = name.substring(prefix.length)
          val end = rest.indexOf('.')
          val child = if (end < 0) rest else rest.substring(0, end)
          // Probe the child and every enclosing-class prefix (cut at each non-leading
          // `$`). The immediate child can also be a sub-package rather than a class
          // (e.g. `expressions` in `o.a.s.sql.catalyst.expressions.UnsafeRow`), and
          // some loaders return a non-null stream for a package-shaped path, so the
          // class-file magic is verified before adding (avoids a phantom class that
          // would clash with the package).
          var i = 1
          while (i <= child.length) {
            if (i == child.length || child.charAt(i) == '$') {
              val binary = prefix + child.substring(0, i)
              if (seen.add(binary) && isClassFileResource(binary.replace('.', '/') + ".class")) {
                out += new ClassLoaderFileObject(binary, classLoader)
              }
            }
            i += 1
          }
        }
      }
    }

    /** True iff `resource` resolves to bytes beginning with the `0xCAFEBABE` magic. */
    private def isClassFileResource(resource: String): Boolean = {
      val stream = classLoader.getResourceAsStream(resource)
      if (stream == null) return false
      try {
        val head = stream.readNBytes(4)
        head.length == 4 &&
          (head(0) & 0xFF) == 0xCA && (head(1) & 0xFF) == 0xFE &&
          (head(2) & 0xFF) == 0xBA && (head(3) & 0xFF) == 0xBE
      } catch {
        case NonFatal(_) => false
      } finally {
        try stream.close() catch { case NonFatal(_) => () }
      }
    }

    private def collectFromDir(
        dir: java.io.File,
        pkg: String,
        recurse: Boolean,
        out: mutable.ArrayBuffer[JavaFileObject],
        seen: mutable.HashSet[String]): Unit = {
      val children = dir.listFiles()
      if (children == null) return
      children.foreach { f =>
        val name = f.getName
        if (f.isFile && name.endsWith(".class")) {
          val binary = if (pkg.isEmpty) name.dropRight(6) else s"$pkg.${name.dropRight(6)}"
          if (seen.add(binary)) out += new ClassLoaderFileObject(binary, classLoader)
        } else if (recurse && f.isDirectory) {
          collectFromDir(f, if (pkg.isEmpty) name else s"$pkg.$name", recurse, out, seen)
        }
      }
    }

    private def collectFromJar(
        url: URL,
        path: String,
        pkg: String,
        recurse: Boolean,
        out: mutable.ArrayBuffer[JavaFileObject],
        seen: mutable.HashSet[String]): Unit = {
      val conn = url.openConnection().asInstanceOf[JarURLConnection]
      conn.setUseCaches(true)
      val jarFile = conn.getJarFile
      val jarKey = jarFile.getName
      // Build (and cache) this jar's full package -> class-names index once.
      val index = jarPackageIndex.get(jarKey, () => buildJarIndex(jarFile))
      val direct = index.getOrElse(path, Seq.empty)
      val classes =
        if (!recurse) direct
        else index.iterator.collect {
          case (p, names) if p == path || p.startsWith(path + "/") => names
        }.flatten.toSeq
      classes.foreach { binary =>
        if (seen.add(binary)) out += new ClassLoaderFileObject(binary, classLoader)
      }
    }

    private def buildJarIndex(jarFile: java.util.jar.JarFile): Map[String, Seq[String]] = {
      val acc = mutable.HashMap.empty[String, mutable.ArrayBuffer[String]]
      val entries = jarFile.entries()
      while (entries.hasMoreElements) {
        val e = entries.nextElement()
        val n = e.getName
        if (!e.isDirectory && n.endsWith(".class")) {
          val slash = n.lastIndexOf('/')
          val pkgPath = if (slash < 0) "" else n.substring(0, slash)
          val binary = n.dropRight(6).replace('/', '.')
          acc.getOrElseUpdate(pkgPath, mutable.ArrayBuffer.empty) += binary
        }
      }
      acc.iterator.map { case (k, v) => (k, v.toSeq) }.toMap
    }
  }

  /**
   * A `CLASS`-kind [[JavaFileObject]] whose bytes are read from a [[ClassLoader]]
   * resource on demand, so the compiler reads exactly the bytecode the runtime
   * loader would serve for that class.
   */
  private class ClassLoaderFileObject(val binaryName: String, loader: ClassLoader)
    extends SimpleJavaFileObject(
      URI.create("classloader:///" + binaryName.replace('.', '/') +
        JavaFileObject.Kind.CLASS.extension),
      JavaFileObject.Kind.CLASS) {
    override def openInputStream(): InputStream = {
      val resource = binaryName.replace('.', '/') + ".class"
      val is = loader.getResourceAsStream(resource)
      if (is == null) throw new IOException(s"class resource not found: $resource")
      is
    }
  }

  private class InMemoryClassLoader(
      classBytes: Map[String, Array[Byte]],
      parent: ClassLoader) extends ClassLoader(parent) {
    override def findClass(name: String): Class[_] = {
      classBytes.get(name) match {
        case Some(bytes) => defineClass(name, bytes, 0, bytes.length)
        case None => throw new ClassNotFoundException(name)
      }
    }
  }
}
