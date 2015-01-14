// scalastyle:off

/* NSC -- new Scala compiler
 * Copyright 2005-2013 LAMP/EPFL
 * @author Paul Phillips
 */

package org.apache.spark.repl

import scala.tools.nsc._
import scala.tools.nsc.interpreter._

import scala.reflect.internal.util.Position
import scala.util.control.Exception.ignoring
import scala.tools.nsc.util.stackTraceString

import org.apache.spark.SPARK_VERSION

/**
 *  Machinery for the asynchronous initialization of the repl.
 */
trait SparkILoopInit {
  self: SparkILoop =>

  /** Print a welcome message */
  def printWelcome() {
    echo("""Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /___/ .__/\_,_/_/ /_/\_\   version %s
      /_/
""".format(SPARK_VERSION))
    import Properties._
    val welcomeMsg = "Using Scala %s (%s, Java %s)".format(
      versionString, javaVmName, javaVersion)
    echo(welcomeMsg)
    echo("Type in expressions to have them evaluated.")
    echo("Type :help for more information.")
   }

  protected def asyncMessage(msg: String) {
    if (isReplInfo || isReplPower)
      echoAndRefresh(msg)
  }

  private val initLock = new java.util.concurrent.locks.ReentrantLock()
  private val initCompilerCondition = initLock.newCondition() // signal the compiler is initialized
  private val initLoopCondition = initLock.newCondition()     // signal the whole repl is initialized
  private val initStart = System.nanoTime

  private def withLock[T](body: => T): T = {
    initLock.lock()
    try body
    finally initLock.unlock()
  }
  // a condition used to ensure serial access to the compiler.
  @volatile private var initIsComplete = false
  @volatile private var initError: String = null
  private def elapsed() = "%.3f".format((System.nanoTime - initStart).toDouble / 1000000000L)

  // the method to be called when the interpreter is initialized.
  // Very important this method does nothing synchronous (i.e. do
  // not try to use the interpreter) because until it returns, the
  // repl's lazy val `global` is still locked.
  protected def initializedCallback() = withLock(initCompilerCondition.signal())

  // Spins off a thread which awaits a single message once the interpreter
  // has been initialized.
  protected def createAsyncListener() = {
    io.spawn {
      withLock(initCompilerCondition.await())
      asyncMessage("[info] compiler init time: " + elapsed() + " s.")
      postInitialization()
    }
  }

  // called from main repl loop
  protected def awaitInitialized(): Boolean = {
    if (!initIsComplete)
      withLock { while (!initIsComplete) initLoopCondition.await() }
    if (initError != null) {
      println("""
        |Failed to initialize the REPL due to an unexpected error.
        |This is a bug, please, report it along with the error diagnostics printed below.
        |%s.""".stripMargin.format(initError)
      )
      false
    } else true
  }
  // private def warningsThunks = List(
  //   () => intp.bind("lastWarnings", "" + typeTag[List[(Position, String)]], intp.lastWarnings _),
  // )

  protected def postInitThunks = List[Option[() => Unit]](
    Some(intp.setContextClassLoader _),
    if (isReplPower) Some(() => enablePowerMode(true)) else None
  ).flatten
  // ++ (
  //   warningsThunks
  // )
  // called once after init condition is signalled
  protected def postInitialization() {
    try {
      postInitThunks foreach (f => addThunk(f()))
      runThunks()
    } catch {
      case ex: Throwable =>
        initError = stackTraceString(ex)
        throw ex
    } finally {
      initIsComplete = true

      if (isAsync) {
        asyncMessage("[info] total init time: " + elapsed() + " s.")
        withLock(initLoopCondition.signal())
      }
    }
  }

  def initializeSpark() {
    intp.beQuietDuring {
      command("""
         @transient val sc = {
           val _sc = org.apache.spark.repl.Main.interp.createSparkContext()
           println("Spark context available as sc.")
           _sc
         }
        """)
      command("import org.apache.spark.SparkContext._")
    }
  }

  // code to be executed only after the interpreter is initialized
  // and the lazy val `global` can be accessed without risk of deadlock.
  private var pendingThunks: List[() => Unit] = Nil
  protected def addThunk(body: => Unit) = synchronized {
    pendingThunks :+= (() => body)
  }
  protected def runThunks(): Unit = synchronized {
    if (pendingThunks.nonEmpty)
      logDebug("Clearing " + pendingThunks.size + " thunks.")

    while (pendingThunks.nonEmpty) {
      val thunk = pendingThunks.head
      pendingThunks = pendingThunks.tail
      thunk()
    }
  }
}
