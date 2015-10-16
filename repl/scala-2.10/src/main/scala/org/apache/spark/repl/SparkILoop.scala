// scalastyle:off

/* NSC -- new Scala compiler
 * Copyright 2005-2013 LAMP/EPFL
 * @author Alexander Spoon
 */

package org.apache.spark.repl


import java.net.URL

import org.apache.spark.annotation.DeveloperApi

import scala.reflect.io.AbstractFile
import scala.tools.nsc._
import scala.tools.nsc.backend.JavaPlatform
import scala.tools.nsc.interpreter._

import scala.tools.nsc.interpreter.{Results => IR}
import Predef.{println => _, _}
import java.io.{BufferedReader, FileReader}
import java.net.URI
import java.util.concurrent.locks.ReentrantLock
import scala.sys.process.Process
import scala.tools.nsc.interpreter.session._
import scala.util.Properties.{jdkHome, javaVersion}
import scala.tools.util.{Javap}
import scala.annotation.tailrec
import scala.collection.mutable.ListBuffer
import scala.concurrent.ops
import scala.tools.nsc.util._
import scala.tools.nsc.interpreter._
import scala.tools.nsc.io.{File, Directory}
import scala.reflect.NameTransformer._
import scala.tools.nsc.util.ScalaClassLoader._
import scala.tools.util._
import scala.language.{implicitConversions, existentials, postfixOps}
import scala.reflect.{ClassTag, classTag}
import scala.tools.reflect.StdRuntimeTags._

import java.lang.{Class => jClass}
import scala.reflect.api.{Mirror, TypeCreator, Universe => ApiUniverse}

import org.apache.spark.Logging
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.util.Utils

/** The Scala interactive shell.  It provides a read-eval-print loop
 *  around the Interpreter class.
 *  After instantiation, clients should call the main() method.
 *
 *  If no in0 is specified, then input will come from the console, and
 *  the class will attempt to provide input editing feature such as
 *  input history.
 *
 *  @author Moez A. Abdel-Gawad
 *  @author  Lex Spoon
 *  @version 1.2
 */
@DeveloperApi
class SparkILoop(
    private val in0: Option[BufferedReader],
    protected val out: JPrintWriter,
    val master: Option[String]
) extends AnyRef with LoopCommands with SparkILoopInit with Logging {
  def this(in0: BufferedReader, out: JPrintWriter, master: String) = this(Some(in0), out, Some(master))
  def this(in0: BufferedReader, out: JPrintWriter) = this(Some(in0), out, None)
  def this() = this(None, new JPrintWriter(Console.out, true), None)

  private var in: InteractiveReader = _   // the input stream from which commands come

  // NOTE: Exposed in package for testing
  private[repl] var settings: Settings = _

  private[repl] var intp: SparkIMain = _

  @deprecated("Use `intp` instead.", "2.9.0") def interpreter = intp
  @deprecated("Use `intp` instead.", "2.9.0") def interpreter_= (i: SparkIMain): Unit = intp = i

  /** Having inherited the difficult "var-ness" of the repl instance,
   *  I'm trying to work around it by moving operations into a class from
   *  which it will appear a stable prefix.
   */
  private def onIntp[T](f: SparkIMain => T): T = f(intp)

  class IMainOps[T <: SparkIMain](val intp: T) {
    import intp._
    import global._

    def printAfterTyper(msg: => String) =
      intp.reporter printMessage afterTyper(msg)

    /** Strip NullaryMethodType artifacts. */
    private def replInfo(sym: Symbol) = {
      sym.info match {
        case NullaryMethodType(restpe) if sym.isAccessor  => restpe
        case info                                         => info
      }
    }
    def echoTypeStructure(sym: Symbol) =
      printAfterTyper("" + deconstruct.show(replInfo(sym)))

    def echoTypeSignature(sym: Symbol, verbose: Boolean) = {
      if (verbose) SparkILoop.this.echo("// Type signature")
      printAfterTyper("" + replInfo(sym))

      if (verbose) {
        SparkILoop.this.echo("\n// Internal Type structure")
        echoTypeStructure(sym)
      }
    }
  }
  implicit def stabilizeIMain(intp: SparkIMain) = new IMainOps[intp.type](intp)

  /** TODO -
   *  -n normalize
   *  -l label with case class parameter names
   *  -c complete - leave nothing out
   */
  private def typeCommandInternal(expr: String, verbose: Boolean): Result = {
    onIntp { intp =>
      val sym = intp.symbolOfLine(expr)
      if (sym.exists) intp.echoTypeSignature(sym, verbose)
      else ""
    }
  }

  // NOTE: Must be public for visibility
  @DeveloperApi
  var sparkContext: SparkContext = _
  var sqlContext: SQLContext = _

  override def echoCommandMessage(msg: String) {
    intp.reporter printMessage msg
  }

  // def isAsync = !settings.Yreplsync.value
  private[repl] def isAsync = false
  // lazy val power = new Power(intp, new StdReplVals(this))(tagOfStdReplVals, classTag[StdReplVals])
  private def history = in.history

  /** The context class loader at the time this object was created */
  protected val originalClassLoader = Utils.getContextOrSparkClassLoader

  // classpath entries added via :cp
  private var addedClasspath: String = ""

  /** A reverse list of commands to replay if the user requests a :replay */
  private var replayCommandStack: List[String] = Nil

  /** A list of commands to replay if the user requests a :replay */
  private def replayCommands = replayCommandStack.reverse

  /** Record a command for replay should the user request a :replay */
  private def addReplay(cmd: String) = replayCommandStack ::= cmd

  private def savingReplayStack[T](body: => T): T = {
    val saved = replayCommandStack
    try body
    finally replayCommandStack = saved
  }
  private def savingReader[T](body: => T): T = {
    val saved = in
    try body
    finally in = saved
  }


  private def sparkCleanUp(){
    echo("Stopping spark context.")
    intp.beQuietDuring {
      command("sc.stop()")
    }
  }
  /** Close the interpreter and set the var to null. */
  private def closeInterpreter() {
    if (intp ne null) {
      sparkCleanUp()
      intp.close()
      intp = null
    }
  }

  class SparkILoopInterpreter extends SparkIMain(settings, out) {
    outer =>

    override private[repl] lazy val formatting = new Formatting {
      def prompt = SparkILoop.this.prompt
    }
    override protected def parentClassLoader = SparkHelper.explicitParentLoader(settings).getOrElse(classOf[SparkILoop].getClassLoader)
  }

  /**
   * Constructs a new interpreter.
   */
  protected def createInterpreter() {
    require(settings != null)

    if (addedClasspath != "") settings.classpath.append(addedClasspath)
    val addedJars =
      if (Utils.isWindows) {
        // Strip any URI scheme prefix so we can add the correct path to the classpath
        // e.g. file:/C:/my/path.jar -> C:/my/path.jar
        SparkILoop.getAddedJars.map { jar => new URI(jar).getPath.stripPrefix("/") }
      } else {
        // We need new URI(jar).getPath here for the case that `jar` includes encoded white space (%20).
        SparkILoop.getAddedJars.map { jar => new URI(jar).getPath }
      }
    // work around for Scala bug
    val totalClassPath = addedJars.foldLeft(
      settings.classpath.value)((l, r) => ClassPath.join(l, r))
    this.settings.classpath.value = totalClassPath

    intp = new SparkILoopInterpreter
  }

  /** print a friendly help message */
  private def helpCommand(line: String): Result = {
    if (line == "") helpSummary()
    else uniqueCommand(line) match {
      case Some(lc) => echo("\n" + lc.longHelp)
      case _        => ambiguousError(line)
    }
  }
  private def helpSummary() = {
    val usageWidth  = commands map (_.usageMsg.length) max
    val formatStr   = "%-" + usageWidth + "s %s %s"

    echo("All commands can be abbreviated, e.g. :he instead of :help.")
    echo("Those marked with a * have more detailed help, e.g. :help imports.\n")

    commands foreach { cmd =>
      val star = if (cmd.hasLongHelp) "*" else " "
      echo(formatStr.format(cmd.usageMsg, star, cmd.help))
    }
  }
  private def ambiguousError(cmd: String): Result = {
    matchingCommands(cmd) match {
      case Nil  => echo(cmd + ": no such command.  Type :help for help.")
      case xs   => echo(cmd + " is ambiguous: did you mean " + xs.map(":" + _.name).mkString(" or ") + "?")
    }
    Result(true, None)
  }
  private def matchingCommands(cmd: String) = commands filter (_.name startsWith cmd)
  private def uniqueCommand(cmd: String): Option[LoopCommand] = {
    // this lets us add commands willy-nilly and only requires enough command to disambiguate
    matchingCommands(cmd) match {
      case List(x)  => Some(x)
      // exact match OK even if otherwise appears ambiguous
      case xs       => xs find (_.name == cmd)
    }
  }
  private var fallbackMode = false 

  private def toggleFallbackMode() {
    val old = fallbackMode
    fallbackMode = !old
    System.setProperty("spark.repl.fallback", fallbackMode.toString)
    echo(s"""
      |Switched ${if (old) "off" else "on"} fallback mode without restarting.
      |       If you have defined classes in the repl, it would 
      |be good to redefine them incase you plan to use them. If you still run
      |into issues it would be good to restart the repl and turn on `:fallback` 
      |mode as first command.
      """.stripMargin)
  }

  /** Show the history */
  private lazy val historyCommand = new LoopCommand("history", "show the history (optional num is commands to show)") {
    override def usage = "[num]"
    def defaultLines = 20

    def apply(line: String): Result = {
      if (history eq NoHistory)
        return "No history available."

      val xs      = words(line)
      val current = history.index
      val count   = try xs.head.toInt catch { case _: Exception => defaultLines }
      val lines   = history.asStrings takeRight count
      val offset  = current - lines.size + 1

      for ((line, index) <- lines.zipWithIndex)
        echo("%3d  %s".format(index + offset, line))
    }
  }

  // When you know you are most likely breaking into the middle
  // of a line being typed.  This softens the blow.
  private[repl] def echoAndRefresh(msg: String) = {
    echo("\n" + msg)
    in.redrawLine()
  }
  private[repl] def echo(msg: String) = {
    out println msg
    out.flush()
  }
  private def echoNoNL(msg: String) = {
    out print msg
    out.flush()
  }

  /** Search the history */
  private def searchHistory(_cmdline: String) {
    val cmdline = _cmdline.toLowerCase
    val offset  = history.index - history.size + 1

    for ((line, index) <- history.asStrings.zipWithIndex ; if line.toLowerCase contains cmdline)
      echo("%d %s".format(index + offset, line))
  }

  private var currentPrompt = Properties.shellPromptString

  /**
   * Sets the prompt string used by the REPL.
   *
   * @param prompt The new prompt string
   */
  @DeveloperApi
  def setPrompt(prompt: String) = currentPrompt = prompt

  /**
   * Represents the current prompt string used by the REPL.
   *
   * @return The current prompt string
   */
  @DeveloperApi
  def prompt = currentPrompt

  import LoopCommand.{ cmd, nullary }

  /** Standard commands */
  private lazy val standardCommands = List(
    cmd("cp", "<path>", "add a jar or directory to the classpath", addClasspath),
    cmd("help", "[command]", "print this summary or command-specific help", helpCommand),
    historyCommand,
    cmd("h?", "<string>", "search the history", searchHistory),
    cmd("imports", "[name name ...]", "show import history, identifying sources of names", importsCommand),
    cmd("implicits", "[-v]", "show the implicits in scope", implicitsCommand),
    cmd("javap", "<path|class>", "disassemble a file or class name", javapCommand),
    cmd("load", "<path>", "load and interpret a Scala file", loadCommand),
    nullary("paste", "enter paste mode: all input up to ctrl-D compiled together", pasteCommand),
//    nullary("power", "enable power user mode", powerCmd),
    nullary("quit", "exit the repl", () => Result(false, None)),
    nullary("replay", "reset execution and replay all previous commands", replay),
    nullary("reset", "reset the repl to its initial state, forgetting all session entries", resetCommand),
    shCommand,
    nullary("silent", "disable/enable automatic printing of results", verbosity),
    nullary("fallback", """
                           |disable/enable advanced repl changes, these fix some issues but may introduce others. 
                           |This mode will be removed once these fixes stablize""".stripMargin, toggleFallbackMode),
    cmd("type", "[-v] <expr>", "display the type of an expression without evaluating it", typeCommand),
    nullary("warnings", "show the suppressed warnings from the most recent line which had any", warningsCommand)
  )

  /** Power user commands */
  private lazy val powerCommands: List[LoopCommand] = List(
    // cmd("phase", "<phase>", "set the implicit phase for power commands", phaseCommand)
  )

  // private def dumpCommand(): Result = {
  //   echo("" + power)
  //   history.asStrings takeRight 30 foreach echo
  //   in.redrawLine()
  // }
  // private def valsCommand(): Result = power.valsDescription

  private val typeTransforms = List(
    "scala.collection.immutable." -> "immutable.",
    "scala.collection.mutable."   -> "mutable.",
    "scala.collection.generic."   -> "generic.",
    "java.lang."                  -> "jl.",
    "scala.runtime."              -> "runtime."
  )

  private def importsCommand(line: String): Result = {
    val tokens    = words(line)
    val handlers  = intp.languageWildcardHandlers ++ intp.importHandlers
    val isVerbose = tokens contains "-v"

    handlers.filterNot(_.importedSymbols.isEmpty).zipWithIndex foreach {
      case (handler, idx) =>
        val (types, terms) = handler.importedSymbols partition (_.name.isTypeName)
        val imps           = handler.implicitSymbols
        val found          = tokens filter (handler importsSymbolNamed _)
        val typeMsg        = if (types.isEmpty) "" else types.size + " types"
        val termMsg        = if (terms.isEmpty) "" else terms.size + " terms"
        val implicitMsg    = if (imps.isEmpty) "" else imps.size + " are implicit"
        val foundMsg       = if (found.isEmpty) "" else found.mkString(" // imports: ", ", ", "")
        val statsMsg       = List(typeMsg, termMsg, implicitMsg) filterNot (_ == "") mkString ("(", ", ", ")")

        intp.reporter.printMessage("%2d) %-30s %s%s".format(
          idx + 1,
          handler.importString,
          statsMsg,
          foundMsg
        ))
    }
  }

  private def implicitsCommand(line: String): Result = onIntp { intp =>
    import intp._
    import global._

    def p(x: Any) = intp.reporter.printMessage("" + x)

    // If an argument is given, only show a source with that
    // in its name somewhere.
    val args     = line split "\\s+"
    val filtered = intp.implicitSymbolsBySource filter {
      case (source, syms) =>
        (args contains "-v") || {
          if (line == "") (source.fullName.toString != "scala.Predef")
          else (args exists (source.name.toString contains _))
        }
    }

    if (filtered.isEmpty)
      return "No implicits have been imported other than those in Predef."

    filtered foreach {
      case (source, syms) =>
        p("/* " + syms.size + " implicit members imported from " + source.fullName + " */")

        // This groups the members by where the symbol is defined
        val byOwner = syms groupBy (_.owner)
        val sortedOwners = byOwner.toList sortBy { case (owner, _) => afterTyper(source.info.baseClasses indexOf owner) }

        sortedOwners foreach {
          case (owner, members) =>
            // Within each owner, we cluster results based on the final result type
            // if there are more than a couple, and sort each cluster based on name.
            // This is really just trying to make the 100 or so implicits imported
            // by default into something readable.
            val memberGroups: List[List[Symbol]] = {
              val groups = members groupBy (_.tpe.finalResultType) toList
              val (big, small) = groups partition (_._2.size > 3)
              val xss = (
                (big sortBy (_._1.toString) map (_._2)) :+
                (small flatMap (_._2))
              )

              xss map (xs => xs sortBy (_.name.toString))
            }

            val ownerMessage = if (owner == source) " defined in " else " inherited from "
            p("  /* " + members.size + ownerMessage + owner.fullName + " */")

            memberGroups foreach { group =>
              group foreach (s => p("  " + intp.symbolDefString(s)))
              p("")
            }
        }
        p("")
    }
  }

  private def findToolsJar() = {
    val jdkPath = Directory(jdkHome)
    val jar     = jdkPath / "lib" / "tools.jar" toFile;

    if (jar isFile)
      Some(jar)
    else if (jdkPath.isDirectory)
      jdkPath.deepFiles find (_.name == "tools.jar")
    else None
  }
  private def addToolsJarToLoader() = {
    val cl = findToolsJar match {
      case Some(tools) => ScalaClassLoader.fromURLs(Seq(tools.toURL), intp.classLoader)
      case _           => intp.classLoader
    }
    if (Javap.isAvailable(cl)) {
      logDebug(":javap available.")
      cl
    }
    else {
      logDebug(":javap unavailable: no tools.jar at " + jdkHome)
      intp.classLoader
    }
  }

  private def newJavap() = new JavapClass(addToolsJarToLoader(), new SparkIMain.ReplStrippingWriter(intp)) {
    override def tryClass(path: String): Array[Byte] = {
      val hd :: rest = path split '.' toList;
      // If there are dots in the name, the first segment is the
      // key to finding it.
      if (rest.nonEmpty) {
        intp optFlatName hd match {
          case Some(flat) =>
            val clazz = flat :: rest mkString NAME_JOIN_STRING
            val bytes = super.tryClass(clazz)
            if (bytes.nonEmpty) bytes
            else super.tryClass(clazz + MODULE_SUFFIX_STRING)
          case _          => super.tryClass(path)
        }
      }
      else {
        // Look for Foo first, then Foo$, but if Foo$ is given explicitly,
        // we have to drop the $ to find object Foo, then tack it back onto
        // the end of the flattened name.
        def className  = intp flatName path
        def moduleName = (intp flatName path.stripSuffix(MODULE_SUFFIX_STRING)) + MODULE_SUFFIX_STRING

        val bytes = super.tryClass(className)
        if (bytes.nonEmpty) bytes
        else super.tryClass(moduleName)
      }
    }
  }
  // private lazy val javap = substituteAndLog[Javap]("javap", NoJavap)(newJavap())
  private lazy val javap =
    try newJavap()
    catch { case _: Exception => null }

  // Still todo: modules.
  private def typeCommand(line0: String): Result = {
    line0.trim match {
      case ""                      => ":type [-v] <expression>"
      case s if s startsWith "-v " => typeCommandInternal(s stripPrefix "-v " trim, true)
      case s                       => typeCommandInternal(s, false)
    }
  }

  private def warningsCommand(): Result = {
    if (intp.lastWarnings.isEmpty)
      "Can't find any cached warnings."
    else
      intp.lastWarnings foreach { case (pos, msg) => intp.reporter.warning(pos, msg) }
  }

  private def javapCommand(line: String): Result = {
    if (javap == null)
      ":javap unavailable, no tools.jar at %s.  Set JDK_HOME.".format(jdkHome)
    else if (javaVersion startsWith "1.7")
      ":javap not yet working with java 1.7"
    else if (line == "")
      ":javap [-lcsvp] [path1 path2 ...]"
    else
      javap(words(line)) foreach { res =>
        if (res.isError) return "Failed: " + res.value
        else res.show()
      }
  }

  private def wrapCommand(line: String): Result = {
    def failMsg = "Argument to :wrap must be the name of a method with signature [T](=> T): T"
    onIntp { intp =>
      import intp._
      import global._

      words(line) match {
        case Nil            =>
          intp.executionWrapper match {
            case ""   => "No execution wrapper is set."
            case s    => "Current execution wrapper: " + s
          }
        case "clear" :: Nil =>
          intp.executionWrapper match {
            case ""   => "No execution wrapper is set."
            case s    => intp.clearExecutionWrapper() ; "Cleared execution wrapper."
          }
        case wrapper :: Nil =>
          intp.typeOfExpression(wrapper) match {
            case PolyType(List(targ), MethodType(List(arg), restpe)) =>
              intp setExecutionWrapper intp.pathToTerm(wrapper)
              "Set wrapper to '" + wrapper + "'"
            case tp =>
              failMsg + "\nFound: <unknown>"
          }
        case _ => failMsg
      }
    }
  }

  private def pathToPhaseWrapper = intp.pathToTerm("$r") + ".phased.atCurrent"
  // private def phaseCommand(name: String): Result = {
  //   val phased: Phased = power.phased
  //   import phased.NoPhaseName

  //   if (name == "clear") {
  //     phased.set(NoPhaseName)
  //     intp.clearExecutionWrapper()
  //     "Cleared active phase."
  //   }
  //   else if (name == "") phased.get match {
  //     case NoPhaseName => "Usage: :phase <expr> (e.g. typer, erasure.next, erasure+3)"
  //     case ph          => "Active phase is '%s'.  (To clear, :phase clear)".format(phased.get)
  //   }
  //   else {
  //     val what = phased.parse(name)
  //     if (what.isEmpty || !phased.set(what))
  //       "'" + name + "' does not appear to represent a valid phase."
  //     else {
  //       intp.setExecutionWrapper(pathToPhaseWrapper)
  //       val activeMessage =
  //         if (what.toString.length == name.length) "" + what
  //         else "%s (%s)".format(what, name)

  //       "Active phase is now: " + activeMessage
  //     }
  //   }
  // }

  /**
   * Provides a list of available commands.
   *
   * @return The list of commands
   */
  @DeveloperApi
  def commands: List[LoopCommand] = standardCommands /*++ (
    if (isReplPower) powerCommands else Nil
  )*/

  private val replayQuestionMessage =
    """|That entry seems to have slain the compiler.  Shall I replay
       |your session? I can re-run each line except the last one.
       |[y/n]
    """.trim.stripMargin

  private def crashRecovery(ex: Throwable): Boolean = {
    echo(ex.toString)
    ex match {
      case _: NoSuchMethodError | _: NoClassDefFoundError =>
        echo("\nUnrecoverable error.")
        throw ex
      case _  =>
        def fn(): Boolean =
          try in.readYesOrNo(replayQuestionMessage, { echo("\nYou must enter y or n.") ; fn() })
          catch { case _: RuntimeException => false }

        if (fn()) replay()
        else echo("\nAbandoning crashed session.")
    }
    true
  }

  /** The main read-eval-print loop for the repl.  It calls
   *  command() for each line of input, and stops when
   *  command() returns false.
   */
  private def loop() {
    def readOneLine() = {
      out.flush()
      in readLine prompt
    }
    // return false if repl should exit
    def processLine(line: String): Boolean = {
      if (isAsync) {
        if (!awaitInitialized()) return false
        runThunks()
      }
      if (line eq null) false               // assume null means EOF
      else command(line) match {
        case Result(false, _)           => false
        case Result(_, Some(finalLine)) => addReplay(finalLine) ; true
        case _                          => true
      }
    }
    def innerLoop() {
      val shouldContinue = try {
        processLine(readOneLine())
      } catch {case t: Throwable => crashRecovery(t)}
      if (shouldContinue)
        innerLoop()
    }
    innerLoop()
  }

  /** interpret all lines from a specified file */
  private def interpretAllFrom(file: File) {
    savingReader {
      savingReplayStack {
        file applyReader { reader =>
          in = SimpleReader(reader, out, false)
          echo("Loading " + file + "...")
          loop()
        }
      }
    }
  }

  /** create a new interpreter and replay the given commands */
  private def replay() {
    reset()
    if (replayCommandStack.isEmpty)
      echo("Nothing to replay.")
    else for (cmd <- replayCommands) {
      echo("Replaying: " + cmd)  // flush because maybe cmd will have its own output
      command(cmd)
      echo("")
    }
  }
  private def resetCommand() {
    echo("Resetting repl state.")
    if (replayCommandStack.nonEmpty) {
      echo("Forgetting this session history:\n")
      replayCommands foreach echo
      echo("")
      replayCommandStack = Nil
    }
    if (intp.namedDefinedTerms.nonEmpty)
      echo("Forgetting all expression results and named terms: " + intp.namedDefinedTerms.mkString(", "))
    if (intp.definedTypes.nonEmpty)
      echo("Forgetting defined types: " + intp.definedTypes.mkString(", "))

    reset()
  }

  private def reset() {
    intp.reset()
    // unleashAndSetPhase()
  }

  /** fork a shell and run a command */
  private lazy val shCommand = new LoopCommand("sh", "run a shell command (result is implicitly => List[String])") {
    override def usage = "<command line>"
    def apply(line: String): Result = line match {
      case ""   => showUsage()
      case _    =>
        val toRun = classOf[ProcessResult].getName + "(" + string2codeQuoted(line) + ")"
        intp interpret toRun
        ()
    }
  }

  private def withFile(filename: String)(action: File => Unit) {
    val f = File(filename)

    if (f.exists) action(f)
    else echo("That file does not exist")
  }

  private def loadCommand(arg: String) = {
    var shouldReplay: Option[String] = None
    withFile(arg)(f => {
      interpretAllFrom(f)
      shouldReplay = Some(":load " + arg)
    })
    Result(true, shouldReplay)
  }

  private def addAllClasspath(args: Seq[String]): Unit = {
    var added = false
    var totalClasspath = ""
    for (arg <- args) {
      val f = File(arg).normalize
      if (f.exists) {
        added = true
        addedClasspath = ClassPath.join(addedClasspath, f.path)
        totalClasspath = ClassPath.join(settings.classpath.value, addedClasspath)
        intp.addUrlsToClassPath(f.toURI.toURL)
        sparkContext.addJar(f.toURI.toURL.getPath)
      }
    }
  }

  private def addClasspath(arg: String): Unit = {
    val f = File(arg).normalize
    if (f.exists) {
      addedClasspath = ClassPath.join(addedClasspath, f.path)
      intp.addUrlsToClassPath(f.toURI.toURL)
      sparkContext.addJar(f.toURI.toURL.getPath)
      echo("Added '%s'.  Your new classpath is:\n\"%s\"".format(f.path, intp.global.classPath.asClasspathString))
    }
    else echo("The path '" + f + "' doesn't seem to exist.")
  }


  private def powerCmd(): Result = {
    if (isReplPower) "Already in power mode."
    else enablePowerMode(false)
  }

  private[repl] def enablePowerMode(isDuringInit: Boolean) = {
    // replProps.power setValue true
    // unleashAndSetPhase()
    // asyncEcho(isDuringInit, power.banner)
  }
  // private def unleashAndSetPhase() {
//     if (isReplPower) {
// //      power.unleash()
//       // Set the phase to "typer"
//       intp beSilentDuring phaseCommand("typer")
//     }
//   }

  private def asyncEcho(async: Boolean, msg: => String) {
    if (async) asyncMessage(msg)
    else echo(msg)
  }

  private def verbosity() = {
    // val old = intp.printResults
    // intp.printResults = !old
    // echo("Switched " + (if (old) "off" else "on") + " result printing.")
  }

  /** Run one command submitted by the user.  Two values are returned:
    * (1) whether to keep running, (2) the line to record for replay,
    * if any. */
  private[repl] def command(line: String): Result = {
    if (line startsWith ":") {
      val cmd = line.tail takeWhile (x => !x.isWhitespace)
      uniqueCommand(cmd) match {
        case Some(lc) => lc(line.tail stripPrefix cmd dropWhile (_.isWhitespace))
        case _        => ambiguousError(cmd)
      }
    }
    else if (intp.global == null) Result(false, None)  // Notice failure to create compiler
    else Result(true, interpretStartingWith(line))
  }

  private def readWhile(cond: String => Boolean) = {
    Iterator continually in.readLine("") takeWhile (x => x != null && cond(x))
  }

  private def pasteCommand(): Result = {
    echo("// Entering paste mode (ctrl-D to finish)\n")
    val code = readWhile(_ => true) mkString "\n"
    echo("\n// Exiting paste mode, now interpreting.\n")
    intp interpret code
    ()
  }

  private object paste extends Pasted {
    val ContinueString = "     | "
    val PromptString   = "scala> "

    def interpret(line: String): Unit = {
      echo(line.trim)
      intp interpret line
      echo("")
    }

    def transcript(start: String) = {
      echo("\n// Detected repl transcript paste: ctrl-D to finish.\n")
      apply(Iterator(start) ++ readWhile(_.trim != PromptString.trim))
    }
  }
  import paste.{ ContinueString, PromptString }

  /** Interpret expressions starting with the first line.
    * Read lines until a complete compilation unit is available
    * or until a syntax error has been seen.  If a full unit is
    * read, go ahead and interpret it.  Return the full string
    * to be recorded for replay, if any.
    */
  private def interpretStartingWith(code: String): Option[String] = {
    // signal completion non-completion input has been received
    in.completion.resetVerbosity()

    def reallyInterpret = {
      val reallyResult = intp.interpret(code)
      (reallyResult, reallyResult match {
        case IR.Error       => None
        case IR.Success     => Some(code)
        case IR.Incomplete  =>
          if (in.interactive && code.endsWith("\n\n")) {
            echo("You typed two blank lines.  Starting a new command.")
            None
          }
          else in.readLine(ContinueString) match {
            case null =>
              // we know compilation is going to fail since we're at EOF and the
              // parser thinks the input is still incomplete, but since this is
              // a file being read non-interactively we want to fail.  So we send
              // it straight to the compiler for the nice error message.
              intp.compileString(code)
              None

            case line => interpretStartingWith(code + "\n" + line)
          }
      })
    }

    /** Here we place ourselves between the user and the interpreter and examine
     *  the input they are ostensibly submitting.  We intervene in several cases:
     *
     *  1) If the line starts with "scala> " it is assumed to be an interpreter paste.
     *  2) If the line starts with "." (but not ".." or "./") it is treated as an invocation
     *     on the previous result.
     *  3) If the Completion object's execute returns Some(_), we inject that value
     *     and avoid the interpreter, as it's likely not valid scala code.
     */
    if (code == "") None
    else if (!paste.running && code.trim.startsWith(PromptString)) {
      paste.transcript(code)
      None
    }
    else if (Completion.looksLikeInvocation(code) && intp.mostRecentVar != "") {
      interpretStartingWith(intp.mostRecentVar + code)
    }
    else if (code.trim startsWith "//") {
      // line comment, do nothing
      None
    }
    else
      reallyInterpret._2
  }

  // runs :load `file` on any files passed via -i
  private def loadFiles(settings: Settings) = settings match {
    case settings: SparkRunnerSettings =>
      for (filename <- settings.loadfiles.value) {
        val cmd = ":load " + filename
        command(cmd)
        addReplay(cmd)
        echo("")
      }
    case _ =>
  }

  /** Tries to create a JLineReader, falling back to SimpleReader:
   *  unless settings or properties are such that it should start
   *  with SimpleReader.
   */
  private def chooseReader(settings: Settings): InteractiveReader = {
    if (settings.Xnojline.value || Properties.isEmacsShell)
      SimpleReader()
    else try new SparkJLineReader(
      if (settings.noCompletion.value) NoCompletion
      else new SparkJLineCompletion(intp)
    )
    catch {
      case ex @ (_: Exception | _: NoClassDefFoundError) =>
        echo("Failed to created SparkJLineReader: " + ex + "\nFalling back to SimpleReader.")
        SimpleReader()
    }
  }

  private val u: scala.reflect.runtime.universe.type = scala.reflect.runtime.universe
  private val m = u.runtimeMirror(Utils.getSparkClassLoader)
  private def tagOfStaticClass[T: ClassTag]: u.TypeTag[T] =
    u.TypeTag[T](
      m,
      new TypeCreator {
        def apply[U <: ApiUniverse with Singleton](m: Mirror[U]): U # Type =
          m.staticClass(classTag[T].runtimeClass.getName).toTypeConstructor.asInstanceOf[U # Type]
      })

  private def process(settings: Settings): Boolean = savingContextLoader {
    if (getMaster() == "yarn-client") System.setProperty("SPARK_YARN_MODE", "true")

    this.settings = settings
    createInterpreter()

    // sets in to some kind of reader depending on environmental cues
    in = in0 match {
      case Some(reader) => SimpleReader(reader, out, true)
      case None         =>
        // some post-initialization
        chooseReader(settings) match {
          case x: SparkJLineReader => addThunk(x.consoleReader.postInit) ; x
          case x                   => x
        }
    }
    lazy val tagOfSparkIMain = tagOfStaticClass[org.apache.spark.repl.SparkIMain]
    // Bind intp somewhere out of the regular namespace where
    // we can get at it in generated code.
    addThunk(intp.quietBind(NamedParam[SparkIMain]("$intp", intp)(tagOfSparkIMain, classTag[SparkIMain])))
    addThunk({
      import scala.tools.nsc.io._
      import Properties.userHome
      import scala.compat.Platform.EOL
      val autorun = replProps.replAutorunCode.option flatMap (f => io.File(f).safeSlurp())
      if (autorun.isDefined) intp.quietRun(autorun.get)
    })

    addThunk(printWelcome())
    addThunk(initializeSpark())

    // it is broken on startup; go ahead and exit
    if (intp.reporter.hasErrors)
      return false

    // This is about the illusion of snappiness.  We call initialize()
    // which spins off a separate thread, then print the prompt and try
    // our best to look ready.  The interlocking lazy vals tend to
    // inter-deadlock, so we break the cycle with a single asynchronous
    // message to an rpcEndpoint.
    if (isAsync) {
      intp initialize initializedCallback()
      createAsyncListener() // listens for signal to run postInitialization
    }
    else {
      intp.initializeSynchronous()
      postInitialization()
    }
    // printWelcome()

    loadFiles(settings)

    try loop()
    catch AbstractOrMissingHandler()
    finally closeInterpreter()

    true
  }

  // NOTE: Must be public for visibility
  @DeveloperApi
  def createSparkContext(): SparkContext = {
    val execUri = System.getenv("SPARK_EXECUTOR_URI")
    val jars = SparkILoop.getAddedJars
    val conf = new SparkConf()
      .setMaster(getMaster())
      .setJars(jars)
      .set("spark.repl.class.uri", intp.classServerUri)
      .setIfMissing("spark.app.name", "Spark shell")
    if (execUri != null) {
      conf.set("spark.executor.uri", execUri)
    }
    sparkContext = new SparkContext(conf)
    logInfo("Created spark context..")
    sparkContext
  }

  @DeveloperApi
  def createSQLContext(): SQLContext = {
    val name = "org.apache.spark.sql.hive.HiveContext"
    val loader = Utils.getContextOrSparkClassLoader
    try {
      sqlContext = loader.loadClass(name).getConstructor(classOf[SparkContext])
        .newInstance(sparkContext).asInstanceOf[SQLContext] 
      logInfo("Created sql context (with Hive support)..")
    }
    catch {
      case _: java.lang.ClassNotFoundException | _: java.lang.NoClassDefFoundError =>
        sqlContext = new SQLContext(sparkContext)
        logInfo("Created sql context..")
    }
    sqlContext
  }

  private def getMaster(): String = {
    val master = this.master match {
      case Some(m) => m
      case None =>
        val envMaster = sys.env.get("MASTER")
        val propMaster = sys.props.get("spark.master")
        propMaster.orElse(envMaster).getOrElse("local[*]")
    }
    master
  }

  /** process command-line arguments and do as they request */
  def process(args: Array[String]): Boolean = {
    val command = new SparkCommandLine(args.toList, msg => echo(msg))
    def neededHelp(): String =
      (if (command.settings.help.value) command.usageMsg + "\n" else "") +
      (if (command.settings.Xhelp.value) command.xusageMsg + "\n" else "")

    // if they asked for no help and command is valid, we call the real main
    neededHelp() match {
      case ""     => command.ok && process(command.settings)
      case help   => echoNoNL(help) ; true
    }
  }

  @deprecated("Use `process` instead", "2.9.0")
  private def main(settings: Settings): Unit = process(settings)
}

object SparkILoop extends Logging {
  implicit def loopToInterpreter(repl: SparkILoop): SparkIMain = repl.intp
  private def echo(msg: String) = Console println msg

  def getAddedJars: Array[String] = {
    val envJars = sys.env.get("ADD_JARS")
    if (envJars.isDefined) {
      logWarning("ADD_JARS environment variable is deprecated, use --jar spark submit argument instead")
    }
    val propJars = sys.props.get("spark.jars").flatMap { p => if (p == "") None else Some(p) }
    val jars = propJars.orElse(envJars).getOrElse("")
    Utils.resolveURIs(jars).split(",").filter(_.nonEmpty)
  }

  // Designed primarily for use by test code: take a String with a
  // bunch of code, and prints out a transcript of what it would look
  // like if you'd just typed it into the repl.
  private[repl] def runForTranscript(code: String, settings: Settings): String = {
    import java.io.{ BufferedReader, StringReader, OutputStreamWriter }

    stringFromStream { ostream =>
      Console.withOut(ostream) {
        val output = new JPrintWriter(new OutputStreamWriter(ostream), true) {
          override def write(str: String) = {
            // completely skip continuation lines
            if (str forall (ch => ch.isWhitespace || ch == '|')) ()
            // print a newline on empty scala prompts
            else if ((str contains '\n') && (str.trim == "scala> ")) super.write("\n")
            else super.write(str)
          }
        }
        val input = new BufferedReader(new StringReader(code)) {
          override def readLine(): String = {
            val s = super.readLine()
            // helping out by printing the line being interpreted.
            if (s != null)
              // scalastyle:off println
              output.println(s)
              // scalastyle:on println
            s
          }
        }
        val repl = new SparkILoop(input, output)

        if (settings.classpath.isDefault)
          settings.classpath.value = sys.props("java.class.path")

        getAddedJars.map(jar => new URI(jar).getPath).foreach(settings.classpath.append(_))

        repl process settings
      }
    }
  }

  /** Creates an interpreter loop with default settings and feeds
   *  the given code to it as input.
   */
  private[repl] def run(code: String, sets: Settings = new Settings): String = {
    import java.io.{ BufferedReader, StringReader, OutputStreamWriter }

    stringFromStream { ostream =>
      Console.withOut(ostream) {
        val input    = new BufferedReader(new StringReader(code))
        val output   = new JPrintWriter(new OutputStreamWriter(ostream), true)
        val repl     = new ILoop(input, output)

        if (sets.classpath.isDefault)
          sets.classpath.value = sys.props("java.class.path")

        repl process sets
      }
    }
  }
  private[repl] def run(lines: List[String]): String = run(lines map (_ + "\n") mkString)
}
