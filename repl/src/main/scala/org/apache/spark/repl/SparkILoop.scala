/* NSC -- new Scala compiler
 * Copyright 2005-2011 LAMP/EPFL
 * @author Alexander Spoon
 */

package org.apache.spark.repl

import scala.tools.nsc._
import scala.tools.nsc.interpreter._

import Predef.{ println => _, _ }
import java.io.{ BufferedReader, FileReader, PrintWriter }
import scala.sys.process.Process
import session._
import scala.tools.nsc.interpreter.{ Results => IR }
import scala.tools.util.{ SignalManager, Signallable, Javap }
import scala.annotation.tailrec
import scala.util.control.Exception.{ ignoring }
import scala.collection.mutable.ListBuffer
import scala.concurrent.ops
import util.{ ClassPath, Exceptional, stringFromWriter, stringFromStream }
import interpreter._
import io.{ File, Sources }

import org.apache.spark.Logging
import org.apache.spark.SparkContext

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
class SparkILoop(in0: Option[BufferedReader], val out: PrintWriter, val master: Option[String])
                extends AnyRef
                   with LoopCommands
                   with Logging
{
  def this(in0: BufferedReader, out: PrintWriter, master: String) = this(Some(in0), out, Some(master))
  def this(in0: BufferedReader, out: PrintWriter) = this(Some(in0), out, None)
  def this() = this(None, new PrintWriter(Console.out, true), None)
  
  var in: InteractiveReader = _   // the input stream from which commands come
  var settings: Settings = _
  var intp: SparkIMain = _

  /*
  lazy val power = {
    val g = intp.global
    Power[g.type](this, g)
  }
  */
  
  // TODO
  // object opt extends AestheticSettings
  // 
  @deprecated("Use `intp` instead.", "2.9.0")
  def interpreter = intp
  
  @deprecated("Use `intp` instead.", "2.9.0")
  def interpreter_= (i: SparkIMain): Unit = intp = i
  
  def history = in.history

  /** The context class loader at the time this object was created */
  protected val originalClassLoader = Thread.currentThread.getContextClassLoader

  // Install a signal handler so we can be prodded.
  private val signallable =
    /*if (isReplDebug) Signallable("Dump repl state.")(dumpCommand())
    else*/ null
    
  // classpath entries added via :cp
  var addedClasspath: String = ""

  /** A reverse list of commands to replay if the user requests a :replay */
  var replayCommandStack: List[String] = Nil

  /** A list of commands to replay if the user requests a :replay */
  def replayCommands = replayCommandStack.reverse

  /** Record a command for replay should the user request a :replay */
  def addReplay(cmd: String) = replayCommandStack ::= cmd
  
  /** Try to install sigint handler: ignore failure.  Signal handler
   *  will interrupt current line execution if any is in progress.
   * 
   *  Attempting to protect the repl from accidental exit, we only honor
   *  a single ctrl-C if the current buffer is empty: otherwise we look
   *  for a second one within a short time.
   */
  private def installSigIntHandler() {
    def onExit() {
      Console.println("") // avoiding "shell prompt in middle of line" syndrome
      sys.exit(1)
    }
    ignoring(classOf[Exception]) {
      SignalManager("INT") = {
        if (intp == null)
          onExit()
        else if (intp.lineManager.running)
          intp.lineManager.cancel()
        else if (in.currentLine != "") {
          // non-empty buffer, so make them hit ctrl-C a second time
          SignalManager("INT") = onExit()
          io.timer(5)(installSigIntHandler())  // and restore original handler if they don't
        }
        else onExit()
      }
    }
  }

  /** Close the interpreter and set the var to null. */
  def closeInterpreter() {
    if (intp ne null) {
      intp.close
      intp = null
      Thread.currentThread.setContextClassLoader(originalClassLoader)
    }
  }
  
  class SparkILoopInterpreter extends SparkIMain(settings, out) {
    override lazy val formatting = new Formatting {
      def prompt = SparkILoop.this.prompt
    }
    override protected def createLineManager() = new Line.Manager {
      override def onRunaway(line: Line[_]): Unit = {
        val template = """
          |// She's gone rogue, captain! Have to take her out!
          |// Calling Thread.stop on runaway %s with offending code:
          |// scala> %s""".stripMargin
        
        echo(template.format(line.thread, line.code))
        // XXX no way to suppress the deprecation warning
        line.thread.stop()
        in.redrawLine()
      }
    }
    override protected def parentClassLoader = {
      SparkHelper.explicitParentLoader(settings).getOrElse( classOf[SparkILoop].getClassLoader )
    }
  }

  /** Create a new interpreter. */
  def createInterpreter() {
    if (addedClasspath != "")
      settings.classpath append addedClasspath
      
    intp = new SparkILoopInterpreter
    intp.setContextClassLoader()
    installSigIntHandler()
  }

  /** print a friendly help message */
  def helpCommand(line: String): Result = {
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
  
  /** Print a welcome message */
  def printWelcome() {
    echo("""Welcome to
      ____              __  
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /___/ .__/\_,_/_/ /_/\_\   version 0.8.0
      /_/                  
""")
    import Properties._
    val welcomeMsg = "Using Scala %s (%s, Java %s)".format(
      versionString, javaVmName, javaVersion) 
    echo(welcomeMsg)
  }
  
  /** Show the history */
  lazy val historyCommand = new LoopCommand("history", "show the history (optional num is commands to show)") {
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

  private def echo(msg: String) = {
    out println msg
    out.flush()
  }
  private def echoNoNL(msg: String) = {
    out print msg
    out.flush()
  }
  
  /** Search the history */
  def searchHistory(_cmdline: String) {
    val cmdline = _cmdline.toLowerCase
    val offset  = history.index - history.size + 1
    
    for ((line, index) <- history.asStrings.zipWithIndex ; if line.toLowerCase contains cmdline)
      echo("%d %s".format(index + offset, line))
  }
  
  private var currentPrompt = Properties.shellPromptString
  def setPrompt(prompt: String) = currentPrompt = prompt
  /** Prompt to print when awaiting input */
  def prompt = currentPrompt
  
  import LoopCommand.{ cmd, nullary }

  /** Standard commands **/
  lazy val standardCommands = List(
    cmd("cp", "<path>", "add a jar or directory to the classpath", addClasspath),
    cmd("help", "[command]", "print this summary or command-specific help", helpCommand),
    historyCommand,
    cmd("h?", "<string>", "search the history", searchHistory),
    cmd("imports", "[name name ...]", "show import history, identifying sources of names", importsCommand),
    cmd("implicits", "[-v]", "show the implicits in scope", implicitsCommand),
    cmd("javap", "<path|class>", "disassemble a file or class name", javapCommand),
    nullary("keybindings", "show how ctrl-[A-Z] and other keys are bound", keybindingsCommand),
    cmd("load", "<path>", "load and interpret a Scala file", loadCommand),
    nullary("paste", "enter paste mode: all input up to ctrl-D compiled together", pasteCommand),
    //nullary("power", "enable power user mode", powerCmd),
    nullary("quit", "exit the interpreter", () => Result(false, None)),
    nullary("replay", "reset execution and replay all previous commands", replay),
    shCommand,
    nullary("silent", "disable/enable automatic printing of results", verbosity),
    cmd("type", "<expr>", "display the type of an expression without evaluating it", typeCommand)
  )
  
  /** Power user commands */
  lazy val powerCommands: List[LoopCommand] = List(
    //nullary("dump", "displays a view of the interpreter's internal state", dumpCommand),
    //cmd("phase", "<phase>", "set the implicit phase for power commands", phaseCommand),
    cmd("wrap", "<method>", "name of method to wrap around each repl line", wrapCommand) withLongHelp ("""
      |:wrap
      |:wrap clear
      |:wrap <method>
      |
      |Installs a wrapper around each line entered into the repl.
      |Currently it must be the simple name of an existing method
      |with the specific signature shown in the following example.
      |
      |def timed[T](body: => T): T = {
      |  val start = System.nanoTime
      |  try body
      |  finally println((System.nanoTime - start) + " nanos elapsed.")
      |}
      |:wrap timed
      |
      |If given no argument, :wrap names the wrapper installed.
      |An argument of clear will remove the wrapper if any is active.
      |Note that wrappers do not compose (a new one replaces the old
      |one) and also that the :phase command uses the same machinery,
      |so setting :wrap will clear any :phase setting.       
    """.stripMargin.trim)
  )
  
  /*
  private def dumpCommand(): Result = {
    echo("" + power)
    history.asStrings takeRight 30 foreach echo
    in.redrawLine()
  }
  */
  
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
  
  private def implicitsCommand(line: String): Result = {
    val intp = SparkILoop.this.intp
    import intp._
    import global.Symbol
    
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
        val sortedOwners = byOwner.toList sortBy { case (owner, _) => intp.afterTyper(source.info.baseClasses indexOf owner) }

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
  
  protected def newJavap() = new Javap(intp.classLoader, new SparkIMain.ReplStrippingWriter(intp)) {
    override def tryClass(path: String): Array[Byte] = {
      // Look for Foo first, then Foo$, but if Foo$ is given explicitly,
      // we have to drop the $ to find object Foo, then tack it back onto
      // the end of the flattened name.
      def className  = intp flatName path
      def moduleName = (intp flatName path.stripSuffix("$")) + "$"

      val bytes = super.tryClass(className)
      if (bytes.nonEmpty) bytes
      else super.tryClass(moduleName)
    }
  }
  private lazy val javap =
    try newJavap()
    catch { case _: Exception => null }
  
  private def typeCommand(line: String): Result = {
    intp.typeOfExpression(line) match {
      case Some(tp) => tp.toString
      case _        => "Failed to determine type."
    }
  }
  
  private def javapCommand(line: String): Result = {
    if (javap == null)
      return ":javap unavailable on this platform."
    if (line == "")
      return ":javap [-lcsvp] [path1 path2 ...]"
    
    javap(words(line)) foreach { res =>
      if (res.isError) return "Failed: " + res.value
      else res.show()
    }
  }
  private def keybindingsCommand(): Result = {
    if (in.keyBindings.isEmpty) "Key bindings unavailable."
    else {
      echo("Reading jline properties for default key bindings.")
      echo("Accuracy not guaranteed: treat this as a guideline only.\n")
      in.keyBindings foreach (x => echo ("" + x))
    }
  }
  private def wrapCommand(line: String): Result = {
    def failMsg = "Argument to :wrap must be the name of a method with signature [T](=> T): T"
    val intp = SparkILoop.this.intp
    val g: intp.global.type = intp.global
    import g._

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
          case Some(PolyType(List(targ), MethodType(List(arg), restpe))) =>
            intp setExecutionWrapper intp.pathToTerm(wrapper)
            "Set wrapper to '" + wrapper + "'"
          case Some(x) =>
            failMsg + "\nFound: " + x
          case _ =>
            failMsg + "\nFound: <unknown>"
        }
      case _ => failMsg
    }
  }

  private def pathToPhaseWrapper = intp.pathToTerm("$r") + ".phased.atCurrent"
  /*
  private def phaseCommand(name: String): Result = {
    // This line crashes us in TreeGen:
    //
    //   if (intp.power.phased set name) "..."
    //
    // Exception in thread "main" java.lang.AssertionError: assertion failed: ._7.type
    //  at scala.Predef$.assert(Predef.scala:99)
    //  at scala.tools.nsc.ast.TreeGen.mkAttributedQualifier(TreeGen.scala:69)
    //  at scala.tools.nsc.ast.TreeGen.mkAttributedQualifier(TreeGen.scala:44)
    //  at scala.tools.nsc.ast.TreeGen.mkAttributedRef(TreeGen.scala:101)
    //  at scala.tools.nsc.ast.TreeGen.mkAttributedStableRef(TreeGen.scala:143)
    //
    // But it works like so, type annotated.
    val phased: Phased = power.phased
    import phased.NoPhaseName

    if (name == "clear") {
      phased.set(NoPhaseName)
      intp.clearExecutionWrapper()
      "Cleared active phase."
    }
    else if (name == "") phased.get match {
      case NoPhaseName => "Usage: :phase <expr> (e.g. typer, erasure.next, erasure+3)"
      case ph          => "Active phase is '%s'.  (To clear, :phase clear)".format(phased.get)
    }
    else {
      val what = phased.parse(name)
      if (what.isEmpty || !phased.set(what)) 
        "'" + name + "' does not appear to represent a valid phase."
      else {
        intp.setExecutionWrapper(pathToPhaseWrapper)
        val activeMessage =
          if (what.toString.length == name.length) "" + what
          else "%s (%s)".format(what, name)
        
        "Active phase is now: " + activeMessage
      }
    }
  }
  */
  
  /** Available commands */
  def commands: List[LoopCommand] = standardCommands /* ++ (
    if (isReplPower) powerCommands else Nil
  )*/
  
  val replayQuestionMessage =
    """|The repl compiler has crashed spectacularly. Shall I replay your
       |session? I can re-run all lines except the last one.
       |[y/n]
    """.trim.stripMargin

  private val crashRecovery: PartialFunction[Throwable, Unit] = {
    case ex: Throwable =>
      if (settings.YrichExes.value) {
        val sources = implicitly[Sources]
        echo("\n" + ex.getMessage)
        echo(
          if (isReplDebug) "[searching " + sources.path + " for exception contexts...]"
          else "[searching for exception contexts...]"
        )
        echo(Exceptional(ex).force().context())
      }
      else {
        echo(util.stackTraceString(ex))
      }
      ex match {
        case _: NoSuchMethodError | _: NoClassDefFoundError =>
          echo("Unrecoverable error.")
          throw ex
        case _  =>
          def fn(): Boolean = in.readYesOrNo(replayQuestionMessage, { echo("\nYou must enter y or n.") ; fn() })
          if (fn()) replay()
          else echo("\nAbandoning crashed session.")
      }
  }

  /** The main read-eval-print loop for the repl.  It calls
   *  command() for each line of input, and stops when
   *  command() returns false.
   */
  def loop() {
    def readOneLine() = {
      out.flush()
      in readLine prompt
    }
    // return false if repl should exit
    def processLine(line: String): Boolean =
      if (line eq null) false               // assume null means EOF
      else command(line) match {
        case Result(false, _)           => false
        case Result(_, Some(finalLine)) => addReplay(finalLine) ; true
        case _                          => true
      }

    while (true) {
      try if (!processLine(readOneLine)) return
      catch crashRecovery
    }
  }

  /** interpret all lines from a specified file */
  def interpretAllFrom(file: File) {    
    val oldIn = in
    val oldReplay = replayCommandStack
    
    try file applyReader { reader =>
      in = SimpleReader(reader, out, false)
      echo("Loading " + file + "...")
      loop()
    }
    finally {
      in = oldIn
      replayCommandStack = oldReplay
    }
  }

  /** create a new interpreter and replay all commands so far */
  def replay() {
    closeInterpreter()
    createInterpreter()
    for (cmd <- replayCommands) {
      echo("Replaying: " + cmd)  // flush because maybe cmd will have its own output
      command(cmd)
      echo("")
    }
  }
  
  /** fork a shell and run a command */
  lazy val shCommand = new LoopCommand("sh", "run a shell command (result is implicitly => List[String])") {
    override def usage = "<command line>"
    def apply(line: String): Result = line match {
      case ""   => showUsage()
      case _    => 
        val toRun = classOf[ProcessResult].getName + "(" + string2codeQuoted(line) + ")"
        intp interpret toRun
        ()
    }
  }
  
  def withFile(filename: String)(action: File => Unit) {
    val f = File(filename)
    
    if (f.exists) action(f)
    else echo("That file does not exist")
  }
  
  def loadCommand(arg: String) = {
    var shouldReplay: Option[String] = None
    withFile(arg)(f => {
      interpretAllFrom(f)
      shouldReplay = Some(":load " + arg)
    })
    Result(true, shouldReplay)
  }

  def addClasspath(arg: String): Unit = {
    val f = File(arg).normalize
    if (f.exists) {
      addedClasspath = ClassPath.join(addedClasspath, f.path)
      val totalClasspath = ClassPath.join(settings.classpath.value, addedClasspath)
      echo("Added '%s'.  Your new classpath is:\n\"%s\"".format(f.path, totalClasspath))
      replay()
    }
    else echo("The path '" + f + "' doesn't seem to exist.")
  }
  
  def powerCmd(): Result = {
    if (isReplPower) "Already in power mode."
    else enablePowerMode()
  }
  def enablePowerMode() = {
    //replProps.power setValue true
    //power.unleash()
    //echo(power.banner)
  }
  
  def verbosity() = {
    val old = intp.printResults
    intp.printResults = !old
    echo("Switched " + (if (old) "off" else "on") + " result printing.")
  }
  
  /** Run one command submitted by the user.  Two values are returned:
    * (1) whether to keep running, (2) the line to record for replay,
    * if any. */
  def command(line: String): Result = {
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
  
  def pasteCommand(): Result = {
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
      // Printing this message doesn't work very well because it's buried in the
      // transcript they just pasted.  Todo: a short timer goes off when
      // lines stop coming which tells them to hit ctrl-D.
      //
      // echo("// Detected repl transcript paste: ctrl-D to finish.")
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
  def interpretStartingWith(code: String): Option[String] = {
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
    else {
      def runCompletion = in.completion execute code map (intp bindValue _)
      /** Due to my accidentally letting file completion execution sneak ahead
       *  of actual parsing this now operates in such a way that the scala
       *  interpretation always wins.  However to avoid losing useful file
       *  completion I let it fail and then check the others.  So if you
       *  type /tmp it will echo a failure and then give you a Directory object.
       *  It's not pretty: maybe I'll implement the silence bits I need to avoid
       *  echoing the failure.
       */
      if (intp isParseable code) {
        val (code, result) = reallyInterpret
        //if (power != null && code == IR.Error)
        //  runCompletion
        
        result
      }
      else runCompletion match {
        case Some(_)  => None // completion hit: avoid the latent error
        case _        => reallyInterpret._2  // trigger the latent error
      }
    }
  }

  // runs :load `file` on any files passed via -i
  def loadFiles(settings: Settings) = settings match {
    case settings: GenericRunnerSettings =>
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
  def chooseReader(settings: Settings): InteractiveReader = {
    if (settings.Xnojline.value || Properties.isEmacsShell)
      SimpleReader()
    else try SparkJLineReader(
      if (settings.noCompletion.value) NoCompletion
      else new SparkJLineCompletion(intp)
    )
    catch {
      case ex @ (_: Exception | _: NoClassDefFoundError) =>
        echo("Failed to created SparkJLineReader: " + ex + "\nFalling back to SimpleReader.")
        SimpleReader()
    }
  }

  def initializeSpark() {
    intp.beQuietDuring {
      command("""
        org.apache.spark.repl.Main.interp.out.println("Creating SparkContext...");
        org.apache.spark.repl.Main.interp.out.flush();
        @transient val sc = org.apache.spark.repl.Main.interp.createSparkContext();
        org.apache.spark.repl.Main.interp.out.println("Spark context available as sc.");
        org.apache.spark.repl.Main.interp.out.flush();
        """)
      command("import org.apache.spark.SparkContext._")
    }
    echo("Type in expressions to have them evaluated.")
    echo("Type :help for more information.")
  }

  var sparkContext: SparkContext = null

  def createSparkContext(): SparkContext = {
    val uri = System.getenv("SPARK_EXECUTOR_URI")
    if (uri != null) {
      System.setProperty("spark.executor.uri", uri)
    }
    val master = this.master match {
      case Some(m) => m
      case None => {
        val prop = System.getenv("MASTER")
        if (prop != null) prop else "local"
      }
    }
    val jars = Option(System.getenv("ADD_JARS")).map(_.split(','))
                                                .getOrElse(new Array[String](0))
                                                .map(new java.io.File(_).getAbsolutePath)
    sparkContext = new SparkContext(master, "Spark shell", System.getenv("SPARK_HOME"), jars)
    sparkContext
  }

  def process(settings: Settings): Boolean = {
    // Ensure logging is initialized before any Spark threads try to use logs
    // (because SLF4J initialization is not thread safe)
    initLogging()

    printWelcome()
    echo("Initializing interpreter...")

    // Add JARS specified in Spark's ADD_JARS variable to classpath
    val jars = Option(System.getenv("ADD_JARS")).map(_.split(',')).getOrElse(new Array[String](0))
    jars.foreach(settings.classpath.append(_))

    this.settings = settings
    createInterpreter()
    
    // sets in to some kind of reader depending on environmental cues
    in = in0 match {
      case Some(reader) => SimpleReader(reader, out, true)
      case None         => chooseReader(settings)
    }

    loadFiles(settings)
    // it is broken on startup; go ahead and exit
    if (intp.reporter.hasErrors)
      return false
    
    try {      
      // this is about the illusion of snappiness.  We call initialize()
      // which spins off a separate thread, then print the prompt and try 
      // our best to look ready.  Ideally the user will spend a
      // couple seconds saying "wow, it starts so fast!" and by the time
      // they type a command the compiler is ready to roll.
      intp.initialize()
      initializeSpark()
      if (isReplPower) {
        echo("Starting in power mode, one moment...\n")
        enablePowerMode()
      }
      loop()
    }
    finally closeInterpreter()
    true
  }

  /** process command-line arguments and do as they request */
  def process(args: Array[String]): Boolean = {
    val command = new CommandLine(args.toList, msg => echo("scala: " + msg))
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
  def main(args: Array[String]): Unit = {
    if (isReplDebug)
      System.out.println(new java.util.Date)
    
    process(args)
  }
  @deprecated("Use `process` instead", "2.9.0")
  def main(settings: Settings): Unit = process(settings)
}

object SparkILoop {
  implicit def loopToInterpreter(repl: SparkILoop): SparkIMain = repl.intp
  private def echo(msg: String) = Console println msg

  // Designed primarily for use by test code: take a String with a
  // bunch of code, and prints out a transcript of what it would look
  // like if you'd just typed it into the repl.
  def runForTranscript(code: String, settings: Settings): String = {
    import java.io.{ BufferedReader, StringReader, OutputStreamWriter }
    
    stringFromStream { ostream =>
      Console.withOut(ostream) {
        val output = new PrintWriter(new OutputStreamWriter(ostream), true) {
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
              output.println(s)
            s
          }
        }
        val repl = new SparkILoop(input, output)
        if (settings.classpath.isDefault)
          settings.classpath.value = sys.props("java.class.path")

        repl process settings
      }
    }
  }
  
  /** Creates an interpreter loop with default settings and feeds
   *  the given code to it as input.
   */
  def run(code: String, sets: Settings = new Settings): String = {
    import java.io.{ BufferedReader, StringReader, OutputStreamWriter }
    
    stringFromStream { ostream =>
      Console.withOut(ostream) {
        val input    = new BufferedReader(new StringReader(code))
        val output   = new PrintWriter(new OutputStreamWriter(ostream), true)
        val repl     = new SparkILoop(input, output)
        
        if (sets.classpath.isDefault)
          sets.classpath.value = sys.props("java.class.path")

        repl process sets
      }
    }
  }
  def run(lines: List[String]): String = run(lines map (_ + "\n") mkString)

  // provide the enclosing type T
  // in order to set up the interpreter's classpath and parent class loader properly
  def breakIf[T: Manifest](assertion: => Boolean, args: NamedParam*): Unit =
    if (assertion) break[T](args.toList)

  // start a repl, binding supplied args
  def break[T: Manifest](args: List[NamedParam]): Unit = {
    val msg = if (args.isEmpty) "" else "  Binding " + args.size + " value%s.".format(
      if (args.size == 1) "" else "s"
    )
    echo("Debug repl starting." + msg)
    val repl = new SparkILoop {
      override def prompt = "\ndebug> "
    }
    repl.settings = new Settings(echo)
    repl.settings.embeddedDefaults[T]
    repl.createInterpreter()
    repl.in = SparkJLineReader(repl)
    
    // rebind exit so people don't accidentally call sys.exit by way of predef
    repl.quietRun("""def exit = println("Type :quit to resume program execution.")""")
    args foreach (p => repl.bind(p.name, p.tpe, p.value))
    repl.loop()

    echo("\nDebug repl exiting.")
    repl.closeInterpreter()
  }  
}
