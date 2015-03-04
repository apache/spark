/* NSC -- new Scala compiler
 * Copyright 2005-2013 LAMP/EPFL
 * @author Alexander Spoon
 */

package scala
package tools.nsc
package interpreter

import scala.language.{ implicitConversions, existentials }
import scala.annotation.tailrec
import Predef.{ println => _, _ }
import interpreter.session._
import StdReplTags._
import scala.reflect.api.{Mirror, Universe, TypeCreator}
import scala.util.Properties.{ jdkHome, javaVersion, versionString, javaVmName }
import scala.tools.nsc.util.{ ClassPath, Exceptional, stringFromWriter, stringFromStream }
import scala.reflect.{ClassTag, classTag}
import scala.reflect.internal.util.{ BatchSourceFile, ScalaClassLoader }
import ScalaClassLoader._
import scala.reflect.io.{ File, Directory }
import scala.tools.util._
import scala.collection.generic.Clearable
import scala.concurrent.{ ExecutionContext, Await, Future, future }
import ExecutionContext.Implicits._
import java.io.{ BufferedReader, FileReader }

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
class SparkILoop(in0: Option[BufferedReader], protected val out: JPrintWriter)
  extends AnyRef
  with LoopCommands
{
  def this(in0: BufferedReader, out: JPrintWriter) = this(Some(in0), out)
  def this() = this(None, new JPrintWriter(Console.out, true))
//
//  @deprecated("Use `intp` instead.", "2.9.0") def interpreter = intp
//  @deprecated("Use `intp` instead.", "2.9.0") def interpreter_= (i: Interpreter): Unit = intp = i

  var in: InteractiveReader = _   // the input stream from which commands come
  var settings: Settings = _
  var intp: SparkIMain = _

  var globalFuture: Future[Boolean] = _

  protected def asyncMessage(msg: String) {
    if (isReplInfo || isReplPower)
      echoAndRefresh(msg)
  }

  def initializeSpark() {
    intp.beQuietDuring {
      command( """
         @transient val sc = {
           val _sc = org.apache.spark.repl.Main.createSparkContext()
           println("Spark context available as sc.")
           _sc
         }
        """)
      command( """
         @transient val sqlContext = {
           val _sqlContext = org.apache.spark.repl.Main.createSQLContext()
           println("SQL context available as sqlContext.")
           _sqlContext
         }
        """)
      command("import org.apache.spark.SparkContext._")
      command("import sqlContext.implicits._")
      command("import sqlContext.sql")
      command("import org.apache.spark.sql.functions._")
    }
  }

  /** Print a welcome message */
  def printWelcome() {
    import org.apache.spark.SPARK_VERSION
    echo("""Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /___/ .__/\_,_/_/ /_/\_\   version %s
      /_/
         """.format(SPARK_VERSION))
    val welcomeMsg = "Using Scala %s (%s, Java %s)".format(
      versionString, javaVmName, javaVersion)
    echo(welcomeMsg)
    echo("Type in expressions to have them evaluated.")
    echo("Type :help for more information.")
  }

  override def echoCommandMessage(msg: String) {
    intp.reporter printUntruncatedMessage msg
  }

  // lazy val power = new Power(intp, new StdReplVals(this))(tagOfStdReplVals, classTag[StdReplVals])
  def history = in.history

  // classpath entries added via :cp
  var addedClasspath: String = ""

  /** A reverse list of commands to replay if the user requests a :replay */
  var replayCommandStack: List[String] = Nil

  /** A list of commands to replay if the user requests a :replay */
  def replayCommands = replayCommandStack.reverse

  /** Record a command for replay should the user request a :replay */
  def addReplay(cmd: String) = replayCommandStack ::= cmd

  def savingReplayStack[T](body: => T): T = {
    val saved = replayCommandStack
    try body
    finally replayCommandStack = saved
  }
  def savingReader[T](body: => T): T = {
    val saved = in
    try body
    finally in = saved
  }

  /** Close the interpreter and set the var to null. */
  def closeInterpreter() {
    if (intp ne null) {
      intp.close()
      intp = null
    }
  }

  class SparkILoopInterpreter extends SparkIMain(settings, out) {
    outer =>

    override lazy val formatting = new Formatting {
      def prompt = SparkILoop.this.prompt
    }
    override protected def parentClassLoader =
      settings.explicitParentLoader.getOrElse( classOf[SparkILoop].getClassLoader )
  }

  /** Create a new interpreter. */
  def createInterpreter() {
    if (addedClasspath != "")
      settings.classpath append addedClasspath

    intp = new SparkILoopInterpreter
  }

  /** print a friendly help message */
  def helpCommand(line: String): Result = {
    if (line == "") helpSummary()
    else uniqueCommand(line) match {
      case Some(lc) => echo("\n" + lc.help)
      case _        => ambiguousError(line)
    }
  }
  private def helpSummary() = {
    val usageWidth  = commands map (_.usageMsg.length) max
    val formatStr   = "%-" + usageWidth + "s %s"

    echo("All commands can be abbreviated, e.g. :he instead of :help.")

    commands foreach { cmd =>
      echo(formatStr.format(cmd.usageMsg, cmd.help))
    }
  }
  private def ambiguousError(cmd: String): Result = {
    matchingCommands(cmd) match {
      case Nil  => echo(cmd + ": no such command.  Type :help for help.")
      case xs   => echo(cmd + " is ambiguous: did you mean " + xs.map(":" + _.name).mkString(" or ") + "?")
    }
    Result(keepRunning = true, None)
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

  // When you know you are most likely breaking into the middle
  // of a line being typed.  This softens the blow.
  protected def echoAndRefresh(msg: String) = {
    echo("\n" + msg)
    in.redrawLine()
  }
  protected def echo(msg: String) = {
    out println msg
    out.flush()
  }

  /** Search the history */
  def searchHistory(_cmdline: String) {
    val cmdline = _cmdline.toLowerCase
    val offset  = history.index - history.size + 1

    for ((line, index) <- history.asStrings.zipWithIndex ; if line.toLowerCase contains cmdline)
      echo("%d %s".format(index + offset, line))
  }

  private val currentPrompt = Properties.shellPromptString

  /** Prompt to print when awaiting input */
  def prompt = currentPrompt

  import LoopCommand.{ cmd, nullary }

  /** Standard commands **/
  lazy val standardCommands = List(
    cmd("cp", "<path>", "add a jar or directory to the classpath", addClasspath),
    cmd("edit", "<id>|<line>", "edit history", editCommand),
    cmd("help", "[command]", "print this summary or command-specific help", helpCommand),
    historyCommand,
    cmd("h?", "<string>", "search the history", searchHistory),
    cmd("imports", "[name name ...]", "show import history, identifying sources of names", importsCommand),
    //cmd("implicits", "[-v]", "show the implicits in scope", intp.implicitsCommand),
    cmd("javap", "<path|class>", "disassemble a file or class name", javapCommand),
    cmd("line", "<id>|<line>", "place line(s) at the end of history", lineCommand),
    cmd("load", "<path>", "interpret lines in a file", loadCommand),
    cmd("paste", "[-raw] [path]", "enter paste mode or paste a file", pasteCommand),
    // nullary("power", "enable power user mode", powerCmd),
    nullary("quit", "exit the interpreter", () => Result(keepRunning = false, None)),
    nullary("replay", "reset execution and replay all previous commands", replay),
    nullary("reset", "reset the repl to its initial state, forgetting all session entries", resetCommand),
    cmd("save", "<path>", "save replayable session to a file", saveCommand),
    shCommand,
    cmd("settings", "[+|-]<options>", "+enable/-disable flags, set compiler options", changeSettings),
    nullary("silent", "disable/enable automatic printing of results", verbosity),
//    cmd("type", "[-v] <expr>", "display the type of an expression without evaluating it", typeCommand),
//    cmd("kind", "[-v] <expr>", "display the kind of expression's type", kindCommand),
    nullary("warnings", "show the suppressed warnings from the most recent line which had any", warningsCommand)
  )

  /** Power user commands */
//  lazy val powerCommands: List[LoopCommand] = List(
//    cmd("phase", "<phase>", "set the implicit phase for power commands", phaseCommand)
//  )

  private def importsCommand(line: String): Result = {
    val tokens    = words(line)
    val handlers  = intp.languageWildcardHandlers ++ intp.importHandlers

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

  private def findToolsJar() = PathResolver.SupplementalLocations.platformTools

  private def addToolsJarToLoader() = {
    val cl = findToolsJar() match {
      case Some(tools) => ScalaClassLoader.fromURLs(Seq(tools.toURL), intp.classLoader)
      case _           => intp.classLoader
    }
    if (Javap.isAvailable(cl)) {
      repldbg(":javap available.")
      cl
    }
    else {
      repldbg(":javap unavailable: no tools.jar at " + jdkHome)
      intp.classLoader
    }
  }
//
//  protected def newJavap() =
//    JavapClass(addToolsJarToLoader(), new IMain.ReplStrippingWriter(intp), Some(intp))
//
//  private lazy val javap = substituteAndLog[Javap]("javap", NoJavap)(newJavap())

  // Still todo: modules.
//  private def typeCommand(line0: String): Result = {
//    line0.trim match {
//      case "" => ":type [-v] <expression>"
//      case s  => intp.typeCommandInternal(s stripPrefix "-v " trim, verbose = s startsWith "-v ")
//    }
//  }

//  private def kindCommand(expr: String): Result = {
//    expr.trim match {
//      case "" => ":kind [-v] <expression>"
//      case s  => intp.kindCommandInternal(s stripPrefix "-v " trim, verbose = s startsWith "-v ")
//    }
//  }

  private def warningsCommand(): Result = {
    if (intp.lastWarnings.isEmpty)
      "Can't find any cached warnings."
    else
      intp.lastWarnings foreach { case (pos, msg) => intp.reporter.warning(pos, msg) }
  }

  private def changeSettings(args: String): Result = {
    def showSettings() = {
      for (s <- settings.userSetSettings.toSeq.sorted) echo(s.toString)
    }
    def updateSettings() = {
      // put aside +flag options
      val (pluses, rest) = (args split "\\s+").toList partition (_.startsWith("+"))
      val tmps = new Settings
      val (ok, leftover) = tmps.processArguments(rest, processAll = true)
      if (!ok) echo("Bad settings request.")
      else if (leftover.nonEmpty) echo("Unprocessed settings.")
      else {
        // boolean flags set-by-user on tmp copy should be off, not on
        val offs = tmps.userSetSettings filter (_.isInstanceOf[Settings#BooleanSetting])
        val (minuses, nonbools) = rest partition (arg => offs exists (_ respondsTo arg))
        // update non-flags
        settings.processArguments(nonbools, processAll = true)
        // also snag multi-value options for clearing, e.g. -Ylog: and -language:
        for {
          s <- settings.userSetSettings
          if s.isInstanceOf[Settings#MultiStringSetting] || s.isInstanceOf[Settings#PhasesSetting]
          if nonbools exists (arg => arg.head == '-' && arg.last == ':' && (s respondsTo arg.init))
        } s match {
          case c: Clearable => c.clear()
          case _ =>
        }
        def update(bs: Seq[String], name: String=>String, setter: Settings#Setting=>Unit) = {
          for (b <- bs)
            settings.lookupSetting(name(b)) match {
              case Some(s) =>
                if (s.isInstanceOf[Settings#BooleanSetting]) setter(s)
                else echo(s"Not a boolean flag: $b")
              case _ =>
                echo(s"Not an option: $b")
            }
        }
        update(minuses, identity, _.tryToSetFromPropertyValue("false"))  // turn off
        update(pluses, "-" + _.drop(1), _.tryToSet(Nil))                 // turn on
      }
    }
    if (args.isEmpty) showSettings() else updateSettings()
  }

  private def javapCommand(line: String): Result = {
//    if (javap == null)
//      ":javap unavailable, no tools.jar at %s.  Set JDK_HOME.".format(jdkHome)
//    else if (line == "")
//      ":javap [-lcsvp] [path1 path2 ...]"
//    else
//      javap(words(line)) foreach { res =>
//        if (res.isError) return "Failed: " + res.value
//        else res.show()
//      }
  }

  private def pathToPhaseWrapper = intp.originalPath("$r") + ".phased.atCurrent"

  private def phaseCommand(name: String): Result = {
//    val phased: Phased = power.phased
//    import phased.NoPhaseName
//
//    if (name == "clear") {
//      phased.set(NoPhaseName)
//      intp.clearExecutionWrapper()
//      "Cleared active phase."
//    }
//    else if (name == "") phased.get match {
//      case NoPhaseName => "Usage: :phase <expr> (e.g. typer, erasure.next, erasure+3)"
//      case ph          => "Active phase is '%s'.  (To clear, :phase clear)".format(phased.get)
//    }
//    else {
//      val what = phased.parse(name)
//      if (what.isEmpty || !phased.set(what))
//        "'" + name + "' does not appear to represent a valid phase."
//      else {
//        intp.setExecutionWrapper(pathToPhaseWrapper)
//        val activeMessage =
//          if (what.toString.length == name.length) "" + what
//          else "%s (%s)".format(what, name)
//
//        "Active phase is now: " + activeMessage
//      }
//    }
  }

  /** Available commands */
  def commands: List[LoopCommand] = standardCommands ++ (
    // if (isReplPower)
    //  powerCommands
    // else
      Nil
    )

  val replayQuestionMessage =
    """|That entry seems to have slain the compiler.  Shall I replay
      |your session? I can re-run each line except the last one.
      |[y/n]
    """.trim.stripMargin

  private val crashRecovery: PartialFunction[Throwable, Boolean] = {
    case ex: Throwable =>
      val (err, explain) = (
        if (intp.isInitializeComplete)
          (intp.global.throwableAsString(ex), "")
        else
          (ex.getMessage, "The compiler did not initialize.\n")
        )
      echo(err)

      ex match {
        case _: NoSuchMethodError | _: NoClassDefFoundError =>
          echo("\nUnrecoverable error.")
          throw ex
        case _  =>
          def fn(): Boolean =
            try in.readYesOrNo(explain + replayQuestionMessage, { echo("\nYou must enter y or n.") ; fn() })
            catch { case _: RuntimeException => false }

          if (fn()) replay()
          else echo("\nAbandoning crashed session.")
      }
      true
  }

  // return false if repl should exit
  def processLine(line: String): Boolean = {
    import scala.concurrent.duration._
    Await.ready(globalFuture, 60.seconds)

    (line ne null) && (command(line) match {
      case Result(false, _)      => false
      case Result(_, Some(line)) => addReplay(line) ; true
      case _                     => true
    })
  }

  private def readOneLine() = {
    out.flush()
    in readLine prompt
  }

  /** The main read-eval-print loop for the repl.  It calls
    *  command() for each line of input, and stops when
    *  command() returns false.
    */
  @tailrec final def loop() {
    if ( try processLine(readOneLine()) catch crashRecovery )
      loop()
  }

  /** interpret all lines from a specified file */
  def interpretAllFrom(file: File) {
    savingReader {
      savingReplayStack {
        file applyReader { reader =>
          in = SimpleReader(reader, out, interactive = false)
          echo("Loading " + file + "...")
          loop()
        }
      }
    }
  }

  /** create a new interpreter and replay the given commands */
  def replay() {
    reset()
    if (replayCommandStack.isEmpty)
      echo("Nothing to replay.")
    else for (cmd <- replayCommands) {
      echo("Replaying: " + cmd)  // flush because maybe cmd will have its own output
      command(cmd)
      echo("")
    }
  }
  def resetCommand() {
    echo("Resetting interpreter state.")
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
  def reset() {
    intp.reset()
    unleashAndSetPhase()
  }

  def lineCommand(what: String): Result = editCommand(what, None)

  // :edit id or :edit line
  def editCommand(what: String): Result = editCommand(what, Properties.envOrNone("EDITOR"))

  def editCommand(what: String, editor: Option[String]): Result = {
    def diagnose(code: String) = {
      echo("The edited code is incomplete!\n")
      val errless = intp compileSources new BatchSourceFile("<pastie>", s"object pastel {\n$code\n}")
      if (errless) echo("The compiler reports no errors.")
    }
    def historicize(text: String) = history match {
      case jlh: JLineHistory => text.lines foreach jlh.add ; jlh.moveToEnd() ; true
      case _ => false
    }
    def edit(text: String): Result = editor match {
      case Some(ed) =>
        val tmp = File.makeTemp()
        tmp.writeAll(text)
        try {
          val pr = new ProcessResult(s"$ed ${tmp.path}")
          pr.exitCode match {
            case 0 =>
              tmp.safeSlurp() match {
                case Some(edited) if edited.trim.isEmpty => echo("Edited text is empty.")
                case Some(edited) =>
                  echo(edited.lines map ("+" + _) mkString "\n")
                  val res = intp interpret edited
                  if (res == IR.Incomplete) diagnose(edited)
                  else {
                    historicize(edited)
                    Result(lineToRecord = Some(edited), keepRunning = true)
                  }
                case None => echo("Can't read edited text. Did you delete it?")
              }
            case x => echo(s"Error exit from $ed ($x), ignoring")
          }
        } finally {
          tmp.delete()
        }
      case None =>
        if (historicize(text)) echo("Placing text in recent history.")
        else echo(f"No EDITOR defined and you can't change history, echoing your text:%n$text")
    }

    // if what is a number, use it as a line number or range in history
    def isNum = what forall (c => c.isDigit || c == '-' || c == '+')
    // except that "-" means last value
    def isLast = (what == "-")
    if (isLast || !isNum) {
      val name = if (isLast) intp.mostRecentVar else what
      val sym = intp.symbolOfIdent(name)
      intp.prevRequestList collectFirst { case r if r.defines contains sym => r } match {
        case Some(req) => edit(req.line)
        case None      => echo(s"No symbol in scope: $what")
      }
    } else try {
      val s = what
      // line 123, 120+3, -3, 120-123, 120-, note -3 is not 0-3 but (cur-3,cur)
      val (start, len) =
        if ((s indexOf '+') > 0) {
          val (a,b) = s splitAt (s indexOf '+')
          (a.toInt, b.drop(1).toInt)
        } else {
          (s indexOf '-') match {
            case -1 => (s.toInt, 1)
            case 0  => val n = s.drop(1).toInt ; (history.index - n, n)
            case _ if s.last == '-' => val n = s.init.toInt ; (n, history.index - n)
            case i  => val n = s.take(i).toInt ; (n, s.drop(i+1).toInt - n)
          }
        }
      import scala.collection.JavaConverters._
      val index = (start - 1) max 0
      val text = history match {
        case jlh: JLineHistory => jlh.entries(index).asScala.take(len) map (_.value) mkString "\n"
        case _ => history.asStrings.slice(index, index + len) mkString "\n"
      }
      edit(text)
    } catch {
      case _: NumberFormatException => echo(s"Bad range '$what'")
        echo("Use line 123, 120+3, -3, 120-123, 120-, note -3 is not 0-3 but (cur-3,cur)")
    }
  }

  /** fork a shell and run a command */
  lazy val shCommand = new LoopCommand("sh", "run a shell command (result is implicitly => List[String])") {
    override def usage = "<command line>"
    def apply(line: String): Result = line match {
      case ""   => showUsage()
      case _    =>
        val toRun = s"new ${classOf[ProcessResult].getName}(${string2codeQuoted(line)})"
        intp interpret toRun
        ()
    }
  }

  def withFile[A](filename: String)(action: File => A): Option[A] = {
    val res = Some(File(filename)) filter (_.exists) map action
    if (res.isEmpty) echo("That file does not exist")  // courtesy side-effect
    res
  }

  def loadCommand(arg: String) = {
    var shouldReplay: Option[String] = None
    withFile(arg)(f => {
      interpretAllFrom(f)
      shouldReplay = Some(":load " + arg)
    })
    Result(keepRunning = true, shouldReplay)
  }

  def saveCommand(filename: String): Result = (
    if (filename.isEmpty) echo("File name is required.")
    else if (replayCommandStack.isEmpty) echo("No replay commands in session")
    else File(filename).printlnAll(replayCommands: _*)
    )

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
    else enablePowerMode(isDuringInit = false)
  }
  def enablePowerMode(isDuringInit: Boolean) = {
    replProps.power setValue true
    unleashAndSetPhase()
    // asyncEcho(isDuringInit, power.banner)
  }
  private def unleashAndSetPhase() {
    if (isReplPower) {
    //  power.unleash()
      // Set the phase to "typer"
      // intp beSilentDuring phaseCommand("typer")
    }
  }

  def asyncEcho(async: Boolean, msg: => String) {
    if (async) asyncMessage(msg)
    else echo(msg)
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
    else if (intp.global == null) Result(keepRunning = false, None)  // Notice failure to create compiler
    else Result(keepRunning = true, interpretStartingWith(line))
  }

  private def readWhile(cond: String => Boolean) = {
    Iterator continually in.readLine("") takeWhile (x => x != null && cond(x))
  }

  def pasteCommand(arg: String): Result = {
    var shouldReplay: Option[String] = None
    def result = Result(keepRunning = true, shouldReplay)
    val (raw, file) =
      if (arg.isEmpty) (false, None)
      else {
        val r = """(-raw)?(\s+)?([^\-]\S*)?""".r
        arg match {
          case r(flag, sep, name) =>
            if (flag != null && name != null && sep == null)
              echo(s"""I assume you mean "$flag $name"?""")
            (flag != null, Option(name))
          case _ =>
            echo("usage: :paste -raw file")
            return result
        }
      }
    val code = file match {
      case Some(name) =>
        withFile(name)(f => {
          shouldReplay = Some(s":paste $arg")
          val s = f.slurp.trim
          if (s.isEmpty) echo(s"File contains no code: $f")
          else echo(s"Pasting file $f...")
          s
        }) getOrElse ""
      case None =>
        echo("// Entering paste mode (ctrl-D to finish)\n")
        val text = (readWhile(_ => true) mkString "\n").trim
        if (text.isEmpty) echo("\n// Nothing pasted, nothing gained.\n")
        else echo("\n// Exiting paste mode, now interpreting.\n")
        text
    }
    def interpretCode() = {
      val res = intp interpret code
      // if input is incomplete, let the compiler try to say why
      if (res == IR.Incomplete) {
        echo("The pasted code is incomplete!\n")
        // Remembrance of Things Pasted in an object
        val errless = intp compileSources new BatchSourceFile("<pastie>", s"object pastel {\n$code\n}")
        if (errless) echo("...but compilation found no error? Good luck with that.")
      }
    }
    def compileCode() = {
      val errless = intp compileSources new BatchSourceFile("<pastie>", code)
      if (!errless) echo("There were compilation errors!")
    }
    if (code.nonEmpty) {
      if (raw) compileCode() else interpretCode()
    }
    result
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
    else if (code.trim startsWith "//") {
      // line comment, do nothing
      None
    }
    else
      reallyInterpret._2
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
    if (settings.Xnojline || Properties.isEmacsShell)
      SimpleReader()
    else try new JLineReader(
      if (settings.noCompletion) NoCompletion
      else new SparkJLineCompletion(intp)
    )
    catch {
      case ex @ (_: Exception | _: NoClassDefFoundError) =>
        echo("Failed to created JLineReader: " + ex + "\nFalling back to SimpleReader.")
        SimpleReader()
    }
  }
  protected def tagOfStaticClass[T: ClassTag]: u.TypeTag[T] =
    u.TypeTag[T](
      m,
      new TypeCreator {
        def apply[U <: Universe with Singleton](m: Mirror[U]): U # Type =
          m.staticClass(classTag[T].runtimeClass.getName).toTypeConstructor.asInstanceOf[U # Type]
      })

  private def loopPostInit() {
    // Bind intp somewhere out of the regular namespace where
    // we can get at it in generated code.
    intp.quietBind(NamedParam[SparkIMain]("$intp", intp)(tagOfStaticClass[SparkIMain], classTag[SparkIMain]))
    // Auto-run code via some setting.
    ( replProps.replAutorunCode.option
      flatMap (f => io.File(f).safeSlurp())
      foreach (intp quietRun _)
      )
    // classloader and power mode setup
    intp.setContextClassLoader()
    if (isReplPower) {
     // replProps.power setValue true
     // unleashAndSetPhase()
     // asyncMessage(power.banner)
    }
    // SI-7418 Now, and only now, can we enable TAB completion.
    in match {
      case x: JLineReader => x.consoleReader.postInit
      case _              =>
    }
  }
  def process(settings: Settings): Boolean = savingContextLoader {
    this.settings = settings
    createInterpreter()

    // sets in to some kind of reader depending on environmental cues
    in = in0.fold(chooseReader(settings))(r => SimpleReader(r, out, interactive = true))
    globalFuture = future {
      intp.initializeSynchronous()
      loopPostInit()
      !intp.reporter.hasErrors
    }
    import scala.concurrent.duration._
    Await.ready(globalFuture, 10 seconds)
    printWelcome()
    initializeSpark()
    loadFiles(settings)

    try loop()
    catch AbstractOrMissingHandler()
    finally closeInterpreter()

    true
  }

  @deprecated("Use `process` instead", "2.9.0")
  def main(settings: Settings): Unit = process(settings) //used by sbt
}

object SparkILoop {
  implicit def loopToInterpreter(repl: SparkILoop): SparkIMain = repl.intp

  // Designed primarily for use by test code: take a String with a
  // bunch of code, and prints out a transcript of what it would look
  // like if you'd just typed it into the repl.
  def runForTranscript(code: String, settings: Settings): String = {
    import java.io.{ BufferedReader, StringReader, OutputStreamWriter }

    stringFromStream { ostream =>
      Console.withOut(ostream) {
        val output = new JPrintWriter(new OutputStreamWriter(ostream), true) {
          override def write(str: String) = {
            // completely skip continuation lines
            if (str forall (ch => ch.isWhitespace || ch == '|')) ()
            else super.write(str)
          }
        }
        val input = new BufferedReader(new StringReader(code.trim + "\n")) {
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
        val output   = new JPrintWriter(new OutputStreamWriter(ostream), true)
        val repl     = new SparkILoop(input, output)

        if (sets.classpath.isDefault)
          sets.classpath.value = sys.props("java.class.path")

        repl process sets
      }
    }
  }
  def run(lines: List[String]): String = run(lines map (_ + "\n") mkString)
}
