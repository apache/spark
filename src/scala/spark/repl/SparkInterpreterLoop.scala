/* NSC -- new Scala compiler
 * Copyright 2005-2010 LAMP/EPFL
 * @author Alexander Spoon
 */

package spark.repl

import scala.tools.nsc
import scala.tools.nsc._

import Predef.{ println => _, _ }
import java.io.{ BufferedReader, FileReader, PrintWriter }
import java.io.IOException

import scala.tools.nsc.{ InterpreterResults => IR }
import scala.annotation.tailrec
import scala.collection.mutable.ListBuffer
import scala.concurrent.ops
import util.{ ClassPath }
import interpreter._
import io.{ File, Process }

import spark.SparkContext

// Classes to wrap up interpreter commands and their results
// You can add new commands by adding entries to val commands
// inside InterpreterLoop.
trait InterpreterControl {
  self: SparkInterpreterLoop =>
  
  // the default result means "keep running, and don't record that line"
  val defaultResult = Result(true, None)
  
  // a single interpreter command
  sealed abstract class Command extends Function1[List[String], Result] {
    def name: String
    def help: String
    def error(msg: String) = {
      out.println(":" + name + " " + msg + ".")
      Result(true, None)
    }
    def usage(): String
  }
  
  case class NoArgs(name: String, help: String, f: () => Result) extends Command {
    def usage(): String = ":" + name
    def apply(args: List[String]) = if (args.isEmpty) f() else error("accepts no arguments")
  }
  
  case class LineArg(name: String, help: String, f: (String) => Result) extends Command {
    def usage(): String = ":" + name + " <line>"
    def apply(args: List[String]) = f(args mkString " ")
  }

  case class OneArg(name: String, help: String, f: (String) => Result) extends Command {
    def usage(): String = ":" + name + " <arg>"
    def apply(args: List[String]) =
      if (args.size == 1) f(args.head)
      else error("requires exactly one argument")
  }

  case class VarArgs(name: String, help: String, f: (List[String]) => Result) extends Command {
    def usage(): String = ":" + name + " [arg]"
    def apply(args: List[String]) = f(args)
  }

  // the result of a single command
  case class Result(keepRunning: Boolean, lineToRecord: Option[String])
}

/** The 
 *  <a href="http://scala-lang.org/" target="_top">Scala</a>
 *  interactive shell.  It provides a read-eval-print loop around
 *  the Interpreter class.
 *  After instantiation, clients should call the <code>main()</code> method.
 *
 *  <p>If no in0 is specified, then input will come from the console, and
 *  the class will attempt to provide input editing feature such as
 *  input history.
 *
 *  @author Moez A. Abdel-Gawad
 *  @author  Lex Spoon
 *  @version 1.2
 */
class SparkInterpreterLoop(
  in0: Option[BufferedReader], val out: PrintWriter, master: Option[String])
extends InterpreterControl {
  def this(in0: BufferedReader, out: PrintWriter, master: String) =
    this(Some(in0), out, Some(master))
  
  def this(in0: BufferedReader, out: PrintWriter) =
    this(Some(in0), out, None)

  def this() = this(None, new PrintWriter(Console.out), None)

  /** The input stream from which commands come, set by main() */
  var in: SparkInteractiveReader = _

  /** The context class loader at the time this object was created */
  protected val originalClassLoader = Thread.currentThread.getContextClassLoader
  
  var settings: Settings = _          // set by main()
  var interpreter: SparkInterpreter = _    // set by createInterpreter()
    
  // classpath entries added via :cp
  var addedClasspath: String = ""

  /** A reverse list of commands to replay if the user requests a :replay */
  var replayCommandStack: List[String] = Nil

  /** A list of commands to replay if the user requests a :replay */
  def replayCommands = replayCommandStack.reverse

  /** Record a command for replay should the user request a :replay */
  def addReplay(cmd: String) = replayCommandStack ::= cmd

  /** Close the interpreter and set the var to <code>null</code>. */
  def closeInterpreter() {
    if (interpreter ne null) {
      interpreter.close
      interpreter = null
      Thread.currentThread.setContextClassLoader(originalClassLoader)
    }
  }

  /** Create a new interpreter. */
  def createInterpreter() {
    if (addedClasspath != "")
      settings.classpath append addedClasspath
      
    interpreter = new SparkInterpreter(settings, out) {
      override protected def parentClassLoader = classOf[SparkInterpreterLoop].getClassLoader
    }
    interpreter.setContextClassLoader()
    // interpreter.quietBind("settings", "spark.repl.SparkInterpreterSettings", interpreter.isettings)
  }

  /** print a friendly help message */
  def printHelp() = {
    out println "All commands can be abbreviated - for example :he instead of :help.\n"
    val cmds = commands map (x => (x.usage, x.help))
    val width: Int = cmds map { case (x, _) => x.length } max
    val formatStr = "%-" + width + "s %s"
    cmds foreach { case (usage, help) => out println formatStr.format(usage, help) }
  } 
  
  /** Print a welcome message */
  def printWelcome() {
    plushln("""Welcome to
      ____              __  
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /___/ .__/\_,_/_/ /_/\_\   version 0.0
      /_/                  
""")

    import Properties._
    val welcomeMsg = "Using Scala %s (%s, Java %s)".format(
      versionString, javaVmName, javaVersion) 
    plushln(welcomeMsg)
  }
  
  /** Show the history */
  def printHistory(xs: List[String]) {
    val defaultLines = 20
    
    if (in.history.isEmpty)
      return println("No history available.")

    val current = in.history.get.index
    val count = try xs.head.toInt catch { case _: Exception => defaultLines }
    val lines = in.historyList takeRight count
    val offset = current - lines.size + 1

    for ((line, index) <- lines.zipWithIndex)
      println("%d %s".format(index + offset, line))
  }
  
  /** Some print conveniences */
  def println(x: Any) = out println x
  def plush(x: Any) = { out print x ; out.flush() }
  def plushln(x: Any) = { out println x ; out.flush() }
  
  /** Search the history */
  def searchHistory(_cmdline: String) {
    val cmdline = _cmdline.toLowerCase
    
    if (in.history.isEmpty)
      return println("No history available.")
    
    val current = in.history.get.index
    val offset = current - in.historyList.size + 1
    
    for ((line, index) <- in.historyList.zipWithIndex ; if line.toLowerCase contains cmdline)
      println("%d %s".format(index + offset, line))
  }
  
  /** Prompt to print when awaiting input */
  val prompt = Properties.shellPromptString

  // most commands do not want to micromanage the Result, but they might want
  // to print something to the console, so we accomodate Unit and String returns.
  object CommandImplicits {
    implicit def u2ir(x: Unit): Result = defaultResult
    implicit def s2ir(s: String): Result = {
      out println s
      defaultResult
    }
  }
  
  /** Standard commands **/
  val standardCommands: List[Command] = {
    import CommandImplicits._
    List(
       OneArg("cp", "add an entry (jar or directory) to the classpath", addClasspath),
       NoArgs("help", "print this help message", printHelp),
       VarArgs("history", "show the history (optional arg: lines to show)", printHistory),
       LineArg("h?", "search the history", searchHistory),
       OneArg("load", "load and interpret a Scala file", load),
       NoArgs("power", "enable power user mode", power),
       NoArgs("quit", "exit the interpreter", () => Result(false, None)),
       NoArgs("replay", "reset execution and replay all previous commands", replay),
       LineArg("sh", "fork a shell and run a command", runShellCmd),
       NoArgs("silent", "disable/enable automatic printing of results", verbosity) 
    )
  }
  
  /** Power user commands */
  var powerUserOn = false
  val powerCommands: List[Command] = {
    import CommandImplicits._
    List(
      OneArg("completions", "generate list of completions for a given String", completions),
      NoArgs("dump", "displays a view of the interpreter's internal state", () => interpreter.power.dump())
      
      // VarArgs("tree", "displays ASTs for specified identifiers",
      //   (xs: List[String]) => interpreter dumpTrees xs)
      // LineArg("meta", "given code which produces scala code, executes the results",
      //   (xs: List[String]) => )
    )
  }
  
  /** Available commands */
  def commands: List[Command] = standardCommands ::: (if (powerUserOn) powerCommands else Nil)

  def initializeSpark() {
    interpreter.beQuietDuring {
      command("""
        spark.repl.Main.interp.out.println("Registering with Mesos...");
        spark.repl.Main.interp.out.flush();
        @transient val sc = spark.repl.Main.interp.createSparkContext();
        sc.waitForRegister();
        spark.repl.Main.interp.out.println("Spark context available as sc.");
        spark.repl.Main.interp.out.flush();
        """)
      command("import spark.SparkContext._");
    }
    plushln("Type in expressions to have them evaluated.")
    plushln("Type :help for more information.")
  }

  def createSparkContext(): SparkContext = {
    val master = this.master match {
      case Some(m) => m
      case None => {
        val prop = System.getenv("MASTER")
        if (prop != null) prop else "local"
      }
    }
    new SparkContext(master, "Spark REPL")
  }

  /** The main read-eval-print loop for the interpreter.  It calls
   *  <code>command()</code> for each line of input, and stops when
   *  <code>command()</code> returns <code>false</code>.
   */
  def repl() {
    def readOneLine() = {
      out.flush
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
    
    while (processLine(readOneLine)) { }
  }

  /** interpret all lines from a specified file */
  def interpretAllFrom(file: File) {    
    val oldIn = in
    val oldReplay = replayCommandStack
    
    try file applyReader { reader =>
      in = new SparkSimpleReader(reader, out, false)
      plushln("Loading " + file + "...")
      repl()
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
      plushln("Replaying: " + cmd)  // flush because maybe cmd will have its own output
      command(cmd)
      out.println
    }
  }
    
  /** fork a shell and run a command */
  def runShellCmd(line: String) {
    // we assume if they're using :sh they'd appreciate being able to pipeline
    interpreter.beQuietDuring {
      interpreter.interpret("import _root_.scala.tools.nsc.io.Process.Pipe._")
    }
    val p = Process(line)
    // only bind non-empty streams
    def add(name: String, it: Iterator[String]) =
      if (it.hasNext) interpreter.bind(name, "scala.List[String]", it.toList)
    
    List(("stdout", p.stdout), ("stderr", p.stderr)) foreach (add _).tupled
  }
  
  def withFile(filename: String)(action: File => Unit) {
    val f = File(filename)
    
    if (f.exists) action(f)
    else out.println("That file does not exist")
  }
  
  def load(arg: String) = {
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
      println("Added '%s'.  Your new classpath is:\n%s".format(f.path, totalClasspath))
      replay()
    }
    else out.println("The path '" + f + "' doesn't seem to exist.")
  }
  
  def completions(arg: String): Unit = {
    val comp = in.completion getOrElse { return println("Completion unavailable.") }
    val xs  = comp completions arg

    injectAndName(xs)
  }
  
  def power() {
    val powerUserBanner =
      """** Power User mode enabled - BEEP BOOP      **
        |** scala.tools.nsc._ has been imported      **
        |** New vals! Try repl, global, power        **
        |** New cmds! :help to discover them         **
        |** New defs! Type power.<tab> to reveal     **""".stripMargin

    powerUserOn = true
    interpreter.unleash()    
    injectOne("history", in.historyList)
    in.completion foreach (x => injectOne("completion", x))
    out println powerUserBanner
  }
  
  def verbosity() = {
    val old = interpreter.printResults
    interpreter.printResults = !old
    out.println("Switched " + (if (old) "off" else "on") + " result printing.")
  }
  
  /** Run one command submitted by the user.  Two values are returned:
    * (1) whether to keep running, (2) the line to record for replay,
    * if any. */
  def command(line: String): Result = {
    def withError(msg: String) = {
      out println msg
      Result(true, None)
    }
    def ambiguous(cmds: List[Command]) = "Ambiguous: did you mean " + cmds.map(":" + _.name).mkString(" or ") + "?"

    // not a command
    if (!line.startsWith(":")) {
      // Notice failure to create compiler
      if (interpreter.compiler == null) return Result(false, None)
      else return Result(true, interpretStartingWith(line))
    }

    val tokens = (line drop 1 split """\s+""").toList
    if (tokens.isEmpty)
      return withError(ambiguous(commands))
    
    val (cmd :: args) = tokens
    
    // this lets us add commands willy-nilly and only requires enough command to disambiguate
    commands.filter(_.name startsWith cmd) match {
      case List(x)  => x(args)
      case Nil      => withError("Unknown command.  Type :help for help.")
      case xs       => withError(ambiguous(xs))
    }
  }

  private val CONTINUATION_STRING = "     | "
  private val PROMPT_STRING = "scala> "
  
  /** If it looks like they're pasting in a scala interpreter
   *  transcript, remove all the formatting we inserted so we
   *  can make some sense of it.
   */
  private var pasteStamp: Long = 0

  /** Returns true if it's long enough to quit. */
  def updatePasteStamp(): Boolean = {
    /* Enough milliseconds between readLines to call it a day. */
    val PASTE_FINISH = 1000

    val prevStamp = pasteStamp
    pasteStamp = System.currentTimeMillis
    
    (pasteStamp - prevStamp > PASTE_FINISH)
  
  }
  /** TODO - we could look for the usage of resXX variables in the transcript.
   *  Right now backreferences to auto-named variables will break.
   */
  
  /** The trailing lines complication was an attempt to work around the introduction
   *  of newlines in e.g. email messages of repl sessions.  It doesn't work because
   *  an unlucky newline can always leave you with a syntactically valid first line,
   *  which is executed before the next line is considered.  So this doesn't actually
   *  accomplish anything, but I'm leaving it in case I decide to try harder.
   */
  case class PasteCommand(cmd: String, trailing: ListBuffer[String] = ListBuffer[String]())
  
  /** Commands start on lines beginning with "scala>" and each successive
   *  line which begins with the continuation string is appended to that command.
   *  Everything else is discarded.  When the end of the transcript is spotted,
   *  all the commands are replayed.
   */
  @tailrec private def cleanTranscript(lines: List[String], acc: List[PasteCommand]): List[PasteCommand] = lines match {
    case Nil                                    => acc.reverse      
    case x :: xs if x startsWith PROMPT_STRING  =>
      val first = x stripPrefix PROMPT_STRING
      val (xs1, xs2) = xs span (_ startsWith CONTINUATION_STRING)
      val rest = xs1 map (_ stripPrefix CONTINUATION_STRING)
      val result = (first :: rest).mkString("", "\n", "\n")
      
      cleanTranscript(xs2, PasteCommand(result) :: acc)
      
    case ln :: lns =>
      val newacc = acc match {
        case Nil => Nil
        case PasteCommand(cmd, trailing) :: accrest =>
          PasteCommand(cmd, trailing :+ ln) :: accrest
      }
      cleanTranscript(lns, newacc)
  }

  /** The timestamp is for safety so it doesn't hang looking for the end
   *  of a transcript.  Ad hoc parsing can't be too demanding.  You can
   *  also use ctrl-D to start it parsing.
   */
  @tailrec private def interpretAsPastedTranscript(lines: List[String]) {
    val line = in.readLine("")
    val finished = updatePasteStamp()

    if (line == null || finished || line.trim == PROMPT_STRING.trim) {
      val xs = cleanTranscript(lines.reverse, Nil)
      println("Replaying %d commands from interpreter transcript." format xs.size)
      for (PasteCommand(cmd, trailing) <- xs) {
        out.flush()
        def runCode(code: String, extraLines: List[String]) {
          (interpreter interpret code) match {
            case IR.Incomplete if extraLines.nonEmpty =>
              runCode(code + "\n" + extraLines.head, extraLines.tail)
            case _ => ()
          }
        }
        runCode(cmd, trailing.toList)
      }
    }
    else
      interpretAsPastedTranscript(line :: lines)
  }

  /** Interpret expressions starting with the first line.
    * Read lines until a complete compilation unit is available
    * or until a syntax error has been seen.  If a full unit is
    * read, go ahead and interpret it.  Return the full string
    * to be recorded for replay, if any.
    */
  def interpretStartingWith(code: String): Option[String] = {
    // signal completion non-completion input has been received
    in.completion foreach (_.resetVerbosity())
    
    def reallyInterpret = interpreter.interpret(code) match {
      case IR.Error       => None
      case IR.Success     => Some(code)
      case IR.Incomplete  =>
        if (in.interactive && code.endsWith("\n\n")) {
          out.println("You typed two blank lines.  Starting a new command.")
          None
        } 
        else in.readLine(CONTINUATION_STRING) match {
          case null =>
            // we know compilation is going to fail since we're at EOF and the
            // parser thinks the input is still incomplete, but since this is
            // a file being read non-interactively we want to fail.  So we send
            // it straight to the compiler for the nice error message.
            interpreter.compileString(code)
            None

          case line => interpretStartingWith(code + "\n" + line)
        }
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
    else if (code startsWith PROMPT_STRING) {
      updatePasteStamp()
      interpretAsPastedTranscript(List(code))
      None
    }
    else if (Completion.looksLikeInvocation(code) && interpreter.mostRecentVar != "") {
      interpretStartingWith(interpreter.mostRecentVar + code)
    }
    else {
      val result = for (comp <- in.completion ; res <- comp execute code) yield res
      result match {
        case Some(res)  => injectAndName(res) ; None   // completion took responsibility, so do not parse
        case _          => reallyInterpret
      }
    }
  }

  // runs :load <file> on any files passed via -i
  def loadFiles(settings: Settings) = settings match {
    case settings: GenericRunnerSettings =>
      for (filename <- settings.loadfiles.value) {
        val cmd = ":load " + filename
        command(cmd)
        addReplay(cmd)
        out.println()
      }
    case _ =>
  }

  def main(settings: Settings) {
    this.settings = settings
    createInterpreter()
    
    // sets in to some kind of reader depending on environmental cues
    in = in0 match {
      case Some(in0)  => new SparkSimpleReader(in0, out, true)
      case None       =>        
        // the interpreter is passed as an argument to expose tab completion info
        if (settings.Xnojline.value || Properties.isEmacsShell) new SparkSimpleReader
        else if (settings.noCompletion.value) SparkInteractiveReader.createDefault()
        else SparkInteractiveReader.createDefault(interpreter)
    }

    loadFiles(settings)
    try {
      // it is broken on startup; go ahead and exit
      if (interpreter.reporter.hasErrors) return
      
      printWelcome()
      
      // this is about the illusion of snappiness.  We call initialize()
      // which spins off a separate thread, then print the prompt and try 
      // our best to look ready.  Ideally the user will spend a
      // couple seconds saying "wow, it starts so fast!" and by the time
      // they type a command the compiler is ready to roll.
      interpreter.initialize()
      initializeSpark()
      repl()
    }
    finally closeInterpreter()
  }
  
  private def objClass(x: Any) = x.asInstanceOf[AnyRef].getClass
  private def objName(x: Any) = {
    val clazz = objClass(x)
    val typeParams = clazz.getTypeParameters
    val basename = clazz.getName
    val tpString = if (typeParams.isEmpty) "" else "[%s]".format(typeParams map (_ => "_") mkString ", ")

    basename + tpString
  }

  // injects one value into the repl; returns pair of name and class
  def injectOne(name: String, obj: Any): Tuple2[String, String] = {
    val className = objName(obj)
    interpreter.quietBind(name, className, obj)
    (name, className)
  }
  def injectAndName(obj: Any): Tuple2[String, String] = {
    val name = interpreter.getVarName
    val className = objName(obj)
    interpreter.bind(name, className, obj)
    (name, className)
  }
  
  // injects list of values into the repl; returns summary string
  def injectDebug(args: List[Any]): String = {
    val strs = 
      for ((arg, i) <- args.zipWithIndex) yield {
        val varName = "p" + (i + 1)
        val (vname, vtype) = injectOne(varName, arg)
        vname + ": " + vtype
      }
    
    if (strs.size == 0) "Set no variables."
    else "Variables set:\n" + strs.mkString("\n")
  }
  
  /** process command-line arguments and do as they request */
  def main(args: Array[String]) {
    def error1(msg: String) = out println ("scala: " + msg)
    val command = new InterpreterCommand(args.toList, error1)
    def neededHelp(): String =
      (if (command.settings.help.value) command.usageMsg + "\n" else "") +
      (if (command.settings.Xhelp.value) command.xusageMsg + "\n" else "")
    
    // if they asked for no help and command is valid, we call the real main
    neededHelp() match {
      case ""     => if (command.ok) main(command.settings) // else nothing
      case help   => plush(help)
    }
  }
}

