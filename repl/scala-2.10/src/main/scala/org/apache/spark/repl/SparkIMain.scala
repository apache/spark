// scalastyle:off

/* NSC -- new Scala compiler
 * Copyright 2005-2013 LAMP/EPFL
 * @author  Martin Odersky
 */

package org.apache.spark.repl

import java.io.File

import scala.tools.nsc._
import scala.tools.nsc.backend.JavaPlatform
import scala.tools.nsc.interpreter._

import Predef.{ println => _, _ }
import scala.tools.nsc.util.{MergedClassPath, stringFromWriter, ScalaClassLoader, stackTraceString}
import scala.reflect.internal.util._
import java.net.URL
import scala.sys.BooleanProp
import io.{AbstractFile, PlainFile, VirtualDirectory}

import reporters._
import symtab.Flags
import scala.reflect.internal.Names
import scala.tools.util.PathResolver
import ScalaClassLoader.URLClassLoader
import scala.tools.nsc.util.Exceptional.unwrap
import scala.collection.{ mutable, immutable }
import scala.util.control.Exception.{ ultimately }
import SparkIMain._
import java.util.concurrent.Future
import typechecker.Analyzer
import scala.language.implicitConversions
import scala.reflect.runtime.{ universe => ru }
import scala.reflect.{ ClassTag, classTag }
import scala.tools.reflect.StdRuntimeTags._
import scala.util.control.ControlThrowable

import org.apache.spark.{Logging, HttpServer, SecurityManager, SparkConf}
import org.apache.spark.util.Utils

// /** directory to save .class files to */
// private class ReplVirtualDirectory(out: JPrintWriter) extends VirtualDirectory("((memory))", None) {
//   private def pp(root: AbstractFile, indentLevel: Int) {
//     val spaces = "    " * indentLevel
//     out.println(spaces + root.name)
//     if (root.isDirectory)
//       root.toList sortBy (_.name) foreach (x => pp(x, indentLevel + 1))
//   }
//   // print the contents hierarchically
//   def show() = pp(this, 0)
// }

  /** An interpreter for Scala code.
   *
   *  The main public entry points are compile(), interpret(), and bind().
   *  The compile() method loads a complete Scala file.  The interpret() method
   *  executes one line of Scala code at the request of the user.  The bind()
   *  method binds an object to a variable that can then be used by later
   *  interpreted code.
   *
   *  The overall approach is based on compiling the requested code and then
   *  using a Java classloader and Java reflection to run the code
   *  and access its results.
   *
   *  In more detail, a single compiler instance is used
   *  to accumulate all successfully compiled or interpreted Scala code.  To
   *  "interpret" a line of code, the compiler generates a fresh object that
   *  includes the line of code and which has public member(s) to export
   *  all variables defined by that code.  To extract the result of an
   *  interpreted line to show the user, a second "result object" is created
   *  which imports the variables exported by the above object and then
   *  exports members called "$eval" and "$print". To accomodate user expressions
   *  that read from variables or methods defined in previous statements, "import"
   *  statements are used.
   *
   *  This interpreter shares the strengths and weaknesses of using the
   *  full compiler-to-Java.  The main strength is that interpreted code
   *  behaves exactly as does compiled code, including running at full speed.
   *  The main weakness is that redefining classes and methods is not handled
   *  properly, because rebinding at the Java level is technically difficult.
   *
   *  @author Moez A. Abdel-Gawad
   *  @author Lex Spoon
   */
  class SparkIMain(
      initialSettings: Settings,
      val out: JPrintWriter,
      propagateExceptions: Boolean = false)
    extends SparkImports with Logging { imain =>

    val conf = new SparkConf()

    val SPARK_DEBUG_REPL: Boolean = (System.getenv("SPARK_DEBUG_REPL") == "1")
    /** Local directory to save .class files too */
    lazy val outputDir = {
      val tmp = System.getProperty("java.io.tmpdir")
      val rootDir = conf.get("spark.repl.classdir",  tmp)
      Utils.createTempDir(rootDir)
    }
    if (SPARK_DEBUG_REPL) {
      echo("Output directory: " + outputDir)
    }

    val virtualDirectory                              = new PlainFile(outputDir) // "directory" for classfiles
    /** Jetty server that will serve our classes to worker nodes */
    val classServerPort                               = conf.getInt("spark.replClassServer.port", 0)
    val classServer                                   = new HttpServer(conf, outputDir, new SecurityManager(conf), classServerPort, "HTTP class server")
    private var currentSettings: Settings             = initialSettings
    var printResults                                  = true      // whether to print result lines
    var totalSilence                                  = false     // whether to print anything
    private var _initializeComplete                   = false     // compiler is initialized
    private var _isInitialized: Future[Boolean]       = null      // set up initialization future
    private var bindExceptions                        = true      // whether to bind the lastException variable
    private var _executionWrapper                     = ""        // code to be wrapped around all lines


    // Start the classServer and store its URI in a spark system property
    // (which will be passed to executors so that they can connect to it)
    classServer.start()
    if (SPARK_DEBUG_REPL) {
      echo("Class server started, URI = " + classServer.uri)
    }

    /** We're going to go to some trouble to initialize the compiler asynchronously.
     *  It's critical that nothing call into it until it's been initialized or we will
     *  run into unrecoverable issues, but the perceived repl startup time goes
     *  through the roof if we wait for it.  So we initialize it with a future and
     *  use a lazy val to ensure that any attempt to use the compiler object waits
     *  on the future.
     */
    private var _classLoader: AbstractFileClassLoader = null                              // active classloader
    private val _compiler: Global                     = newCompiler(settings, reporter)   // our private compiler

    private trait ExposeAddUrl extends URLClassLoader { def addNewUrl(url: URL) = this.addURL(url) }
    private var _runtimeClassLoader: URLClassLoader with ExposeAddUrl = null              // wrapper exposing addURL

    private val nextReqId = {
      var counter = 0
      () => { counter += 1 ; counter }
    }

    def compilerClasspath: Seq[URL] = (
      if (isInitializeComplete) global.classPath.asURLs
      else new PathResolver(settings).result.asURLs  // the compiler's classpath
      )
    def settings = currentSettings
    def mostRecentLine = prevRequestList match {
      case Nil      => ""
      case req :: _ => req.originalLine
    }
    // Run the code body with the given boolean settings flipped to true.
    def withoutWarnings[T](body: => T): T = beQuietDuring {
      val saved = settings.nowarn.value
      if (!saved)
        settings.nowarn.value = true

      try body
      finally if (!saved) settings.nowarn.value = false
    }

    /** construct an interpreter that reports to Console */
    def this(settings: Settings) = this(settings, new NewLinePrintWriter(new ConsoleWriter, true))
    def this() = this(new Settings())

    lazy val repllog: Logger = new Logger {
      val out: JPrintWriter = imain.out
      val isInfo: Boolean  = BooleanProp keyExists "scala.repl.info"
      val isDebug: Boolean = BooleanProp keyExists "scala.repl.debug"
      val isTrace: Boolean = BooleanProp keyExists "scala.repl.trace"
    }
    lazy val formatting: Formatting = new Formatting {
      val prompt = Properties.shellPromptString
    }
    lazy val reporter: ConsoleReporter = new SparkIMain.ReplReporter(this)

    import formatting._
    import reporter.{ printMessage, withoutTruncating }

    // This exists mostly because using the reporter too early leads to deadlock.
    private def echo(msg: String) { Console println msg }
    private def _initSources = List(new BatchSourceFile("<init>", "class $repl_$init { }"))
    private def _initialize() = {
      try {
        // todo. if this crashes, REPL will hang
        new _compiler.Run() compileSources _initSources
        _initializeComplete = true
        true
      }
      catch AbstractOrMissingHandler()
    }
    private def tquoted(s: String) = "\"\"\"" + s + "\"\"\""

    // argument is a thunk to execute after init is done
    def initialize(postInitSignal: => Unit) {
      synchronized {
        if (_isInitialized == null) {
          _isInitialized = io.spawn {
            try _initialize()
            finally postInitSignal
          }
        }
      }
    }
    def initializeSynchronous(): Unit = {
      if (!isInitializeComplete) {
        _initialize()
        assert(global != null, global)
      }
    }
    def isInitializeComplete = _initializeComplete

    /** the public, go through the future compiler */
    lazy val global: Global = {
      if (isInitializeComplete) _compiler
      else {
        // If init hasn't been called yet you're on your own.
        if (_isInitialized == null) {
          logWarning("Warning: compiler accessed before init set up.  Assuming no postInit code.")
          initialize(())
        }
        //       // blocks until it is ; false means catastrophic failure
        if (_isInitialized.get()) _compiler
        else null
      }
    }
    @deprecated("Use `global` for access to the compiler instance.", "2.9.0")
    lazy val compiler: global.type = global

    import global._
    import definitions.{ScalaPackage, JavaLangPackage, termMember, typeMember}
    import rootMirror.{RootClass, getClassIfDefined, getModuleIfDefined, getRequiredModule, getRequiredClass}

    implicit class ReplTypeOps(tp: Type) {
      def orElse(other: => Type): Type    = if (tp ne NoType) tp else other
      def andAlso(fn: Type => Type): Type = if (tp eq NoType) tp else fn(tp)
    }

    // TODO: If we try to make naming a lazy val, we run into big time
    // scalac unhappiness with what look like cycles.  It has not been easy to
    // reduce, but name resolution clearly takes different paths.
    object naming extends {
      val global: imain.global.type = imain.global
    } with Naming {
      // make sure we don't overwrite their unwisely named res3 etc.
      def freshUserTermName(): TermName = {
        val name = newTermName(freshUserVarName())
        if (definedNameMap contains name) freshUserTermName()
        else name
      }
      def isUserTermName(name: Name) = isUserVarName("" + name)
      def isInternalTermName(name: Name) = isInternalVarName("" + name)
    }
    import naming._

    object deconstruct extends {
      val global: imain.global.type = imain.global
    } with StructuredTypeStrings

    lazy val memberHandlers = new {
      val intp: imain.type = imain
    } with SparkMemberHandlers
    import memberHandlers._

    /** Temporarily be quiet */
    def beQuietDuring[T](body: => T): T = {
      val saved = printResults
      printResults = false
      try body
      finally printResults = saved
    }
    def beSilentDuring[T](operation: => T): T = {
      val saved = totalSilence
      totalSilence = true
      try operation
      finally totalSilence = saved
    }

    def quietRun[T](code: String) = beQuietDuring(interpret(code))


     private def logAndDiscard[T](label: String, alt: => T): PartialFunction[Throwable, T] = {
      case t: ControlThrowable => throw t
      case t: Throwable        =>
        logDebug(label + ": " + unwrap(t))
        logDebug(stackTraceString(unwrap(t)))
      alt
    }
    /** takes AnyRef because it may be binding a Throwable or an Exceptional */

    private def withLastExceptionLock[T](body: => T, alt: => T): T = {
      assert(bindExceptions, "withLastExceptionLock called incorrectly.")
      bindExceptions = false

      try     beQuietDuring(body)
      catch   logAndDiscard("withLastExceptionLock", alt)
      finally bindExceptions = true
    }

    def executionWrapper = _executionWrapper
    def setExecutionWrapper(code: String) = _executionWrapper = code
    def clearExecutionWrapper() = _executionWrapper = ""

    /** interpreter settings */
    lazy val isettings = new SparkISettings(this)

    /** Instantiate a compiler.  Overridable. */
    protected def newCompiler(settings: Settings, reporter: Reporter): ReplGlobal = {
      settings.outputDirs setSingleOutput virtualDirectory
      settings.exposeEmptyPackage.value = true
      new Global(settings, reporter) with ReplGlobal {
        override def toString: String = "<global>"
      }
    }

    /**
     * Adds any specified jars to the compile and runtime classpaths.
     *
     * @note Currently only supports jars, not directories
     * @param urls The list of items to add to the compile and runtime classpaths
     */
    def addUrlsToClassPath(urls: URL*): Unit = {
      new Run // Needed to force initialization of "something" to correctly load Scala classes from jars
      urls.foreach(_runtimeClassLoader.addNewUrl) // Add jars/classes to runtime for execution
      updateCompilerClassPath(urls: _*)           // Add jars/classes to compile time for compiling
    }

    protected def updateCompilerClassPath(urls: URL*): Unit = {
      require(!global.forMSIL) // Only support JavaPlatform

      val platform = global.platform.asInstanceOf[JavaPlatform]

      val newClassPath = mergeUrlsIntoClassPath(platform, urls: _*)

      // NOTE: Must use reflection until this is exposed/fixed upstream in Scala
      val fieldSetter = platform.getClass.getMethods
        .find(_.getName.endsWith("currentClassPath_$eq")).get
      fieldSetter.invoke(platform, Some(newClassPath))

      // Reload all jars specified into our compiler
      global.invalidateClassPathEntries(urls.map(_.getPath): _*)
    }

    protected def mergeUrlsIntoClassPath(platform: JavaPlatform, urls: URL*): MergedClassPath[AbstractFile] = {
      // Collect our new jars/directories and add them to the existing set of classpaths
      val allClassPaths = (
        platform.classPath.asInstanceOf[MergedClassPath[AbstractFile]].entries ++
        urls.map(url => {
          platform.classPath.context.newClassPath(
            if (url.getProtocol == "file") {
              val f = new File(url.getPath)
              if (f.isDirectory)
                io.AbstractFile.getDirectory(f)
              else
                io.AbstractFile.getFile(f)
            } else {
              io.AbstractFile.getURL(url)
            }
          )
        })
      ).distinct

      // Combine all of our classpaths (old and new) into one merged classpath
      new MergedClassPath(allClassPaths, platform.classPath.context)
    }

    /** Parent classloader.  Overridable. */
    protected def parentClassLoader: ClassLoader =
      SparkHelper.explicitParentLoader(settings).getOrElse( this.getClass.getClassLoader() )

    /* A single class loader is used for all commands interpreted by this Interpreter.
     It would also be possible to create a new class loader for each command
     to interpret.  The advantages of the current approach are:

    - Expressions are only evaluated one time.  This is especially
    significant for I/O, e.g. "val x = Console.readLine"

    The main disadvantage is:

    - Objects, classes, and methods cannot be rebound.  Instead, definitions
    shadow the old ones, and old code objects refer to the old
    definitions.
    */
    def resetClassLoader() = {
      logDebug("Setting new classloader: was " + _classLoader)
      _classLoader = null
      ensureClassLoader()
    }
    final def ensureClassLoader() {
      if (_classLoader == null)
        _classLoader = makeClassLoader()
    }
    def classLoader: AbstractFileClassLoader = {
      ensureClassLoader()
      _classLoader
    }
    private class TranslatingClassLoader(parent: ClassLoader) extends AbstractFileClassLoader(virtualDirectory, parent) {
      /** Overridden here to try translating a simple name to the generated
       *  class name if the original attempt fails.  This method is used by
       *  getResourceAsStream as well as findClass.
       */
      override protected def findAbstractFile(name: String): AbstractFile = {
        super.findAbstractFile(name) match {
          // deadlocks on startup if we try to translate names too early
          case null if isInitializeComplete =>
            generatedName(name) map (x => super.findAbstractFile(x)) orNull
          case file                         =>
            file
        }
      }
    }
    private def makeClassLoader(): AbstractFileClassLoader =
      new TranslatingClassLoader(parentClassLoader match {
        case null   => ScalaClassLoader fromURLs compilerClasspath
        case p      =>
          _runtimeClassLoader = new URLClassLoader(compilerClasspath, p) with ExposeAddUrl
          _runtimeClassLoader
      })

    def getInterpreterClassLoader() = classLoader

    // Set the current Java "context" class loader to this interpreter's class loader
    def setContextClassLoader() = classLoader.setAsContext()

    /** Given a simple repl-defined name, returns the real name of
     *  the class representing it, e.g. for "Bippy" it may return
     *  {{{
     *    $line19.$read$$iw$$iw$$iw$$iw$$iw$$iw$$iw$$iw$Bippy
     *  }}}
     */
    def generatedName(simpleName: String): Option[String] = {
      if (simpleName endsWith nme.MODULE_SUFFIX_STRING) optFlatName(simpleName.init) map (_ + nme.MODULE_SUFFIX_STRING)
      else optFlatName(simpleName)
    }
    def flatName(id: String)    = optFlatName(id) getOrElse id
    def optFlatName(id: String) = requestForIdent(id) map (_ fullFlatName id)

    def allDefinedNames = definedNameMap.keys.toList.sorted
    def pathToType(id: String): String = pathToName(newTypeName(id))
    def pathToTerm(id: String): String = pathToName(newTermName(id))
    def pathToName(name: Name): String = {
      if (definedNameMap contains name)
        definedNameMap(name) fullPath name
      else name.toString
    }

    /** Most recent tree handled which wasn't wholly synthetic. */
    private def mostRecentlyHandledTree: Option[Tree] = {
      prevRequests.reverse foreach { req =>
        req.handlers.reverse foreach {
          case x: MemberDefHandler if x.definesValue && !isInternalTermName(x.name) => return Some(x.member)
          case _ => ()
        }
      }
      None
    }

    /** Stubs for work in progress. */
    def handleTypeRedefinition(name: TypeName, old: Request, req: Request) = {
      for (t1 <- old.simpleNameOfType(name) ; t2 <- req.simpleNameOfType(name)) {
        logDebug("Redefining type '%s'\n  %s -> %s".format(name, t1, t2))
      }
    }

    def handleTermRedefinition(name: TermName, old: Request, req: Request) = {
      for (t1 <- old.compilerTypeOf get name ; t2 <- req.compilerTypeOf get name) {
    //    Printing the types here has a tendency to cause assertion errors, like
        //   assertion failed: fatal: <refinement> has owner value x, but a class owner is required
        // so DBG is by-name now to keep it in the family.  (It also traps the assertion error,
        // but we don't want to unnecessarily risk hosing the compiler's internal state.)
        logDebug("Redefining term '%s'\n  %s -> %s".format(name, t1, t2))
      }
    }

    def recordRequest(req: Request) {
      if (req == null || referencedNameMap == null)
        return

      prevRequests += req
      req.referencedNames foreach (x => referencedNameMap(x) = req)

      // warning about serially defining companions.  It'd be easy
      // enough to just redefine them together but that may not always
      // be what people want so I'm waiting until I can do it better.
      for {
        name   <- req.definedNames filterNot (x => req.definedNames contains x.companionName)
        oldReq <- definedNameMap get name.companionName
        newSym <- req.definedSymbols get name
        oldSym <- oldReq.definedSymbols get name.companionName
        if Seq(oldSym, newSym).permutations exists { case Seq(s1, s2) => s1.isClass && s2.isModule }
      } {
        afterTyper(replwarn(s"warning: previously defined $oldSym is not a companion to $newSym."))
        replwarn("Companions must be defined together; you may wish to use :paste mode for this.")
      }

      // Updating the defined name map
      req.definedNames foreach { name =>
        if (definedNameMap contains name) {
          if (name.isTypeName) handleTypeRedefinition(name.toTypeName, definedNameMap(name), req)
          else handleTermRedefinition(name.toTermName, definedNameMap(name), req)
        }
         definedNameMap(name) = req
      }
    }

    def replwarn(msg: => String) {
      if (!settings.nowarnings.value)
        printMessage(msg)
    }

    def isParseable(line: String): Boolean = {
      beSilentDuring {
        try parse(line) match {
          case Some(xs) => xs.nonEmpty  // parses as-is
          case None     => true         // incomplete
        }
        catch { case x: Exception =>    // crashed the compiler
          replwarn("Exception in isParseable(\"" + line + "\"): " + x)
           false
         }
      }
    }

    def compileSourcesKeepingRun(sources: SourceFile*) = {
      val run = new Run()
      reporter.reset()
      run compileSources sources.toList
      (!reporter.hasErrors, run)
    }

    /** Compile an nsc SourceFile.  Returns true if there are
     *  no compilation errors, or false otherwise.
     */
    def compileSources(sources: SourceFile*): Boolean =
      compileSourcesKeepingRun(sources: _*)._1

    /** Compile a string.  Returns true if there are no
     *  compilation errors, or false otherwise.
     */
    def compileString(code: String): Boolean =
      compileSources(new BatchSourceFile("<script>", code))

    /** Build a request from the user. `trees` is `line` after being parsed.
     */
    private def buildRequest(line: String, trees: List[Tree]): Request = {
      executingRequest = new Request(line, trees)
      executingRequest
    }

  // rewriting "5 // foo" to "val x = { 5 // foo }" creates broken code because
  // the close brace is commented out.  Strip single-line comments.
  // ... but for error message output reasons this is not used, and rather than
  // enclosing in braces it is constructed like "val x =\n5 // foo".
  private def removeComments(line: String): String = {
    showCodeIfDebugging(line) // as we're about to lose our // show
    line.lines map (s => s indexOf "//" match {
      case -1   => s
      case idx  => s take idx
    }) mkString "\n"
  }

  private def safePos(t: Tree, alt: Int): Int =
    try t.pos.startOrPoint
  catch { case _: UnsupportedOperationException => alt }

  // Given an expression like 10 * 10 * 10 we receive the parent tree positioned
  // at a '*'.  So look at each subtree and find the earliest of all positions.
  private def earliestPosition(tree: Tree): Int = {
    var pos = Int.MaxValue
    tree foreach { t =>
      pos = math.min(pos, safePos(t, Int.MaxValue))
    }
    pos
  }


  private def requestFromLine(line: String, synthetic: Boolean): Either[IR.Result, Request] = {
    val content = indentCode(line)
    val trees = parse(content) match {
      case None         => return Left(IR.Incomplete)
      case Some(Nil)    => return Left(IR.Error) // parse error or empty input
      case Some(trees)  => trees
    }
    logDebug(
      trees map (t => {
        // [Eugene to Paul] previously it just said `t map ...`
        // because there was an implicit conversion from Tree to a list of Trees
        // however Martin and I have removed the conversion
        // (it was conflicting with the new reflection API),
        // so I had to rewrite this a bit
        val subs = t collect { case sub => sub }
        subs map (t0 =>
          "  " + safePos(t0, -1) + ": " + t0.shortClass + "\n"
                ) mkString ""
      }) mkString "\n"
    )
    // If the last tree is a bare expression, pinpoint where it begins using the
    // AST node position and snap the line off there.  Rewrite the code embodied
    // by the last tree as a ValDef instead, so we can access the value.
    trees.last match {
      case _:Assign                        => // we don't want to include assignments
        case _:TermTree | _:Ident | _:Select => // ... but do want other unnamed terms.
          val varName  = if (synthetic) freshInternalVarName() else freshUserVarName()
      val rewrittenLine = (
        // In theory this would come out the same without the 1-specific test, but
        // it's a cushion against any more sneaky parse-tree position vs. code mismatches:
        // this way such issues will only arise on multiple-statement repl input lines,
        // which most people don't use.
        if (trees.size == 1) "val " + varName + " =\n" + content
        else {
          // The position of the last tree
          val lastpos0 = earliestPosition(trees.last)
          // Oh boy, the parser throws away parens so "(2+2)" is mispositioned,
          // with increasingly hard to decipher positions as we move on to "() => 5",
          // (x: Int) => x + 1, and more.  So I abandon attempts to finesse and just
          // look for semicolons and newlines, which I'm sure is also buggy.
          val (raw1, raw2) = content splitAt lastpos0
          logDebug("[raw] " + raw1 + "   <--->   " + raw2)

          val adjustment = (raw1.reverse takeWhile (ch => (ch != ';') && (ch != '\n'))).size
          val lastpos = lastpos0 - adjustment

          // the source code split at the laboriously determined position.
          val (l1, l2) = content splitAt lastpos
          logDebug("[adj] " + l1 + "   <--->   " + l2)

          val prefix   = if (l1.trim == "") "" else l1 + ";\n"
          // Note to self: val source needs to have this precise structure so that
          // error messages print the user-submitted part without the "val res0 = " part.
          val combined   = prefix + "val " + varName + " =\n" + l2

          logDebug(List(
            "    line" -> line,
            " content" -> content,
            "     was" -> l2,
            "combined" -> combined) map {
              case (label, s) => label + ": '" + s + "'"
            } mkString "\n"
          )
          combined
        }
      )
        // Rewriting    "foo ; bar ; 123"
        // to           "foo ; bar ; val resXX = 123"
        requestFromLine(rewrittenLine, synthetic) match {
          case Right(req) => return Right(req withOriginalLine line)
          case x          => return x
        }
      case _ =>
    }
    Right(buildRequest(line, trees))
  }

  // normalize non-public types so we don't see protected aliases like Self
  def normalizeNonPublic(tp: Type) = tp match {
    case TypeRef(_, sym, _) if sym.isAliasType && !sym.isPublic => tp.dealias
    case _                                                      => tp
  }

  /**
   *  Interpret one line of input. All feedback, including parse errors
   *  and evaluation results, are printed via the supplied compiler's
   *  reporter. Values defined are available for future interpreted strings.
   *
   *  The return value is whether the line was interpreter successfully,
   *  e.g. that there were no parse errors.
   */
  def interpret(line: String): IR.Result = interpret(line, false)
  def interpretSynthetic(line: String): IR.Result = interpret(line, true)
  def interpret(line: String, synthetic: Boolean): IR.Result = {
    def loadAndRunReq(req: Request) = {
      classLoader.setAsContext()
      val (result, succeeded) = req.loadAndRun

      /** To our displeasure, ConsoleReporter offers only printMessage,
       *  which tacks a newline on the end.  Since that breaks all the
       *  output checking, we have to take one off to balance.
       */
      if (succeeded) {
        if (printResults && result != "")
          printMessage(result stripSuffix "\n")
        else if (isReplDebug) // show quiet-mode activity
          printMessage(result.trim.lines map ("[quiet] " + _) mkString "\n")

        // Book-keeping.  Have to record synthetic requests too,
        // as they may have been issued for information, e.g. :type
        recordRequest(req)
        IR.Success
      }
        else {
          // don't truncate stack traces
          withoutTruncating(printMessage(result))
          IR.Error
        }
    }

    if (global == null) IR.Error
    else requestFromLine(line, synthetic) match {
      case Left(result) => result
      case Right(req)   =>
        // null indicates a disallowed statement type; otherwise compile and
        // fail if false (implying e.g. a type error)
        if (req == null || !req.compile) IR.Error
        else loadAndRunReq(req)
    }
  }

  /** Bind a specified name to a specified value.  The name may
   *  later be used by expressions passed to interpret.
   *
   *  @param name      the variable name to bind
   *  @param boundType the type of the variable, as a string
   *  @param value     the object value to bind to it
   *  @return          an indication of whether the binding succeeded
   */
  def bind(name: String, boundType: String, value: Any, modifiers: List[String] = Nil): IR.Result = {
    val bindRep = new ReadEvalPrint()
    val run = bindRep.compile("""
                              |object %s {
                                |  var value: %s = _
                              |  def set(x: Any) = value = x.asInstanceOf[%s]
                              |}
                              """.stripMargin.format(bindRep.evalName, boundType, boundType)
                            )
    bindRep.callEither("set", value) match {
      case Left(ex) =>
        logDebug("Set failed in bind(%s, %s, %s)".format(name, boundType, value))
        logDebug(util.stackTraceString(ex))
        IR.Error

      case Right(_) =>
        val line = "%sval %s = %s.value".format(modifiers map (_ + " ") mkString, name, bindRep.evalPath)
      logDebug("Interpreting: " + line)
      interpret(line)
    }
  }
  def directBind(name: String, boundType: String, value: Any): IR.Result = {
    val result = bind(name, boundType, value)
    if (result == IR.Success)
      directlyBoundNames += newTermName(name)
    result
  }
  def directBind(p: NamedParam): IR.Result                                    = directBind(p.name, p.tpe, p.value)
  def directBind[T: ru.TypeTag : ClassTag](name: String, value: T): IR.Result = directBind((name, value))

  def rebind(p: NamedParam): IR.Result = {
    val name     = p.name
    val oldType  = typeOfTerm(name) orElse { return IR.Error }
    val newType  = p.tpe
    val tempName = freshInternalVarName()

    quietRun("val %s = %s".format(tempName, name))
    quietRun("val %s = %s.asInstanceOf[%s]".format(name, tempName, newType))
  }
  def quietImport(ids: String*): IR.Result = beQuietDuring(addImports(ids: _*))
  def addImports(ids: String*): IR.Result =
    if (ids.isEmpty) IR.Success
    else interpret("import " + ids.mkString(", "))

  def quietBind(p: NamedParam): IR.Result                               = beQuietDuring(bind(p))
  def bind(p: NamedParam): IR.Result                                    = bind(p.name, p.tpe, p.value)
  def bind[T: ru.TypeTag : ClassTag](name: String, value: T): IR.Result = bind((name, value))
  def bindSyntheticValue(x: Any): IR.Result                             = bindValue(freshInternalVarName(), x)
  def bindValue(x: Any): IR.Result                                      = bindValue(freshUserVarName(), x)
  def bindValue(name: String, x: Any): IR.Result                        = bind(name, TypeStrings.fromValue(x), x)

  /** Reset this interpreter, forgetting all user-specified requests. */
  def reset() {
    clearExecutionWrapper()
    resetClassLoader()
    resetAllCreators()
    prevRequests.clear()
    referencedNameMap.clear()
    definedNameMap.clear()
    virtualDirectory.delete()
    virtualDirectory.create()
  }

  /** This instance is no longer needed, so release any resources
   *  it is using.  The reporter's output gets flushed.
   */
  def close() {
    reporter.flush()
    classServer.stop()
  }

  /**
   * Captures the session names (which are set by system properties) once, instead of for each line.
   */
  object FixedSessionNames {
    val lineName    = sessionNames.line
    val readName    = sessionNames.read
    val evalName    = sessionNames.eval
    val printName   = sessionNames.print
    val resultName  = sessionNames.result
  }

  /** Here is where we:
   *
   *  1) Read some source code, and put it in the "read" object.
   *  2) Evaluate the read object, and put the result in the "eval" object.
   *  3) Create a String for human consumption, and put it in the "print" object.
   *
   *  Read! Eval! Print! Some of that not yet centralized here.
   */
  class ReadEvalPrint(val lineId: Int) {
    def this() = this(freshLineId())

    private var lastRun: Run = _
    private var evalCaught: Option[Throwable] = None
    private var conditionalWarnings: List[ConditionalWarning] = Nil

    val packageName = FixedSessionNames.lineName + lineId
    val readName    = FixedSessionNames.readName
    val evalName    = FixedSessionNames.evalName
    val printName   = FixedSessionNames.printName
    val resultName  = FixedSessionNames.resultName

    def bindError(t: Throwable) = {
      // Immediately throw the exception if we are asked to propagate them
      if (propagateExceptions) {
        throw unwrap(t)
      }
      if (!bindExceptions) // avoid looping if already binding
        throw t

      val unwrapped = unwrap(t)
      withLastExceptionLock[String]({
        directBind[Throwable]("lastException", unwrapped)(tagOfThrowable, classTag[Throwable])
        util.stackTraceString(unwrapped)
      }, util.stackTraceString(unwrapped))
    }

    // TODO: split it out into a package object and a regular
    // object and we can do that much less wrapping.
    def packageDecl = "package " + packageName

    def pathTo(name: String)   = packageName + "." + name
    def packaged(code: String) = packageDecl + "\n\n" + code

    def readPath  = pathTo(readName)
    def evalPath  = pathTo(evalName)
    def printPath = pathTo(printName)

    def call(name: String, args: Any*): AnyRef = {
      val m = evalMethod(name)
      logDebug("Invoking: " + m)
      if (args.nonEmpty)
        logDebug("  with args: " + args.mkString(", "))

      m.invoke(evalClass, args.map(_.asInstanceOf[AnyRef]): _*)
    }

    def callEither(name: String, args: Any*): Either[Throwable, AnyRef] =
      try Right(call(name, args: _*))
    catch { case ex: Throwable => Left(ex) }

    def callOpt(name: String, args: Any*): Option[AnyRef] =
      try Some(call(name, args: _*))
    catch { case ex: Throwable => bindError(ex) ; None }

    class EvalException(msg: String, cause: Throwable) extends RuntimeException(msg, cause) { }

    private def evalError(path: String, ex: Throwable) =
      throw new EvalException("Failed to load '" + path + "': " + ex.getMessage, ex)

    private def load(path: String): Class[_] = {
      try Class.forName(path, true, classLoader)
      catch { case ex: Throwable => evalError(path, unwrap(ex)) }
    }

    lazy val evalClass = load(evalPath)
    lazy val evalValue = callEither(resultName) match {
      case Left(ex)      => evalCaught = Some(ex) ; None
      case Right(result) => Some(result)
    }

    def compile(source: String): Boolean = compileAndSaveRun("<console>", source)

    /** The innermost object inside the wrapper, found by
     * following accessPath into the outer one.
     */
    def resolvePathToSymbol(accessPath: String): Symbol = {
      // val readRoot  = getRequiredModule(readPath)   // the outermost wrapper
      // MATEI: Changed this to getClass because the root object is no longer a module (Scala singleton object)

      val readRoot  = rootMirror.getClassByName(newTypeName(readPath))   // the outermost wrapper
      (accessPath split '.').foldLeft(readRoot: Symbol) {
        case (sym, "")    => sym
        case (sym, name)  => afterTyper(termMember(sym, name))
      }
    }
    /** We get a bunch of repeated warnings for reasons I haven't
     *  entirely figured out yet.  For now, squash.
     */
    private def updateRecentWarnings(run: Run) {
      def loop(xs: List[(Position, String)]): List[(Position, String)] = xs match {
        case Nil                  => Nil
        case ((pos, msg)) :: rest =>
          val filtered = rest filter { case (pos0, msg0) =>
            (msg != msg0) || (pos.lineContent.trim != pos0.lineContent.trim) || {
              // same messages and same line content after whitespace removal
              // but we want to let through multiple warnings on the same line
              // from the same run.  The untrimmed line will be the same since
              // there's no whitespace indenting blowing it.
              (pos.lineContent == pos0.lineContent)
            }
                                    }
        ((pos, msg)) :: loop(filtered)
      }
     // PRASHANT: This leads to a NoSuchMethodError for _.warnings. Yet to figure out its purpose.
      // val warnings = loop(run.allConditionalWarnings flatMap (_.warnings))
      // if (warnings.nonEmpty)
      //   mostRecentWarnings = warnings
    }
    private def evalMethod(name: String) = evalClass.getMethods filter (_.getName == name) match {
      case Array(method) => method
      case xs            => sys.error("Internal error: eval object " + evalClass + ", " + xs.mkString("\n", "\n", ""))
    }
    private def compileAndSaveRun(label: String, code: String) = {
      showCodeIfDebugging(code)
      val (success, run) = compileSourcesKeepingRun(new BatchSourceFile(label, packaged(code)))
      updateRecentWarnings(run)
      lastRun = run
      success
    }
  }

  /** One line of code submitted by the user for interpretation */
  // private
  class Request(val line: String, val trees: List[Tree]) {
    val reqId = nextReqId()
    val lineRep = new ReadEvalPrint()

    private var _originalLine: String = null
    def withOriginalLine(s: String): this.type = { _originalLine = s ; this }
    def originalLine = if (_originalLine == null) line else _originalLine

    /** handlers for each tree in this request */
    val handlers: List[MemberHandler] = trees map (memberHandlers chooseHandler _)
    def defHandlers = handlers collect { case x: MemberDefHandler => x }

    /** all (public) names defined by these statements */
    val definedNames = handlers flatMap (_.definedNames)

    /** list of names used by this expression */
    val referencedNames: List[Name] = handlers flatMap (_.referencedNames)

    /** def and val names */
    def termNames = handlers flatMap (_.definesTerm)
    def typeNames = handlers flatMap (_.definesType)
    def definedOrImported = handlers flatMap (_.definedOrImported)
    def definedSymbolList = defHandlers flatMap (_.definedSymbols)

    def definedTypeSymbol(name: String) = definedSymbols(newTypeName(name))
    def definedTermSymbol(name: String) = definedSymbols(newTermName(name))

    val definedClasses = handlers.exists {
      case _: ClassHandler => true
      case _ => false
    }

    /** Code to import bound names from previous lines - accessPath is code to
     * append to objectName to access anything bound by request.
     */
    val SparkComputedImports(importsPreamble, importsTrailer, accessPath) =
      importsCode(referencedNames.toSet, definedClasses)

    /** Code to access a variable with the specified name */
    def fullPath(vname: String) = {
      // lineRep.readPath + accessPath + ".`%s`".format(vname)
      lineRep.readPath + ".INSTANCE" + accessPath + ".`%s`".format(vname)
    }
      /** Same as fullpath, but after it has been flattened, so:
       *  $line5.$iw.$iw.$iw.Bippy      // fullPath
       *  $line5.$iw$$iw$$iw$Bippy      // fullFlatName
       */
      def fullFlatName(name: String) =
        // lineRep.readPath + accessPath.replace('.', '$') + nme.NAME_JOIN_STRING + name
        lineRep.readPath + ".INSTANCE" + accessPath.replace('.', '$') + nme.NAME_JOIN_STRING + name

    /** The unmangled symbol name, but supplemented with line info. */
    def disambiguated(name: Name): String = name + " (in " + lineRep + ")"

    /** Code to access a variable with the specified name */
    def fullPath(vname: Name): String = fullPath(vname.toString)

    /** the line of code to compute */
    def toCompute = line

    /** generate the source code for the object that computes this request */
    private object ObjectSourceCode extends CodeAssembler[MemberHandler] {
      def path = pathToTerm("$intp")
      def envLines = {
        if (!isReplPower) Nil // power mode only for now
        // $intp is not bound; punt, but include the line.
        else if (path == "$intp") List(
          "def $line = " + tquoted(originalLine),
          "def $trees = Nil"
        )
        else List(
          "def $line  = " + tquoted(originalLine),
          "def $req = %s.requestForReqId(%s).orNull".format(path, reqId),
          "def $trees = if ($req eq null) Nil else $req.trees".format(lineRep.readName, path, reqId)
        )
      }

      val preamble = """
        |class %s extends Serializable {
        |  %s%s%s
      """.stripMargin.format(lineRep.readName, envLines.map("  " + _ + ";\n").mkString, importsPreamble, indentCode(toCompute))
      val postamble = importsTrailer + "\n}" + "\n" +
        "object " + lineRep.readName + " {\n" +
        "  val INSTANCE = new " + lineRep.readName + "();\n" +
        "}\n"
      val generate = (m: MemberHandler) => m extraCodeToEvaluate Request.this

      /*
      val preamble = """
        |object %s extends Serializable {
        |%s%s%s
      """.stripMargin.format(lineRep.readName, envLines.map("  " + _ + ";\n").mkString, importsPreamble, indentCode(toCompute))
      val postamble = importsTrailer + "\n}"
      val generate = (m: MemberHandler) => m extraCodeToEvaluate Request.this
      */

    }

    private object ResultObjectSourceCode extends CodeAssembler[MemberHandler] {
      /** We only want to generate this code when the result
       *  is a value which can be referred to as-is.
       */
      val evalResult =
        if (!handlers.last.definesValue) ""
        else handlers.last.definesTerm match {
          case Some(vname) if typeOf contains vname =>
            "lazy val %s = %s".format(lineRep.resultName, fullPath(vname))
          case _  => ""
        }

      // first line evaluates object to make sure constructor is run
      // initial "" so later code can uniformly be: + etc
      val preamble = """
      |object %s {
      |  %s
      |  val %s: String = %s {
      |    %s
      |    (""
      """.stripMargin.format(
        lineRep.evalName, evalResult, lineRep.printName,
        executionWrapper, lineRep.readName + ".INSTANCE" + accessPath
      )
      val postamble = """
      |    )
      |  }
      |}
      """.stripMargin
      val generate = (m: MemberHandler) => m resultExtractionCode Request.this
    }

    // get it
    def getEvalTyped[T] : Option[T] = getEval map (_.asInstanceOf[T])
    def getEval: Option[AnyRef] = {
      // ensure it has been compiled
      compile
      // try to load it and call the value method
      lineRep.evalValue filterNot (_ == null)
    }

    /** Compile the object file.  Returns whether the compilation succeeded.
     *  If all goes well, the "types" map is computed. */
    lazy val compile: Boolean = {
      // error counting is wrong, hence interpreter may overlook failure - so we reset
      reporter.reset()

      // compile the object containing the user's code
      lineRep.compile(ObjectSourceCode(handlers)) && {
        // extract and remember types
        typeOf
        typesOfDefinedTerms

        // Assign symbols to the original trees
        // TODO - just use the new trees.
        defHandlers foreach { dh =>
          val name = dh.member.name
          definedSymbols get name foreach { sym =>
            dh.member setSymbol sym
           logDebug("Set symbol of " + name + " to " + sym.defString)
          }
        }

        // compile the result-extraction object
        withoutWarnings(lineRep compile ResultObjectSourceCode(handlers))
      }
    }

    lazy val resultSymbol = lineRep.resolvePathToSymbol(accessPath)
    def applyToResultMember[T](name: Name, f: Symbol => T) = afterTyper(f(resultSymbol.info.nonPrivateDecl(name)))

    /* typeOf lookup with encoding */
    def lookupTypeOf(name: Name) = typeOf.getOrElse(name, typeOf(global.encode(name.toString)))
    def simpleNameOfType(name: TypeName) = (compilerTypeOf get name) map (_.typeSymbol.simpleName)

    private def typeMap[T](f: Type => T) =
      mapFrom[Name, Name, T](termNames ++ typeNames)(x => f(cleanMemberDecl(resultSymbol, x)))

    /** Types of variables defined by this request. */
    lazy val compilerTypeOf = typeMap[Type](x => x) withDefaultValue NoType
    /** String representations of same. */
    lazy val typeOf         = typeMap[String](tp => afterTyper(tp.toString))

    // lazy val definedTypes: Map[Name, Type] = {
    //   typeNames map (x => x -> afterTyper(resultSymbol.info.nonPrivateDecl(x).tpe)) toMap
    // }
    lazy val definedSymbols = (
      termNames.map(x => x -> applyToResultMember(x, x => x)) ++
      typeNames.map(x => x -> compilerTypeOf(x).typeSymbolDirect)
    ).toMap[Name, Symbol] withDefaultValue NoSymbol

    lazy val typesOfDefinedTerms = mapFrom[Name, Name, Type](termNames)(x => applyToResultMember(x, _.tpe))

    /** load and run the code using reflection */
    def loadAndRun: (String, Boolean) = {
      try   { ("" + (lineRep call sessionNames.print), true) }
      catch { case ex: Throwable => (lineRep.bindError(ex), false) }
    }

    override def toString = "Request(line=%s, %s trees)".format(line, trees.size)
  }

  /** Returns the name of the most recent interpreter result.
   *  Mostly this exists so you can conveniently invoke methods on
   *  the previous result.
   */
  def mostRecentVar: String =
    if (mostRecentlyHandledTree.isEmpty) ""
    else "" + (mostRecentlyHandledTree.get match {
      case x: ValOrDefDef           => x.name
      case Assign(Ident(name), _)   => name
      case ModuleDef(_, name, _)    => name
      case _                        => naming.mostRecentVar
    })

  private var mostRecentWarnings: List[(global.Position, String)] = Nil
  def lastWarnings = mostRecentWarnings

  def treesForRequestId(id: Int): List[Tree] =
    requestForReqId(id).toList flatMap (_.trees)

  def requestForReqId(id: Int): Option[Request] =
    if (executingRequest != null && executingRequest.reqId == id) Some(executingRequest)
    else prevRequests find (_.reqId == id)

  def requestForName(name: Name): Option[Request] = {
    assert(definedNameMap != null, "definedNameMap is null")
    definedNameMap get name
  }

  def requestForIdent(line: String): Option[Request] =
    requestForName(newTermName(line)) orElse requestForName(newTypeName(line))

  def requestHistoryForName(name: Name): List[Request] =
    prevRequests.toList.reverse filter (_.definedNames contains name)


  def definitionForName(name: Name): Option[MemberHandler] =
    requestForName(name) flatMap { req =>
      req.handlers find (_.definedNames contains name)
    }

  def valueOfTerm(id: String): Option[AnyRef] =
    requestForName(newTermName(id)) flatMap (_.getEval)

  def classOfTerm(id: String): Option[JClass] =
    valueOfTerm(id) map (_.getClass)

  def typeOfTerm(id: String): Type = newTermName(id) match {
    case nme.ROOTPKG  => RootClass.tpe
    case name         => requestForName(name).fold(NoType: Type)(_ compilerTypeOf name)
  }

  def symbolOfType(id: String): Symbol =
    requestForName(newTypeName(id)).fold(NoSymbol: Symbol)(_ definedTypeSymbol id)

  def symbolOfTerm(id: String): Symbol =
    requestForIdent(newTermName(id)).fold(NoSymbol: Symbol)(_ definedTermSymbol id)

  def runtimeClassAndTypeOfTerm(id: String): Option[(JClass, Type)] = {
    classOfTerm(id) flatMap { clazz =>
      new RichClass(clazz).supers find(c => !(new RichClass(c).isScalaAnonymous)) map { nonAnon =>
        (nonAnon, runtimeTypeOfTerm(id))
      }
    }
  }

  def runtimeTypeOfTerm(id: String): Type = {
    typeOfTerm(id) andAlso { tpe =>
      val clazz      = classOfTerm(id) getOrElse { return NoType }
      val staticSym  = tpe.typeSymbol
      val runtimeSym = getClassIfDefined(clazz.getName)

      if ((runtimeSym != NoSymbol) && (runtimeSym != staticSym) && (runtimeSym isSubClass staticSym))
        runtimeSym.info
      else NoType
    }
  }
  def cleanMemberDecl(owner: Symbol, member: Name): Type = afterTyper {
    normalizeNonPublic {
      owner.info.nonPrivateDecl(member).tpe match {
        case NullaryMethodType(tp) => tp
        case tp                    => tp
      }
    }
  }

  object exprTyper extends {
    val repl: SparkIMain.this.type = imain
  } with SparkExprTyper { }

  def parse(line: String): Option[List[Tree]] = exprTyper.parse(line)

  def symbolOfLine(code: String): Symbol =
    exprTyper.symbolOfLine(code)

  def typeOfExpression(expr: String, silent: Boolean = true): Type =
    exprTyper.typeOfExpression(expr, silent)

  protected def onlyTerms(xs: List[Name]) = xs collect { case x: TermName => x }
  protected def onlyTypes(xs: List[Name]) = xs collect { case x: TypeName => x }

  def definedTerms      = onlyTerms(allDefinedNames) filterNot isInternalTermName
  def definedTypes      = onlyTypes(allDefinedNames)
  def definedSymbols    = prevRequestList.flatMap(_.definedSymbols.values).toSet[Symbol]
  def definedSymbolList = prevRequestList flatMap (_.definedSymbolList) filterNot (s => isInternalTermName(s.name))

  // Terms with user-given names (i.e. not res0 and not synthetic)
    def namedDefinedTerms = definedTerms filterNot (x => isUserVarName("" + x) || directlyBoundNames(x))

  private def findName(name: Name) = definedSymbols find (_.name == name) getOrElse NoSymbol

  /** Translate a repl-defined identifier into a Symbol.
   */
  def apply(name: String): Symbol =
    types(name) orElse terms(name)

  def types(name: String): Symbol = {
    val tpname = newTypeName(name)
    findName(tpname) orElse getClassIfDefined(tpname)
  }
  def terms(name: String): Symbol = {
    val termname = newTypeName(name)
    findName(termname) orElse getModuleIfDefined(termname)
  }
  // [Eugene to Paul] possibly you could make use of TypeTags here
  def types[T: ClassTag] : Symbol = types(classTag[T].runtimeClass.getName)
  def terms[T: ClassTag] : Symbol = terms(classTag[T].runtimeClass.getName)
  def apply[T: ClassTag] : Symbol = apply(classTag[T].runtimeClass.getName)

  def classSymbols  = allDefSymbols collect { case x: ClassSymbol => x }
  def methodSymbols = allDefSymbols collect { case x: MethodSymbol => x }

  /** the previous requests this interpreter has processed */
  private var executingRequest: Request = _
  private val prevRequests       = mutable.ListBuffer[Request]()
  private val referencedNameMap  = mutable.Map[Name, Request]()
  private val definedNameMap     = mutable.Map[Name, Request]()
  private val directlyBoundNames = mutable.Set[Name]()

  def allHandlers    = prevRequestList flatMap (_.handlers)
  def allDefHandlers = allHandlers collect { case x: MemberDefHandler => x }
  def allDefSymbols  = allDefHandlers map (_.symbol) filter (_ ne NoSymbol)

  def lastRequest         = if (prevRequests.isEmpty) null else prevRequests.last
  def prevRequestList     = prevRequests.toList
  def allSeenTypes        = prevRequestList flatMap (_.typeOf.values.toList) distinct
  def allImplicits        = allHandlers filter (_.definesImplicit) flatMap (_.definedNames)
  def importHandlers      = allHandlers collect { case x: ImportHandler => x }

  def visibleTermNames: List[Name] = definedTerms ++ importedTerms distinct

  /** Another entry point for tab-completion, ids in scope */
  def unqualifiedIds = visibleTermNames map (_.toString) filterNot (_ contains "$") sorted

  /** Parse the ScalaSig to find type aliases */
  def aliasForType(path: String) = ByteCode.aliasForType(path)

  def withoutUnwrapping(op: => Unit): Unit = {
    val saved = isettings.unwrapStrings
    isettings.unwrapStrings = false
    try op
    finally isettings.unwrapStrings = saved
  }

  def symbolDefString(sym: Symbol) = {
    TypeStrings.quieter(
      afterTyper(sym.defString),
      sym.owner.name + ".this.",
      sym.owner.fullName + "."
    )
  }

  def showCodeIfDebugging(code: String) {
    /** Secret bookcase entrance for repl debuggers: end the line
     *  with "// show" and see what's going on.
     */
    def isShow    = code.lines exists (_.trim endsWith "// show")
    def isShowRaw = code.lines exists (_.trim endsWith "// raw")

    // old style
    beSilentDuring(parse(code)) foreach { ts =>
      ts foreach { t =>
        if (isShow || isShowRaw)
          withoutUnwrapping(echo(asCompactString(t)))
        else
          withoutUnwrapping(logDebug(asCompactString(t)))
      }
    }
  }

  // debugging
  def debugging[T](msg: String)(res: T) = {
    logDebug(msg + " " + res)
    res
  }
}

/** Utility methods for the Interpreter. */
object SparkIMain {
  // The two name forms this is catching are the two sides of this assignment:
  //
  // $line3.$read.$iw.$iw.Bippy =
  //   $line3.$read$$iw$$iw$Bippy@4a6a00ca
  private def removeLineWrapper(s: String) = s.replaceAll("""\$line\d+[./]\$(read|eval|print)[$.]""", "")
  private def removeIWPackages(s: String)  = s.replaceAll("""\$(iw|iwC|read|eval|print)[$.]""", "")
  private def removeSparkVals(s: String)   = s.replaceAll("""\$VAL[0-9]+[$.]""", "")

  def stripString(s: String)               = removeSparkVals(removeIWPackages(removeLineWrapper(s)))

  trait CodeAssembler[T] {
    def preamble: String
    def generate: T => String
    def postamble: String

    def apply(contributors: List[T]): String = stringFromWriter { code =>
      code println preamble
      contributors map generate foreach (code println _)
      code println postamble
    }
  }

  trait StrippingWriter {
    def isStripping: Boolean
    def stripImpl(str: String): String
    def strip(str: String): String = if (isStripping) stripImpl(str) else str
  }
  trait TruncatingWriter {
    def maxStringLength: Int
    def isTruncating: Boolean
    def truncate(str: String): String = {
      if (isTruncating && (maxStringLength != 0 && str.length > maxStringLength))
        (str take maxStringLength - 3) + "..."
      else str
    }
  }
  abstract class StrippingTruncatingWriter(out: JPrintWriter)
          extends JPrintWriter(out)
             with StrippingWriter
             with TruncatingWriter {
    self =>

    def clean(str: String): String = truncate(strip(str))
    override def write(str: String) = super.write(clean(str))
  }
  class ReplStrippingWriter(intp: SparkIMain) extends StrippingTruncatingWriter(intp.out) {
    import intp._
    def maxStringLength    = isettings.maxPrintString
    def isStripping        = isettings.unwrapStrings
    def isTruncating       = reporter.truncationOK

    def stripImpl(str: String): String = naming.unmangle(str)
  }

  class ReplReporter(intp: SparkIMain) extends ConsoleReporter(intp.settings, null, new ReplStrippingWriter(intp)) {
    override def printMessage(msg: String) {
      // Avoiding deadlock when the compiler starts logging before
      // the lazy val is done.
      if (intp.isInitializeComplete) {
        if (intp.totalSilence) ()
        else super.printMessage(msg)
      }
      else Console.println(msg)
    }
  }
}

class SparkISettings(intp: SparkIMain) extends Logging {
  /** A list of paths where :load should look */
  var loadPath = List(".")

  /** Set this to true to see repl machinery under -Yrich-exceptions.
   */
  var showInternalStackTraces = false

  /** The maximum length of toString to use when printing the result
   *  of an evaluation.  0 means no maximum.  If a printout requires
   *  more than this number of characters, then the printout is
   *  truncated.
   */
  var maxPrintString = 800

  /** The maximum number of completion candidates to print for tab
   *  completion without requiring confirmation.
   */
  var maxAutoprintCompletion = 250

  /** String unwrapping can be disabled if it is causing issues.
   *  Settings this to false means you will see Strings like "$iw.$iw.".
   */
  var unwrapStrings = true

  def deprecation_=(x: Boolean) = {
    val old = intp.settings.deprecation.value
    intp.settings.deprecation.value = x
    if (!old && x) logDebug("Enabled -deprecation output.")
    else if (old && !x) logDebug("Disabled -deprecation output.")
  }

  def deprecation: Boolean = intp.settings.deprecation.value

  def allSettings = Map(
    "maxPrintString" -> maxPrintString,
    "maxAutoprintCompletion" -> maxAutoprintCompletion,
    "unwrapStrings" -> unwrapStrings,
    "deprecation" -> deprecation
  )

  private def allSettingsString =
    allSettings.toList sortBy (_._1) map { case (k, v) => "  " + k + " = " + v + "\n" } mkString

  override def toString = """
    | SparkISettings {
    | %s
    | }""".stripMargin.format(allSettingsString)
}
