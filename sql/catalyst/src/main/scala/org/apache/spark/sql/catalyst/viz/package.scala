package org.apache.spark.sql.catalyst

import java.io.{File, FileWriter}

import scala.reflect.runtime.universe._
import scala.sys.process._

import org.apache.spark.sql.catalyst.trees._

/**
 * Functions for visualizing trees using GraphViz.
 */
package object viz {

  private val processLogger = ProcessLogger(
    (o: String) => println("out " + o),
    (e: String) => println("err " + e))

  implicit class TreeViz[T <: TreeNode[T]](t: TreeNode[T]) {

    /**
     * Displays this tree using GraphViz
     * Only works on Mac when GraphViz is installed...
     */
    def viz = {
      val grapher = new GraphTree
      grapher.generate(t)
      val pngFile = grapher.finish()
      s"open $pngFile" ! processLogger
    }
  }

  private class GraphTree extends GraphViz {
    lazy val file = File.createTempFile("queryPlan", ".dot")

    /* Need a function with return type Unit for Generator */
    def generate[T <: TreeNode[T]](plan: TreeNode[T]): Unit = {
      digraph("QueryPlan") {
        generatePlan(plan)
      }
    }

    def d(s: String) = s"""<font face="helvetica" POINT-SIZE="10">$s</font>"""

    /* Recursive function to draw individual plan nodes */
    protected def generatePlan[T <: TreeNode[T]](plan: TreeNode[T]): DotNode = plan match {
      case plan: TreeNode[T] with Product =>
        val arguments = argNamesForClass(plan.getClass).zip(plan.productIterator.toSeq).flatMap {
          case (_, value) if plan.children contains value => Nil
          case (_, None) => Nil
          case (name, Some(value)) => (name, value) :: Nil
          case (name, s: Seq[_]) if s.length > 3 => (name, s.mkString("\n")) :: Nil
          case (name, s: Seq[_]) => (name, s.mkString(", ")) :: Nil
          case (name, value) => (name, value) :: Nil
        }.map { case (k, v) => (k, v.toString.replaceAll("&", "&amp;").replaceAll("<", "&lt;").replaceAll(">", "&gt;").replaceAll("#\\d+", "").replaceAll("\\n", """<br align="left"/>"""))}

        val formattedArgs = if (arguments.size == 1) arguments.map(v => s"""<td COLSPAN="2">${d(v._2)}<br align="left"/></td>""") else arguments.map { case (k, v) => s"""<td ALIGN="right" BORDER="1">${d(k)}</td><td ALIGN="left" BORDER="1">${d(v)}<br align="left"/></td>"""}

        val nodeTitle = s"""<table border="0" cellborder="0"><tr><td COLSPAN="2" ALIGN="center"><font face="helvetica-bold" POINT-SIZE="12">${plan.nodeName}</font></td></tr>"""
        val nodeArgs = s"""${formattedArgs.map(r => s"<tr>$r</tr>").mkString("")}</table>"""
        outputNode(nodeTitle + nodeArgs, children = plan.children.map(generatePlan))
    }

    protected def argNamesForClass(clazz: Class[_]): Seq[String] = {
      val m = runtimeMirror(getClass.getClassLoader)
      val classSymbol = m.staticClass(clazz.getName)
      val tpe = classSymbol.selfType
      val params = tpe.members.find(_.name.toString == "<init>").get.asMethod.paramss
      params.flatten.map(_.name.toString)
    }
  }

  private trait GraphViz extends FileGenerator {
    protected val pngFile = File.createTempFile("queryPlan", ".png")
    private val curId = new java.util.concurrent.atomic.AtomicInteger

    protected def nextId = curId.getAndIncrement()

    abstract override def finish(): File = {
      val dotFile = super.finish()
      Seq("dot", "-o" + pngFile, "-Tpng", dotFile.getCanonicalPath) ! processLogger
      pngFile
    }

    protected def digraph[A](name: String)(f: => A): A = {
      outputBraced("digraph %s".format(name)) {
        output("rankdir=BT;")
        output("ranksep=0.2;")
        f
      }
    }

    case class DotNode(id: String)

    protected def outputNode(label: String, shape: String = "plaintext", fillcolor: String = "azure3", children: Seq[DotNode] = Nil): DotNode = {
      val node = DotNode("node" + nextId)
      output(node.id, "[label=", quote(label), ", shape=", shape, ", fillcolor=", fillcolor, "];")
      children.foreach(outputEdge(_, node))
      node
    }

    protected def outputEdge(src: DotNode, dest: DotNode, label: String = "", color: String = "black", arrowsize: Double = 0.5): DotNode = {
      output(src.id, " -> ", dest.id, "[label=", quote(label), ", color=", quote(color), ", arrowsize=", arrowsize.toString, "];")
      dest
    }

    protected def outputCluster[A](label: String*)(func: => A): A = {
      outputBraced("subGraph ", "cluster" + nextId) {
        output("label=", quote(label.mkString), ";")
        func
      }
    }
  }

  private trait StringGenerator extends Generator[String] {
    val sb = new StringBuilder

    def append(s: String): Unit = append(s)

    def finish(): String = sb.toString()
  }

  private trait FileGenerator extends Generator[File] {
    val file: File
    val fileWriter = new FileWriter(file)

    def append(s: String) = fileWriter.write(s)

    def finish(): File = {
      fileWriter.close()
      file
    }
  }

  /**
   * Framework for a generator class that takes in a Tree of type InputType and returns a string.
   * You provide the generate function, we take care of indentation and string building.
   */
  private abstract class Generator[OutputType] {
    val indentChar = "  "
    var indent: Int = 0

    def append(s: String): Unit

    def finish(): OutputType

    /**
     * Increases the indentation level for all outputs that are made during the provided function.
     * Intended to be nested arbitarily.
     */
    protected def indent[A](func: => A): A = {
      indent += 1
      val result: A = func
      indent -= 1
      result
    }

    /**
     * Append the concatinated strings provided in parts to the StringBuilder followed by a newline
     */
    protected def output(parts: String*): Unit = {
      (0 to indent).foreach((i) => append(indentChar))
      parts.foreach(append)
      append("\n")
    }

    protected def outputBraced[A](parts: String*)(child: => A): A =
      outputCont(" {\n", "}\n", parts: _*)(child)

    protected def outputParen[A](parts: String*)(child: => A): A =
      outputCont(" (\n", ");\n", parts: _*)(child)

    protected def outputCont[A](start: String, end: String, parts: String*)(child: => A): A = {
      (0 to indent).foreach((i) => append(indentChar))
      parts.foreach(append)
      append(start)
      val result: A = indent {
        child
      }
      (0 to indent).foreach((i) => append(indentChar))
      append(end)
      result
    }

    protected def outputPartial(parts: String*): Unit = {
      (0 to indent).foreach((i) => append(indentChar))
      parts.foreach(append)
    }

    protected def outputPartialCont(parts: String*): Unit = {
      parts.foreach(append)
    }

    protected def outputPartialEnd(): Unit = {
      append("\n")
    }

    protected def quote(string: String) =
      if (string contains "<") {
        "<" + string + ">"
      } else {
        "\"" + string.replaceAll("\"", "\\\"").replaceAll("\n", "\\\\n") + "\""
      }
  }
}