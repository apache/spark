package spark.ui.env

import javax.servlet.http.HttpServletRequest

import org.eclipse.jetty.server.Handler

import scala.collection.JavaConversions._

import spark.ui.JettyUtils._
import spark.ui.UIUtils.headerSparkPage
import spark.ui.Page.Environment
import spark.SparkContext
import spark.ui.UIUtils

import scala.xml.Node

private[spark] class EnvironmentUI(sc: SparkContext) {

  def getHandlers = Seq[(String, Handler)](
    ("/environment", (request: HttpServletRequest) => envDetails(request))
  )

  def envDetails(request: HttpServletRequest): Seq[Node] = {
    val properties = System.getProperties.iterator.toSeq

    val classPathProperty = properties
        .filter{case (k, v) => k.contains("java.class.path")}
        .headOption
        .getOrElse("", "")
    val sparkProperties = properties.filter(_._1.contains("spark"))
    val otherProperties = properties.diff(sparkProperties :+ classPathProperty)

    val propertyHeaders = Seq("Name", "Value")
    def propertyRow(kv: (String, String)) = <tr><td>{kv._1}</td><td>{kv._2}</td></tr>
    val propertyTable = UIUtils.listingTable(
      propertyHeaders, propertyRow, sparkProperties ++ otherProperties)

    val classPathEntries = classPathProperty._2
        .split(System.getProperty("path.separator", ":"))
        .filterNot(e => e.isEmpty)
        .map(e => (e, "System Classpath"))
    val addedJars = sc.addedJars.iterator.toSeq.map{case (path, time) => (path, "Added By User")}
    val addedFiles = sc.addedFiles.iterator.toSeq.map{case (path, time) => (path, "Added By User")}
    val classPath = addedJars ++ addedFiles ++ classPathEntries

    val classPathHeaders = Seq("Resource", "Source")
    def classPathRow(data: (String, String)) = <tr><td>{data._1}</td><td>{data._2}</td></tr>
    val classPathTable = UIUtils.listingTable(classPathHeaders, classPathRow, classPath)

    val content =
      <span>
        <h2>System Properties</h2> {propertyTable}
        <h2>Classpath Entries</h2> {classPathTable}
      </span>

    headerSparkPage(content, sc, "Environment", Environment)
  }
}
