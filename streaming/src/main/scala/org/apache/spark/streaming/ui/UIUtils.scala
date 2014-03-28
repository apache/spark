package org.apache.spark.streaming.ui

import scala.xml.Node
import org.apache.spark.ui.Page

private[spark] object UIUtils {

  import org.apache.spark.ui.UIUtils.prependBaseUri

  def headerStreamingPage(
      content: => Seq[Node],
      basePath: String,
      appName: String,
      title: String): Seq[Node] = {
    val overview = {
      <li><a href={prependBaseUri(basePath)}>Overview</a></li>
    }

    <html>
      <head>
        <meta http-equiv="Content-type" content="text/html; charset=utf-8" />
        <link rel="stylesheet" href={prependBaseUri("/static/bootstrap.min.css")}
              type="text/css" />
        <link rel="stylesheet" href={prependBaseUri("/static/webui.css")}
              type="text/css" />
        <script src={prependBaseUri("/static/sorttable.js")} ></script>
        <title>{appName} - {title}</title>
      </head>
      <body>
        <div class="navbar navbar-static-top">
          <div class="navbar-inner">
            <a href={prependBaseUri(basePath, "/")} class="brand">
              <img src={prependBaseUri("/static/spark-logo-77x50px-hd.png")} />
            </a>
            <ul class="nav">
              {overview}
            </ul>
            <p class="navbar-text pull-right"><strong>{appName}</strong> application UI</p>
          </div>
        </div>

        <div class="container-fluid">
          <div class="row-fluid">
            <div class="span12">
              <h3 style="vertical-align: bottom; display: inline-block;">
                {title}
              </h3>
            </div>
          </div>
          {content}
        </div>
      </body>
    </html>
  }

  def listingTable[T](
      headers: Seq[String],
      makeRow: T => Seq[Node],
      rows: Seq[T],
      fixedWidth: Boolean = false): Seq[Node] = {
    org.apache.spark.ui.UIUtils.listingTable(headers, makeRow, rows, fixedWidth)
  }

  def listingTable[T](
      headers: Seq[String],
      rows: Seq[Seq[String]],
      fixedWidth: Boolean = false
    ): Seq[Node] = {
    def makeRow(data: Seq[String]): Seq[Node] = <tr> {data.map(d => <td>{d}</td>)} </tr>
    org.apache.spark.ui.UIUtils.listingTable(headers, makeRow, rows, fixedWidth)
  }
}
