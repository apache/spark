package org.apache.spark.deploy.rest

import java.io.{IOException, DataOutputStream, FileNotFoundException}
import java.net.{ConnectException, SocketException, HttpURLConnection, URL}
import javax.servlet.http.HttpServletResponse

import scala.io.Source

import com.google.common.base.Charsets

import org.apache.spark.Logging
import org.apache.spark.util.Utils

private[rest] object RestSubmissionClientHelper extends Logging {

  /** Send a GET request to the specified URL. */
  def get(url: URL): String = {
    logDebug(s"Sending GET request to server at $url.")
    val conn = url.openConnection().asInstanceOf[HttpURLConnection]
    conn.setRequestMethod("GET")
    readResponse(conn)
  }

  /** Send a POST request to the specified URL. */
  def post(url: URL): String = update(url, "POST")

  /** Send a POST request with the given JSON as the body to the specified URL. */
  def post(url: URL, json: String) = updateJson(url, "POST", json)

  /** Send a PUT request to the specified URL. */
  def put(url: URL): String = update(url, "PUT")

  /** Send a PUT request with the given JSON as the body to the specified URL. */
  def put(url: URL, json: String) = updateJson(url, "PUT", json)

  private def update(url: URL, method: String): String = {
    logDebug(s"Sending POST request to server at $url.")
    val conn = url.openConnection().asInstanceOf[HttpURLConnection]
    conn.setRequestMethod(method)
    readResponse(conn)
  }

  private def updateJson(url: URL, method: String, json: String): String = {
    logDebug(s"Sending POST request to server at $url:\n$json")
    val conn = url.openConnection().asInstanceOf[HttpURLConnection]
    conn.setRequestMethod("POST")
    conn.setRequestProperty("Content-Type", "application/json")
    conn.setRequestProperty("charset", "utf-8")
    conn.setDoOutput(true)
    try {
      val out = new DataOutputStream(conn.getOutputStream)
      Utils.tryWithSafeFinally {
        out.write(json.getBytes(Charsets.UTF_8))
      } {
        out.close()
      }
    } catch {
      case e: ConnectException =>
        throw new SubmitRestConnectionException("Connect Exception when connect to server", e)
    }
    readResponse(conn)
  }

  /**
   * Read the response from the server and return a json string.
   * If the response represents an error, report the embedded message to the user.
   */
  private def readResponse(connection: HttpURLConnection): String = {
    try {
      val dataStream =
        if (connection.getResponseCode == HttpServletResponse.SC_OK) {
          connection.getInputStream
        } else {
          connection.getErrorStream
        }
      // If the server threw an exception while writing a response, it will not have a body
      if (dataStream == null) {
        throw new IOException("Server returned empty body")
      }
      val responseJson = Source.fromInputStream(dataStream).mkString
      logDebug(s"Response from the server:\n$responseJson")
      responseJson
    } catch {
      case unreachable @ (_: FileNotFoundException | _: SocketException | _: IOException) =>
        throw new SubmitRestConnectionException("Unable to connect to server", unreachable)
    }
  }
}
