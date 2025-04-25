package org.apache.spark.util

import jakarta.servlet.http.{HttpServletRequest, HttpServletResponse}
import java.net.{HttpURLConnection, URL}
import java.util.NoSuchElementException
import scala.jdk.CollectionConverters._
import org.apache.spark.internal.Logging

object TfyHttpAuthUtils extends Logging { // Add Logging if needed
    def checkJobRunStatus(appId: String, request: HttpServletRequest): Unit = {
        val sfyServiceUrl = sys.env.get("SFY_SERVER_URL")
            .getOrElse(throw new NoSuchElementException("SFY_SERVER_URL environment variable not set"))

        val url = new URL(s"$sfyServiceUrl/v1/x/jobs/runs/external-id/$appId")
        val connection = url.openConnection().asInstanceOf[HttpURLConnection]
        try {
            connection.setRequestMethod("GET")
            connection.setConnectTimeout(5000) // Add reasonable timeouts
            connection.setReadTimeout(5000)

            // Add all headers from the original request to the new request
            val headerNames = request.getHeaderNames()
            if (headerNames != null) {
                headerNames.asScala.foreach { headerName =>
                    connection.setRequestProperty(headerName, request.getHeader(headerName))
                }
            }

            connection.connect()
            val statusCode = connection.getResponseCode

            if (statusCode != HttpServletResponse.SC_OK) {
                throw new Exception(s"An error occurred while checking the job run status for $appId, received status code: $statusCode")
            }
        } finally {
            connection.disconnect()
        }
    }
}