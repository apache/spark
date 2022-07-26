/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.jmx

import com.fasterxml.jackson.core.{JsonFactory, JsonGenerator}
import java.io.Writer
import java.lang.management.ManagementFactory
import java.lang.reflect.Array
import java.util.Locale
import javax.management._
import javax.management.openmbean.{CompositeData, TabularData}
import javax.servlet.http.{HttpServlet, HttpServletRequest, HttpServletResponse}
import org.slf4j.LoggerFactory
import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

import org.apache.spark.jmx.exception.{CheckAttributeException, CheckQryNameException}

class JMXJsonServlet extends HttpServlet {

  import JMXJsonServlet._


  override def doGet(request: HttpServletRequest, response: HttpServletResponse): Unit = {
    val generator = buildJsonGenerator(response.getWriter)
    try {
      // 1. match get parameter
      // if match get, then process get parameter
      // 2. match qry parameter
      // match qry not empty, then query all parameter -> set qry '*:*'
      val args: (String, ObjectName, String) = Option(
        request.getParameter("get")
      ) match {
        case Some(x) => parseObjectNameAttribute("get", x)
        case None => Option(request.getParameter("qry")) match {
          case Some(x) => parseObjectNameAttribute(qryName = x)
          case None => parseObjectNameAttribute(qryName = "*:*")
        }
      }
      buildJmxResponse(generator, args)
    } catch {
      case _: CheckQryNameException =>
        generator.writeStringField("result", "ERROR")
        generator.writeStringField("message", "query format is not as expected.")
        generator.close()
        response.setStatus(HttpServletResponse.SC_BAD_REQUEST)
      case e: CheckAttributeException =>
        generator.writeStringField("result", "ERROR")
        generator.writeStringField("message", "No attribute with name " + e.getAttribute
          + " was found.")
        // if we catch here, must has construct start {Object, Array}
        generator.writeEndObject()
        generator.writeEndArray()
        generator.close()
        response.setStatus(HttpServletResponse.SC_NOT_FOUND)
    } finally {
      Option(generator).foreach(_.close())
    }
  }

}

object JMXJsonServlet {
  private[jmx] val ACCESS_CONTROL_ALLOW_METHODS = "Access-Control-Allow-Methods"
  private[jmx] val ACCESS_CONTROL_ALLOW_ORIGIN = "Access-Control-Allow-Origin"

  private[jmx] val qryNameMatcher = "^(.+):(.+)$".r

  private val mBeanServer: MBeanServer = ManagementFactory.getPlatformMBeanServer
  private val factory = new JsonFactory()

  @transient private val log = LoggerFactory.getLogger(classOf[JMXJsonServlet])

  /**
   * @param generator json generator
   * @param args (qry_name, qry_object_name, attribute)
   */
  @throws[CheckAttributeException]
  private[jmx] def buildJmxResponse(
    generator: JsonGenerator,
    args: (String, ObjectName, String)
  ): Unit = {
    val (qryName, _, attribute) = args
    generator.writeArrayFieldStart("beans")
    val mBeanNames = mBeanServer.queryNames(new ObjectName(qryName), null)
    for (mBeanName <- mBeanNames.asScala) {
      // 1. get info from management
      val mBeanInfo = Try(mBeanServer.getMBeanInfo(mBeanName)) match {
        case Failure(e) =>
          log.error(s"Problem while trying to process JMX query: $qryName " +
            s"with MBean $mBeanName", e)
          null
        case Success(value) => value
      }
      // 2. if we cant get info, then skip for continue
      if (mBeanInfo != null) {


        // 4. get attributeType and modelerType
        def processException(e: Exception): Unit = {
          log.error("getting attribute " + attribute + " of " + mBeanName + " threw an exception",
            e);
        }

        // 5. build { code, prs, attributeinfo}
        var code = mBeanInfo.getClassName
        var prs = ""
        var attributeinfo: Object = null

        try {
          if ("org.apache.commons.modeler.BaseModelMBean".equals(code)) {
            prs = "modelerType"
            code = mBeanServer.getAttribute(mBeanName, prs).asInstanceOf[String]
          }
          if (attribute != null) {
            prs = attribute;
            attributeinfo = mBeanServer.getAttribute(mBeanName, prs)
          }
        } catch {
          case e: AttributeNotFoundException => processException(e)
          case e: MBeanException => processException(e)
          case e: RuntimeException => processException(e)
          case e: ReflectionException => processException(e)
        }

        // 6. write start object
        generator.writeStartObject()
        generator.writeStringField("name", mBeanName.toString)

        // 7. write modelerType
        generator.writeStringField("modelerType", code)

        // 8. check
        // when attribute exists, whether attributeinfo is null
        if ((attribute != null) && (attributeinfo == null)) {
          throw new CheckAttributeException(attribute)
        }

        // 9. write attribute
        if (attribute != null) {
          writeAttribute(generator, attribute, attributeinfo)
        } else {
          mBeanInfo.getAttributes.foreach { attr =>
            writeAttribute(generator, mBeanName, attr)
          }
        }
        generator.writeEndObject()
      }
    }
    generator.writeEndArray()
  }

  /**
   * @param generator json generator
   * @param mBeanObjectName management bean of JMX
   * @param attrInfo attribute info
   */
  private def writeAttribute(
    generator: JsonGenerator,
    mBeanObjectName: ObjectName,
    attrInfo: MBeanAttributeInfo
  ): Unit = {
    // 1. check readable
    if (!attrInfo.isReadable) return

    def attributeContains(value: String*): Boolean = {
      for (matchStr <- value) {
        if (attrInfo.getName.contains(matchStr)) {
          return true
        }
      }
      false
    }

    // 2. check special character
    if (attributeContains("=", ":", " ")) return
    var value: Object = null
    val attributeName = attrInfo.getName
    try {
      value = mBeanServer.getAttribute(mBeanObjectName, attributeName)
    } catch {
      case e: RuntimeMBeanException =>
        if (e.getCause.isInstanceOf[UnsupportedOperationException]) {
          log.debug("getting attribute " + attributeName + " of "
            + mBeanObjectName + " threw an exception", e)
        } else {
          log.error("getting attribute " + attributeName + " of "
            + mBeanObjectName + " threw an exception", e)
        }
        return
      case e: RuntimeErrorException =>
        log.debug("getting attribute " + attributeName + " of "
          + mBeanObjectName + " threw an exception", e)
        return
      case _: AttributeNotFoundException =>
        return
      case e: MBeanException =>
        log.error("getting attribute " + attributeName + " of "
          + mBeanObjectName + " threw an exception", e)
        return
      case e: RuntimeException =>
        log.error("getting attribute " + attributeName + " of "
          + mBeanObjectName + " threw an exception", e)
        return
      case e: ReflectionException =>
        log.error("getting attribute " + attributeName + " of "
          + mBeanObjectName + " threw an exception", e)
        return
      case _: InstanceNotFoundException =>
        return
    }
    writeAttribute(generator, attributeName, value)
  }

  private def writeAttribute(
    generator: JsonGenerator,
    attributeName: String,
    value: Object
  ): Unit = {
    generator.writeFieldName(attributeName)
    writeObject(generator, value)
  }

  /**
   * @param jg    json generator
   * @param value attribute object
   */
  private def writeObject(jg: JsonGenerator, value: Object): Unit = {
    if (value == null) jg.writeNull()
    else {
      val c: Class[_] = value.getClass
      if (c.isArray) {
        jg.writeStartArray()
        val len = Array.getLength(value)
        for (j <- 0 until len) {
          val item = Array.get(value, j)
          writeObject(jg, item)
        }
        jg.writeEndArray()
      } else value match {
        case n: Number =>
          jg.writeNumber(n.toString)
        case b: java.lang.Boolean =>
          jg.writeBoolean(b)
        case cds: CompositeData =>
          val comp = cds.getCompositeType
          val keys = comp.keySet
          jg.writeStartObject()
          for (key <- keys.asScala) {
            writeAttribute(jg, key, cds.get(key))
          }
          jg.writeEndObject()
        case tds: TabularData =>
          jg.writeStartArray()
          for (entry <- tds.values.asScala) {
            writeObject(jg, entry.asInstanceOf[Object])
          }
          jg.writeEndArray()
        case _ => jg.writeString(value.toString)
      }
    }
  }


  /**
   * @param response the basic response
   */
  private[jmx] def buildBasicResponse(response: HttpServletResponse): Unit = {
    response.setContentType("application/json; charset=utf8")
    response.setHeader(ACCESS_CONTROL_ALLOW_METHODS, "GET")
    response.setHeader(ACCESS_CONTROL_ALLOW_ORIGIN, "*")
  }

  /**
   * @param qryType GET | null
   * @param qryName name key
   * @return (ObjectName, Attribute)
   */
  @throws[CheckQryNameException]
  private[jmx] def parseObjectNameAttribute(
    qryType: String = "qry",
    qryName: String):
  (String, ObjectName, String) = {
    if (!checkQryName(qryName)) {
      throw new CheckQryNameException
    }
    qryType.toUpperCase(Locale.ROOT) match {
      case "GET" =>
        val splitName = qryName.split(":")
        (qryName, new ObjectName(splitName(0)), splitName(1))
      case _ =>
        (qryName, new ObjectName(qryName), null)
    }
  }

  /**
   * @param qryName key:valye
   * @return check result
   */
  private[jmx] def checkQryName(qryName: String): Boolean = {
    if (qryNameMatcher.findAllIn(qryName).isEmpty) {
      return false
    }
    true
  }

  /**
   * @param writer java writer
   * @return json generator
   */
  private[jmx] def buildJsonGenerator(writer: Writer) = {
    val generator = factory.createGenerator(writer)
    generator.disable(JsonGenerator.Feature.AUTO_CLOSE_TARGET)
    generator.useDefaultPrettyPrinter()
    generator.writeStartObject()
    generator
  }
}