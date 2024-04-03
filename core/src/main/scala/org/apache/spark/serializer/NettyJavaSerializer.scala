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

package org.apache.spark.serializer

import java.io._
import java.nio.ByteBuffer

import scala.reflect.ClassTag

import org.apache.spark.SparkConf
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.util.{
  ByteBufferInputStream,
  ByteBufferOutputStream,
  Utils
}

private[spark] class NettyJavaSerializationStream(
    out: OutputStream,
    counterReset: Int,
    extraDebugInfo: Boolean
) extends SerializationStream {
  private val objOut = new ObjectOutputStream(out)
  private var counter = 0

  /** Calling reset to avoid memory leak:
    * http://stackoverflow.com/questions/1281549/memory-leak-traps-in-the-java-standard-api
    * But only call it every 100th time to avoid bloated serialization streams
    * (when the stream 'resets' object class descriptions have to be re-written)
    */
  def writeObject[T: ClassTag](t: T): SerializationStream = {
    try {
      objOut.writeObject(t)
    } catch {
      case e: NotSerializableException if extraDebugInfo =>
        throw SerializationDebugger.improveException(t, e)
    }
    counter += 1
    if (counterReset > 0 && counter >= counterReset) {
      objOut.reset()
      counter = 0
    }
    this
  }

  def flush() { objOut.flush() }
  def close() { objOut.close() }
}

private[spark] class NettyJavaDeserializationStream(
    in: InputStream,
    loader: ClassLoader
) extends DeserializationStream {

  private val objIn = new ObjectInputStream(in) {
    override def resolveClass(desc: ObjectStreamClass): Class[_] =
      try {
        val denyList = Array(
          "bsh.XThis",
          "bsh.Interpreter",
          "com.mchange.v2.c3p0.PoolBackedDataSource",
          "com.mchange.v2.c3p0.impl.PoolBackedDataSourceBase",
          "clojure.lang.PersistentArrayMap",
          "clojure.inspector.proxy$javax.swing.table.AbstractTableModel$ff19274a",
          "org.apache.commons.beanutils.BeanComparator",
          "org.apache.commons.collections.Transformer",
          "org.apache.commons.collections.functors.ChainedTransformer",
          "org.apache.commons.collections.functors.ConstantTransformer",
          "org.apache.commons.collections.functors.InstantiateTransformer",
          "org.apache.commons.collections.map.LazyMap",
          "org.apache.commons.collections.functors.InvokerTransformer",
          "org.apache.commons.collections.keyvalue.TiedMapEntry",
          "org.apache.commons.collections4.comparators.TransformingComparator",
          "org.apache.commons.collections4.functors.InvokerTransformer",
          "org.apache.commons.collections4.functors.ChainedTransformer",
          "org.apache.commons.collections4.functors.ConstantTransformer",
          "org.apache.commons.collections4.functors.InstantiateTransformer",
          "org.apache.commons.fileupload.disk.DiskFileItem",
          "org.apache.commons.io.output.DeferredFileOutputStream",
          "org.apache.commons.io.output.ThresholdingOutputStream",
          "org.apache.wicket.util.upload.DiskFileItem",
          "org.apache.wicket.util.io.DeferredFileOutputStream",
          "org.apache.wicket.util.io.ThresholdingOutputStream",
          "org.codehaus.groovy.runtime.ConvertedClosure",
          "org.codehaus.groovy.runtime.MethodClosure",
          "org.hibernate.engine.spi.TypedValue",
          "org.hibernate.tuple.component.AbstractComponentTuplizer",
          "org.hibernate.tuple.component.PojoComponentTuplizer",
          "org.hibernate.type.AbstractType",
          "org.hibernate.type.ComponentType",
          "org.hibernate.type.Type",
          "org.hibernate.EntityMode",
          "com.sun.rowset.JdbcRowSetImpl",
          "org.jboss.interceptor.builder.InterceptionModelBuilder",
          "org.jboss.interceptor.builder.MethodReference",
          "org.jboss.interceptor.proxy.DefaultInvocationContextFactory",
          "org.jboss.interceptor.proxy.InterceptorMethodHandler",
          "org.jboss.interceptor.reader.ClassMetadataInterceptorReference",
          "org.jboss.interceptor.reader.DefaultMethodMetadata",
          "org.jboss.interceptor.reader.ReflectiveClassMetadata",
          "org.jboss.interceptor.reader.SimpleInterceptorMetadata",
          "org.jboss.interceptor.spi.instance.InterceptorInstantiator",
          "org.jboss.interceptor.spi.metadata.InterceptorReference",
          "org.jboss.interceptor.spi.metadata.MethodMetadata",
          "org.jboss.interceptor.spi.model.InterceptionType",
          "org.jboss.interceptor.spi.model.InterceptionModel",
          "sun.rmi.server.UnicastRef",
          "sun.rmi.transport.LiveRef",
          "sun.rmi.transport.tcp.TCPEndpoint",
          "java.rmi.server.RemoteObject",
          "java.rmi.server.RemoteRef",
          "java.rmi.server.UnicastRemoteObject",
          "sun.rmi.server.ActivationGroupImpl",
          "sun.rmi.server.UnicastServerRef",
          "org.springframework.aop.framework.AdvisedSupport",
          "net.sf.json.JSONObject",
          "org.jboss.weld.interceptor.builder.InterceptionModelBuilder",
          "org.jboss.weld.interceptor.builder.MethodReference",
          "org.jboss.weld.interceptor.proxy.DefaultInvocationContextFactory",
          "org.jboss.weld.interceptor.proxy.InterceptorMethodHandler",
          "org.jboss.weld.interceptor.reader.ClassMetadataInterceptorReference",
          "org.jboss.weld.interceptor.reader.DefaultMethodMetadata",
          "org.jboss.weld.interceptor.reader.ReflectiveClassMetadata",
          "org.jboss.weld.interceptor.reader.SimpleInterceptorMetadata",
          "org.jboss.weld.interceptor.spi.instance.InterceptorInstantiator",
          "org.jboss.weld.interceptor.spi.metadata.InterceptorReference",
          "org.jboss.weld.interceptor.spi.metadata.MethodMetadata",
          "org.jboss.weld.interceptor.spi.model.InterceptionModel",
          "org.jboss.weld.interceptor.spi.model.InterceptionType",
          "org.python.core.PyObject",
          "org.python.core.PyBytecode",
          "org.python.core.PyFunction",
          "org.mozilla.javascript.**",
          "org.apache.myfaces.context.servlet.FacesContextImpl",
          "org.apache.myfaces.context.servlet.FacesContextImplBase",
          "org.apache.myfaces.el.CompositeELResolver",
          "org.apache.myfaces.el.unified.FacesELContext",
          "org.apache.myfaces.view.facelets.el.ValueExpressionMethodExpression",
          "com.sun.syndication.feed.impl.ObjectBean",
          "org.springframework.beans.factory.ObjectFactory",
          "org.springframework.aop.framework.AdvisedSupport",
          "org.springframework.aop.target.SingletonTargetSource",
          "com.vaadin.data.util.NestedMethodProperty",
          "com.vaadin.data.util.PropertysetIte"
        )
        var denied = false
        for (deniedClass <- denyList) {
          if (desc.getName == deniedClass) {
            denied = true
          }
        }
        if (denied) {
          throw new ClassNotFoundException(
            "Class is not allowed for deserialization: " + desc.getName
          )
        }
        // scalastyle:off classforname
        Class.forName(desc.getName, false, loader)
        // scalastyle:on classforname
      } catch {
        case e: ClassNotFoundException =>
          NettyJavaDeserializationStream.primitiveMappings.getOrElse(
            desc.getName,
            throw e
          )
      }
  }

  def readObject[T: ClassTag](): T = objIn.readObject().asInstanceOf[T]
  def close() { objIn.close() }
}

private object NettyJavaDeserializationStream {
  val primitiveMappings = Map[String, Class[_]](
    "boolean" -> classOf[Boolean],
    "byte" -> classOf[Byte],
    "char" -> classOf[Char],
    "short" -> classOf[Short],
    "int" -> classOf[Int],
    "long" -> classOf[Long],
    "float" -> classOf[Float],
    "double" -> classOf[Double],
    "void" -> classOf[Void]
  )
}

private[spark] class NettyJavaSerializerInstance(
    counterReset: Int,
    extraDebugInfo: Boolean,
    defaultClassLoader: ClassLoader
) extends JavaSerializerInstance(counterReset, extraDebugInfo, defaultClassLoader) {

  override def serialize[T: ClassTag](t: T): ByteBuffer = {
    val bos = new ByteBufferOutputStream()
    val out = serializeStream(bos)
    out.writeObject(t)
    out.close()
    bos.toByteBuffer
  }

  override def deserialize[T: ClassTag](bytes: ByteBuffer): T = {
    val bis = new ByteBufferInputStream(bytes)
    val in = deserializeStream(bis)
    in.readObject()
  }

  override def deserialize[T: ClassTag](
      bytes: ByteBuffer,
      loader: ClassLoader
  ): T = {
    val bis = new ByteBufferInputStream(bytes)
    val in = deserializeStream(bis, loader)
    in.readObject()
  }

  override def serializeStream(s: OutputStream): SerializationStream = {
    new NettyJavaSerializationStream(s, counterReset, extraDebugInfo)
  }

  override def deserializeStream(s: InputStream): DeserializationStream = {
    new NettyJavaDeserializationStream(s, defaultClassLoader)
  }

  override def deserializeStream(
      s: InputStream,
      loader: ClassLoader
  ): DeserializationStream = {
    new NettyJavaDeserializationStream(s, loader)
  }
}

/** :: DeveloperApi :: A Spark serializer that uses Java's built-in
  * serialization.
  *
  * @note
  *   This serializer is not guaranteed to be wire-compatible across different
  *   versions of Spark. It is intended to be used to serialize/de-serialize
  *   data within a single Spark application.
  */
@DeveloperApi
class NettyJavaSerializer(conf: SparkConf)
    extends JavaSerializer
    with Externalizable {
  private var counterReset =
    conf.getInt("spark.serializer.objectStreamReset", 100)
  private var extraDebugInfo =
    conf.getBoolean("spark.serializer.extraDebugInfo", true)

  protected def this() = this(new SparkConf()) // For deserialization only

  override def newInstance(): SerializerInstance = {
    val classLoader =
      defaultClassLoader.getOrElse(Thread.currentThread.getContextClassLoader)
    new NettyJavaSerializerInstance(counterReset, extraDebugInfo, classLoader)
  }

  override def writeExternal(out: ObjectOutput): Unit = Utils.tryOrIOException {
    out.writeInt(counterReset)
    out.writeBoolean(extraDebugInfo)
  }

  override def readExternal(in: ObjectInput): Unit = Utils.tryOrIOException {
    counterReset = in.readInt()
    extraDebugInfo = in.readBoolean()
  }
}

