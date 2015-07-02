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
package org.apache.spark.sql.hive

import java.net.URI
import java.sql.{Date => SqlDate, Timestamp}

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.serializers.FieldSerializer
import com.esotericsoftware.kryo.{Kryo, Serializer}
import org.antlr.runtime.CommonToken
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.metastore.api.Date
import org.apache.hadoop.hive.ql.exec.{ColumnInfo, Operator}
import org.apache.hadoop.hive.ql.plan.{AbstractOperatorDesc, OperatorDesc}
import org.objenesis.strategy.StdInstantiatorStrategy
/**
 * This is a copy and paste of parts of  `org.apache.hadoop.hive.ql.exec.Utilities`,
 * to work around the issue that ASF hive has shaded their version of Kryo.
 *
 * This version is in sync with the version of Kryo used by the rest of Spark.
 */
private[hive] object ThreadKryoSerializer {

  private var runtimeSerializationKryo: ThreadLocal[Kryo] = new ThreadLocal[Kryo]() {
    protected override def initialValue: Kryo = {
      val kryo = new Kryo
      kryo.setClassLoader(Thread.currentThread.getContextClassLoader)
      kryo.register(classOf[Date], new SqlDateSerializer)
      kryo.register(classOf[Timestamp], new TimestampSerializer)
      kryo.register(classOf[Path], new PathSerializer)

      kryo.setInstantiatorStrategy(new StdInstantiatorStrategy)
      removeField(kryo, classOf[Operator[_ <: OperatorDesc]], "colExprMap")
      removeField(kryo, classOf[ColumnInfo], "objectInspector")
      removeField(kryo, classOf[AbstractOperatorDesc], "statistics")
      kryo
    }
  }

  /**
   * Get the thread-local Kryo instance
   * @return an instance of Kryo for the use by this thread alone.
   */
  def get(): Kryo = runtimeSerializationKryo.get()

  /**
   * Kryo serializer for timestamp.
   */
  private class TimestampSerializer extends Serializer[Timestamp] {
    def read(kryo: Kryo, input: Input, clazz: Class[Timestamp]): Timestamp = {
      val ts = new Timestamp(input.readLong)
      ts.setNanos(input.readInt)
      ts
    }

    def write(kryo: Kryo, output: Output, ts: Timestamp) {
      output.writeLong(ts.getTime)
      output.writeInt(ts.getNanos)
    }
  }

  /** Custom Kryo serializer for sql date, otherwise Kryo gets confused between
   SqlDate and java.util.Date while deserializing
    */
  private class SqlDateSerializer extends Serializer[SqlDate] {
    def read(kryo: Kryo, input: Input, clazz: Class[SqlDate]): SqlDate = {
      return new SqlDate(input.readLong)
    }

    def write(kryo: Kryo, output: Output, sqlDate: SqlDate) {
      output.writeLong(sqlDate.getTime)
    }
  }

  private class CommonTokenSerializer extends Serializer[CommonToken] {
    def read(kryo: Kryo, input: Input, clazz: Class[CommonToken]): CommonToken = {
      return new CommonToken(input.readInt, input.readString)
    }

    def write(kryo: Kryo, output: Output, token: CommonToken) {
      output.writeInt(token.getType)
      output.writeString(token.getText)
    }
  }

  private class PathSerializer extends Serializer[Path] {
    def write(kryo: Kryo, output: Output, path: Path) {
      output.writeString(path.toUri.toString)
    }

    def read(kryo: Kryo, input: Input, `type`: Class[Path]): Path = {
      return new Path(URI.create(input.readString))
    }
  }

  protected def removeField(kryo: Kryo, fieldtype: Class[_],
      fieldName: String) {
    val fld = new FieldSerializer(kryo, fieldtype)
    fld.removeField(fieldName)
    kryo.register(fieldtype, fld)
  }

}
