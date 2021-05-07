/*
 * This file is copied from Uber Remote Shuffle Service
(https://github.com/uber/RemoteShuffleService) and modified.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.serializer

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.{Output => KryoOutput, UnsafeOutput => KryoUnsafeOutput}

import java.io._
import scala.reflect.ClassTag

object RssKryoSerializationStream {
    def newStream(serializerInstance: KryoSerializerInstance,
                  bufferSize: Int,
                  maxBufferSize: Int):
        RssKryoSerializationStream = {
        new RssKryoSerializationStream(serializerInstance, false, bufferSize, maxBufferSize)
    }
}

class RssKryoSerializationStream(
        serInstance: KryoSerializerInstance,
        useUnsafe: Boolean,
        bufferSize: Int,
        maxBufferSize: Int) extends SerializationStream {

    private[this] var output: KryoOutput =
            if (useUnsafe) new KryoUnsafeOutput(bufferSize, maxBufferSize)
            else new KryoOutput(bufferSize, maxBufferSize)

    private[this] var kryo: Kryo = serInstance.borrowKryo()

    override def writeObject[T: ClassTag](t: T): SerializationStream = {
        kryo.writeClassAndObject(output, t)
        this
    }

    override def flush(): Unit = {
        if (output == null) {
            throw new IOException("Stream is closed")
        }
        output.flush()
    }

    override def close(): Unit = {
        if (output != null) {
            try {
                output.close()
            } finally {
                serInstance.releaseKryo(kryo)
                kryo = null
                output = null
            }
        }
    }

    def position(): Int = output.position()

    def toBytes: Array[Byte] = output.toBytes

    def clear(): Unit = output.clear()
}
