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

// This file is placed in different package to make sure all of these components work well
// when they are outside of org.apache.spark.
package other.supplier

import scala.collection.mutable
import scala.reflect.ClassTag

import akka.serialization.Serialization

import org.apache.spark.SparkConf
import org.apache.spark.deploy.master._

class CustomRecoveryModeFactory(
  conf: SparkConf,
  serialization: Serialization
) extends StandaloneRecoveryModeFactory(conf, serialization) {

  CustomRecoveryModeFactory.instantiationAttempts += 1

  /**
   * PersistenceEngine defines how the persistent data(Information about worker, driver etc..)
   * is handled for recovery.
   *
   */
  override def createPersistenceEngine(): PersistenceEngine =
    new CustomPersistenceEngine(serialization)

  /**
   * Create an instance of LeaderAgent that decides who gets elected as master.
   */
  override def createLeaderElectionAgent(master: LeaderElectable): LeaderElectionAgent =
    new CustomLeaderElectionAgent(master)
}

object CustomRecoveryModeFactory {
  @volatile var instantiationAttempts = 0
}

class CustomPersistenceEngine(serialization: Serialization) extends PersistenceEngine {
  val data = mutable.HashMap[String, Array[Byte]]()

  CustomPersistenceEngine.lastInstance = Some(this)

  /**
   * Defines how the object is serialized and persisted. Implementation will
   * depend on the store used.
   */
  override def persist(name: String, obj: Object): Unit = {
    CustomPersistenceEngine.persistAttempts += 1
    serialization.serialize(obj) match {
      case util.Success(bytes) => data += name -> bytes
      case util.Failure(cause) => throw new RuntimeException(cause)
    }
  }

  /**
   * Defines how the object referred by its name is removed from the store.
   */
  override def unpersist(name: String): Unit = {
    CustomPersistenceEngine.unpersistAttempts += 1
    data -= name
  }

  /**
   * Gives all objects, matching a prefix. This defines how objects are
   * read/deserialized back.
   */
  override def read[T: ClassTag](prefix: String): Seq[T] = {
    CustomPersistenceEngine.readAttempts += 1
    val clazz = implicitly[ClassTag[T]].runtimeClass.asInstanceOf[Class[T]]
    val results = for ((name, bytes) <- data; if name.startsWith(prefix))
      yield serialization.deserialize(bytes, clazz)

    results.find(_.isFailure).foreach {
      case util.Failure(cause) => throw new RuntimeException(cause)
    }

    results.flatMap(_.toOption).toSeq
  }
}

object CustomPersistenceEngine {
  @volatile var persistAttempts = 0
  @volatile var unpersistAttempts = 0
  @volatile var readAttempts = 0

  @volatile var lastInstance: Option[CustomPersistenceEngine] = None
}

class CustomLeaderElectionAgent(val masterActor: LeaderElectable) extends LeaderElectionAgent {
  masterActor.electedLeader()
}

